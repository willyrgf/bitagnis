package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/willyrgf/bitagnis/lib"
)

const (
	supportedAxeOSVersion = "v2.8.1"
	supportedASICModel    = "BM1370"
	supportedBoardVersion = "601"

	defaultRebootDeadline   = 2 * time.Minute
	defaultHealthDeadline   = 3 * time.Minute
	defaultRediscoveryDelay = 2 * time.Second
	rebootUptimeTolerance   = 5 * time.Second
	startupHealthyPolls     = 2
)

var errMinerNotFound = errors.New("miner not found during rediscovery")

type minerObservation struct {
	miner    lib.DiscoveredMiner
	info     lib.Info
	asic     lib.ASICSettings
	settings lib.Settings
	state    lib.MinerState
	created  bool
}

type mutationDiscovery func(context.Context, string) (lib.DiscoveredMiner, error)

type miningPasswordResolver func(lib.MiningSettings) (string, string, error)

type mutationCoordinator struct {
	mu sync.Mutex

	devices          deviceAPI
	states           optimizerStateStore
	settings         lib.SettingsFile
	discover         mutationDiscovery
	resolvePasswords miningPasswordResolver
	logger           *log.Logger
	reset            func(string)

	routes            map[string]lib.DiscoveredMiner
	expectedHostnames map[string]string

	selected           []string
	startupIndex       int
	startupHealth      map[string]int
	startupHealthSince map[string]time.Time
	startupBlocked     string
	gateOpen           bool
	reapply            map[string]bool

	active       map[string]*activeMutation
	normalActive string
	nextMutation uint64
	results      chan mutationResult

	rebootDeadline   time.Duration
	healthDeadline   time.Duration
	rediscoveryDelay time.Duration
	now              func() time.Time
}

type activeMutation struct {
	id     uint64
	kind   lib.MutationKind
	point  lib.OperatingPoint
	normal bool
	cancel context.CancelFunc
}

type mutationRequest struct {
	id       uint64
	macAddr  string
	hostname string
	ip       string
	kind     lib.MutationKind
	point    lib.OperatingPoint
	info     lib.Info
	settings lib.Settings

	primaryPassword  string
	fallbackPassword string
}

type mutationResult struct {
	id       uint64
	macAddr  string
	hostname string
	kind     lib.MutationKind
	point    lib.OperatingPoint
	miner    lib.DiscoveredMiner
	err      error
}

func newMutationCoordinator(
	devices deviceAPI,
	states optimizerStateStore,
	settings lib.SettingsFile,
	discovered []lib.DiscoveredMiner,
	reapply map[string]bool,
	discover mutationDiscovery,
	resolvePasswords miningPasswordResolver,
	logger *log.Logger,
	reset func(string),
) *mutationCoordinator {
	routes := make(map[string]lib.DiscoveredMiner, len(discovered))
	selected := make([]string, 0, len(discovered))
	for _, miner := range discovered {
		routes[miner.Info.MacAddr] = miner
		selected = append(selected, miner.Info.MacAddr)
	}
	sort.Strings(selected)
	if resolvePasswords == nil {
		resolvePasswords = func(lib.MiningSettings) (string, string, error) {
			return "", "", fmt.Errorf("password source is unavailable")
		}
	}
	if logger == nil {
		logger = log.Default()
	}
	if reset == nil {
		reset = func(string) {}
	}
	return &mutationCoordinator{
		devices:            devices,
		states:             states,
		settings:           settings,
		discover:           discover,
		resolvePasswords:   resolvePasswords,
		logger:             logger,
		reset:              reset,
		routes:             routes,
		expectedHostnames:  make(map[string]string),
		selected:           selected,
		startupHealth:      make(map[string]int),
		startupHealthSince: make(map[string]time.Time),
		reapply:            cloneBoolMap(reapply),
		active:             make(map[string]*activeMutation),
		results:            make(chan mutationResult, max(len(discovered)*2, 8)),
		rebootDeadline:     defaultRebootDeadline,
		healthDeadline:     defaultHealthDeadline,
		rediscoveryDelay:   defaultRediscoveryDelay,
		now:                time.Now,
	}
}

func (coordinator *mutationCoordinator) RequireHostnames(
	expected map[string]string,
) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.expectedHostnames = make(map[string]string, len(expected))
	for macAddr, hostname := range expected {
		coordinator.expectedHostnames[macAddr] = hostname
	}
}

func (coordinator *mutationCoordinator) Routes() []lib.DiscoveredMiner {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	routes := make([]lib.DiscoveredMiner, 0, len(coordinator.routes))
	for _, miner := range coordinator.routes {
		routes = append(routes, miner)
	}
	sort.Slice(routes, func(left int, right int) bool {
		return routes[left].Info.MacAddr < routes[right].Info.MacAddr
	})
	return routes
}

func (coordinator *mutationCoordinator) UpdateDiscovery(discovered []lib.DiscoveredMiner) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	for _, miner := range discovered {
		if _, selected := coordinator.routes[miner.Info.MacAddr]; selected {
			coordinator.routes[miner.Info.MacAddr] = miner
		}
	}
}

func (coordinator *mutationCoordinator) GateOpen() bool {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	return coordinator.gateOpen
}

func (coordinator *mutationCoordinator) Advance(
	ctx context.Context,
	observations map[string]*minerObservation,
	now time.Time,
) error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()

	appliedResult, err := coordinator.applyResultsLocked()
	if err != nil {
		return err
	}
	if appliedResult {
		return nil
	}
	for macAddr, observation := range observations {
		state, err := coordinator.states.LoadMiner(macAddr)
		if err != nil {
			return err
		}
		observation.state = state
	}
	coordinator.cancelSupersededLocked(observations)

	pendingWithoutObservation := false
	safetyPending := false
	for _, macAddr := range coordinator.selected {
		if observations[macAddr] != nil {
			continue
		}
		state, err := coordinator.states.LoadMiner(macAddr)
		if err != nil {
			if coordinator.gateOpen {
				return err
			}
			continue
		}
		if state.PendingKind == "" && !state.MiningPending {
			continue
		}
		pendingWithoutObservation = true
		if state.PendingKind == lib.MutationOverheatRecovery ||
			state.Phase == lib.PhaseCooldown {
			safetyPending = true
		}
	}
	for _, macAddr := range sortedObservationMACs(observations) {
		observation := observations[macAddr]
		if !safetyMutation(observation) {
			continue
		}
		safetyPending = true
		if coordinator.canStartLocked(observation, true) {
			coordinator.startLocked(ctx, observation, true, "", "")
		}
	}
	if safetyPending {
		return nil
	}
	if pendingWithoutObservation {
		return nil
	}
	for _, macAddr := range sortedObservationMACs(observations) {
		observation := observations[macAddr]
		if err := validateMutationIdentity(observation.info, macAddr); err != nil {
			coordinator.startupBlocked = observation.info.Hostname
			coordinator.logger.Printf(
				"Normal mutation blocked for %s because device identity is unsupported",
				observation.info.Hostname,
			)
			return nil
		}
	}
	for macAddr, expected := range coordinator.expectedHostnames {
		observation := observations[macAddr]
		if observation == nil {
			continue
		}
		if observation.info.Hostname != expected {
			coordinator.startupBlocked = expected
			coordinator.logger.Printf(
				"Normal mutation blocked because selected hostname %s changed",
				expected,
			)
			return nil
		}
	}

	for _, macAddr := range sortedObservationMACs(observations) {
		observation := observations[macAddr]
		if observation.state.PendingKind == "" {
			continue
		}
		if coordinator.normalActive == "" &&
			coordinator.canStartLocked(observation, false) {
			coordinator.startLocked(ctx, observation, false, "", "")
		}
		return nil
	}
	if coordinator.normalActive != "" || coordinator.startupBlocked != "" {
		return nil
	}

	if !coordinator.gateOpen {
		return coordinator.advanceStartupLocked(ctx, observations, now)
	}
	return nil
}

func (coordinator *mutationCoordinator) advanceStartupLocked(
	ctx context.Context,
	observations map[string]*minerObservation,
	now time.Time,
) error {
	if coordinator.startupIndex >= len(coordinator.selected) {
		return coordinator.openGateLocked(now)
	}
	macAddr := coordinator.selected[coordinator.startupIndex]
	observation := observations[macAddr]
	if observation == nil {
		coordinator.startupHealth[macAddr] = 0
		if coordinator.healthExpiredLocked(macAddr, now) {
			coordinator.blockHealthLocked(macAddr)
		}
		return nil
	}

	if operatingPointFromInfo(observation.info) != observation.state.CurrentPoint() ||
		observation.state.ObservedCount != 0 {
		coordinator.startupHealth[macAddr] = 0
		delete(coordinator.startupHealthSince, macAddr)
		return nil
	}

	mining := observation.settings.Mining
	forced := coordinator.reapply[observation.info.Hostname]
	drift := mining.Enabled && !miningReadbackMatches(observation.info, mining)
	if observation.state.MiningPending && !mining.Enabled {
		coordinator.startupBlocked = observation.info.Hostname
		coordinator.logger.Printf(
			"Mining startup blocked for %s: durable mining obligation requires mining to remain enabled",
			observation.info.Hostname,
		)
		return nil
	}
	if mining.Enabled && (observation.state.MiningPending || forced || drift) {
		coordinator.startupHealth[macAddr] = 0
		delete(coordinator.startupHealthSince, macAddr)
		if !observation.state.MiningPending {
			observation.state.MiningPending = true
			if err := coordinator.states.SaveMiner(&observation.state); err != nil {
				return fmt.Errorf(
					"persist mining configuration obligation for %s: %w",
					observation.info.Hostname,
					err,
				)
			}
		}
		delete(coordinator.reapply, observation.info.Hostname)
		primaryPassword, fallbackPassword, err := coordinator.resolveMiningPasswords(mining)
		if err != nil {
			coordinator.startupBlocked = observation.info.Hostname
			coordinator.logger.Printf(
				"Mining startup blocked for %s: %s",
				observation.info.Hostname,
				err,
			)
			return nil
		}
		if coordinator.canStartLocked(observation, false) {
			coordinator.startLocked(
				ctx,
				observation,
				false,
				primaryPassword,
				fallbackPassword,
			)
		}
		return nil
	}

	if now.Before(observation.state.RampUntil) ||
		!startupHealthy(observation.info, observation.settings) {
		coordinator.startupHealth[macAddr] = 0
		if now.Before(observation.state.RampUntil) {
			return nil
		}
		if coordinator.healthExpiredLocked(macAddr, now) {
			coordinator.blockHealthLocked(macAddr)
		}
		return nil
	}
	if coordinator.healthExpiredLocked(macAddr, now) {
		coordinator.blockHealthLocked(macAddr)
		return nil
	}
	coordinator.startupHealth[macAddr]++
	if coordinator.startupHealth[macAddr] < startupHealthyPolls {
		return nil
	}
	coordinator.startupIndex++
	return nil
}

func (coordinator *mutationCoordinator) healthExpiredLocked(
	macAddr string,
	now time.Time,
) bool {
	since := coordinator.startupHealthSince[macAddr]
	if since.IsZero() {
		coordinator.startupHealthSince[macAddr] = now
		return false
	}
	return now.Sub(since) >= coordinator.healthDeadline
}

func (coordinator *mutationCoordinator) blockHealthLocked(macAddr string) {
	state, err := coordinator.states.LoadMiner(macAddr)
	hostname := macAddr
	if err == nil {
		hostname = state.Hostname
	}
	coordinator.startupBlocked = hostname
	coordinator.logger.Printf("Startup health deadline expired for %s", hostname)
}

func (coordinator *mutationCoordinator) openGateLocked(now time.Time) error {
	for _, macAddr := range coordinator.selected {
		state, err := coordinator.states.LoadMiner(macAddr)
		if err != nil {
			return err
		}
		settings, err := coordinator.settings.ForHost(state.Hostname)
		if err != nil {
			return err
		}
		state.RampUntil = now.Add(settings.RampUpTime)
		state.PhaseStartedAt = now
		state.ObservedFrequency = 0
		state.ObservedCoreVoltage = 0
		state.ObservedCount = 0
		if err := coordinator.states.SaveMiner(&state); err != nil {
			return fmt.Errorf("open startup gate for %s: %w", state.Hostname, err)
		}
		coordinator.reset(macAddr)
	}
	coordinator.gateOpen = true
	coordinator.logger.Printf("Startup safety gate opened for %d miners", len(coordinator.selected))
	return nil
}

func (coordinator *mutationCoordinator) canStartLocked(
	observation *minerObservation,
	safety bool,
) bool {
	if observation == nil {
		return false
	}
	macAddr := observation.state.MacAddr
	if validateMutationIdentity(observation.info, macAddr) != nil {
		return false
	}
	if _, active := coordinator.active[macAddr]; active {
		return false
	}
	if !safety && coordinator.normalActive != "" {
		return false
	}
	if observation.state.PendingKind == lib.MutationOverheatRecovery &&
		!safeToRecover(observation.info, observation.settings) {
		return false
	}
	if observation.info.Temp <= 0 ||
		observation.info.VRTemp <= 0 ||
		observation.info.Power <= 0 {
		return false
	}
	if _, failed := instantaneousSafetyFailure(observation.info, observation.settings); failed {
		return false
	}
	if hasPowerFault(observation.info) {
		return false
	}
	return true
}

func (coordinator *mutationCoordinator) startLocked(
	ctx context.Context,
	observation *minerObservation,
	safety bool,
	primaryPassword string,
	fallbackPassword string,
) {
	kind := observation.state.PendingKind
	point := observation.state.PendingPoint()
	if kind == "" && observation.state.MiningPending {
		kind = lib.MutationMiningConfiguration
	}
	if kind == "" {
		return
	}
	coordinator.nextMutation++
	flowContext, cancel := context.WithCancel(ctx)
	active := &activeMutation{
		id:     coordinator.nextMutation,
		kind:   kind,
		point:  point,
		normal: !safety,
		cancel: cancel,
	}
	coordinator.active[observation.state.MacAddr] = active
	if active.normal {
		coordinator.normalActive = observation.state.MacAddr
	}
	request := mutationRequest{
		id:               active.id,
		macAddr:          observation.state.MacAddr,
		hostname:         observation.state.Hostname,
		ip:               observation.state.IP,
		kind:             kind,
		point:            point,
		info:             observation.info,
		settings:         observation.settings,
		primaryPassword:  primaryPassword,
		fallbackPassword: fallbackPassword,
	}
	go func() {
		result := coordinator.execute(flowContext, request)
		select {
		case coordinator.results <- result:
		case <-ctx.Done():
		}
	}()
}

func (coordinator *mutationCoordinator) execute(
	ctx context.Context,
	request mutationRequest,
) mutationResult {
	result := mutationResult{
		id:       request.id,
		macAddr:  request.macAddr,
		hostname: request.hostname,
		kind:     request.kind,
		point:    request.point,
	}
	fail := func(err error) mutationResult {
		result.err = redactMutationError(
			err,
			request.primaryPassword,
			request.fallbackPassword,
		)
		return result
	}

	if request.kind == lib.MutationOperatingPoint ||
		request.kind == lib.MutationOverheatRecovery {
		asic, err := coordinator.devices.GetASICSettings(ctx, request.ip)
		if err != nil {
			return fail(fmt.Errorf("read advertised operating points: %w", err))
		}
		if asic.ASICModel != supportedASICModel ||
			!operatingPointAdvertised(asic, request.point) {
			return fail(fmt.Errorf("pending operating point is not advertised by the supported ASIC"))
		}
	}
	preflight, err := coordinator.devices.GetSystemInfo(ctx, request.ip)
	if err != nil {
		return fail(fmt.Errorf("pre-PATCH information read failed: %w", err))
	}
	preflightObservedAt := coordinator.now()
	if err := validateMutationIdentity(preflight, request.macAddr); err != nil {
		return fail(err)
	}
	if operatingPointFromInfo(preflight) != operatingPointFromInfo(request.info) {
		return fail(fmt.Errorf("operating point changed before PATCH"))
	}
	if request.kind == lib.MutationMiningConfiguration &&
		!sameMiningReadback(preflight, request.info) {
		return fail(fmt.Errorf("readable mining settings changed before PATCH"))
	}
	if err := validateMutationSafety(preflight, request.settings, request.kind); err != nil {
		return fail(err)
	}
	current, err := coordinator.states.LoadMiner(request.macAddr)
	if err != nil {
		return fail(err)
	}
	if !mutationStillPending(current, request) {
		return fail(fmt.Errorf("durable mutation intent changed before PATCH"))
	}
	request.info = preflight

	switch request.kind {
	case lib.MutationOperatingPoint, lib.MutationOverheatRecovery:
		if request.kind == lib.MutationOverheatRecovery {
			err = coordinator.devices.PatchOverheatRecovery(ctx, request.point, request.ip)
		} else {
			err = coordinator.devices.PatchOperatingPoint(ctx, request.point, request.ip)
		}
		if err != nil {
			return fail(fmt.Errorf("operating-point PATCH failed: %w", err))
		}
	case lib.MutationMiningConfiguration:
		err = coordinator.devices.PatchMiningConfiguration(
			ctx,
			request.settings.Mining,
			request.primaryPassword,
			request.fallbackPassword,
			request.ip,
		)
		if err != nil {
			return fail(fmt.Errorf("mining configuration PATCH failed: %w", err))
		}
	default:
		return fail(fmt.Errorf("unsupported mutation kind %q", request.kind))
	}

	restartRequestedAt := coordinator.now()
	if err := coordinator.devices.Restart(ctx, request.ip); err != nil {
		return fail(fmt.Errorf("restart request was ambiguous: %w", err))
	}
	miner, err := coordinator.waitForVerifiedBoot(
		ctx,
		request,
		preflightObservedAt,
		restartRequestedAt,
	)
	if err != nil {
		return fail(err)
	}
	result.miner = miner
	return result
}

func (coordinator *mutationCoordinator) waitForVerifiedBoot(
	ctx context.Context,
	request mutationRequest,
	preflightObservedAt time.Time,
	restartRequestedAt time.Time,
) (lib.DiscoveredMiner, error) {
	deadline := restartRequestedAt.Add(coordinator.rebootDeadline)
	var verificationErr error
	for {
		if err := ctx.Err(); err != nil {
			return lib.DiscoveredMiner{}, err
		}
		miner, err := coordinator.discover(ctx, request.macAddr)
		if err == nil {
			if identityErr := validateMutationIdentity(miner.Info, request.macAddr); identityErr != nil {
				return lib.DiscoveredMiner{}, identityErr
			}
			elapsed := coordinator.now().Sub(preflightObservedAt)
			if proveNewBoot(request.info.UpTimeSeconds, miner.Info.UpTimeSeconds, elapsed) {
				if err := verifyMutationReadback(request, miner.Info); err != nil {
					verificationErr = err
					if !postRestartVerificationMaySettle(
						miner.Info,
						request.settings,
						request.kind,
					) {
						return lib.DiscoveredMiner{}, err
					}
				} else {
					return miner, nil
				}
			}
		} else if !errors.Is(err, errMinerNotFound) &&
			!errors.Is(err, context.Canceled) {
			return lib.DiscoveredMiner{}, fmt.Errorf("rediscover miner: %w", err)
		}
		if !coordinator.now().Before(deadline) {
			if verificationErr != nil {
				return lib.DiscoveredMiner{}, fmt.Errorf(
					"post-restart verification deadline expired: %w",
					verificationErr,
				)
			}
			return lib.DiscoveredMiner{}, fmt.Errorf("reboot proof deadline expired")
		}
		timer := time.NewTimer(coordinator.rediscoveryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return lib.DiscoveredMiner{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func postRestartVerificationMaySettle(
	info lib.Info,
	settings lib.Settings,
	kind lib.MutationKind,
) bool {
	if hasPowerFault(info) {
		return false
	}
	if info.OverHeatMode != 0 && kind != lib.MutationOverheatRecovery {
		return false
	}
	_, unsafe := instantaneousSafetyFailure(info, settings)
	return !unsafe
}

func (coordinator *mutationCoordinator) applyResultsLocked() (bool, error) {
	processed := false
	for {
		select {
		case result := <-coordinator.results:
			processed = true
			active := coordinator.active[result.macAddr]
			if active == nil || active.id != result.id {
				continue
			}
			delete(coordinator.active, result.macAddr)
			if active.normal && coordinator.normalActive == result.macAddr {
				coordinator.normalActive = ""
			}
			active.cancel()
			if result.err != nil {
				if result.kind == lib.MutationMiningConfiguration &&
					!errors.Is(result.err, context.Canceled) {
					coordinator.startupBlocked = result.hostname
				}
				if !errors.Is(result.err, context.Canceled) {
					coordinator.logger.Printf(
						"Mutation %s failed for %s: %s",
						result.kind,
						result.hostname,
						result.err,
					)
				}
				continue
			}
			if err := coordinator.completeMutationLocked(result); err != nil {
				if result.kind == lib.MutationMiningConfiguration {
					coordinator.startupBlocked = result.hostname
				}
				return processed, err
			}
		default:
			return processed, nil
		}
	}
}

func (coordinator *mutationCoordinator) completeMutationLocked(
	result mutationResult,
) error {
	state, err := coordinator.states.LoadMiner(result.macAddr)
	if err != nil {
		return err
	}
	switch result.kind {
	case lib.MutationOperatingPoint, lib.MutationOverheatRecovery:
		if state.PendingKind != result.kind || state.PendingPoint() != result.point {
			return nil
		}
		state.SetCurrentPoint(result.point)
		state.ClearPendingMutation()
		if state.Phase == lib.PhaseCooldown {
			state.Phase = lib.PhaseBaseline
		}
		if result.kind == lib.MutationOverheatRecovery {
			state.OverheatPending = false
			state.SetBestPoint(lib.OperatingPoint{})
			state.BestHashRate = 0
			state.SetFallbackPoint(lib.OperatingPoint{})
			state.Phase = lib.PhaseCooldown
		}
	case lib.MutationMiningConfiguration:
		if !state.MiningPending {
			return nil
		}
		state.MiningPending = false
	default:
		return fmt.Errorf("complete mutation: unsupported kind %q", result.kind)
	}
	settings, err := coordinator.settings.ForHost(state.Hostname)
	if err != nil {
		return err
	}
	completedAt := coordinator.now()
	state.IP = result.miner.IP
	state.RampUntil = completedAt.Add(settings.RampUpTime)
	state.PhaseStartedAt = completedAt
	state.ObservedFrequency = 0
	state.ObservedCoreVoltage = 0
	state.ObservedCount = 0
	if err := coordinator.states.SaveMiner(&state); err != nil {
		return fmt.Errorf("complete mutation for %s: %w", state.Hostname, err)
	}
	coordinator.routes[result.macAddr] = result.miner
	coordinator.startupHealth[result.macAddr] = 0
	delete(coordinator.startupHealthSince, result.macAddr)
	coordinator.reset(result.macAddr)
	coordinator.logger.Printf(
		"Mutation %s verified after restart for %s",
		result.kind,
		result.hostname,
	)
	return nil
}

func (coordinator *mutationCoordinator) cancelSupersededLocked(
	observations map[string]*minerObservation,
) {
	for macAddr, active := range coordinator.active {
		observation := observations[macAddr]
		if observation == nil {
			continue
		}
		if active.kind == lib.MutationMiningConfiguration {
			if !observation.state.MiningPending ||
				observation.state.PendingKind != "" {
				active.cancel()
			}
			continue
		}
		if observation.state.PendingKind != active.kind ||
			observation.state.PendingPoint() != active.point {
			active.cancel()
		}
	}
}

func (coordinator *mutationCoordinator) resolveMiningPasswords(
	settings lib.MiningSettings,
) (string, string, error) {
	primary, fallback, err := coordinator.resolvePasswords(settings)
	if err != nil {
		return "", "", fmt.Errorf("load passwords from .env: %w", err)
	}
	if err := validResolvedPassword(primary); err != nil {
		return "", "", fmt.Errorf("primary password entry is invalid")
	}
	if err := validResolvedPassword(fallback); err != nil {
		return "", "", fmt.Errorf("fallback password entry is invalid")
	}
	return primary, fallback, nil
}

func validResolvedPassword(password string) error {
	if password == "" {
		return fmt.Errorf("password is empty")
	}
	if !utf8.ValidString(password) || len([]byte(password)) > 255 {
		return fmt.Errorf("password must be valid UTF-8 and at most 255 bytes")
	}
	return nil
}

func validateMutationIdentity(info lib.Info, macAddr string) error {
	switch {
	case info.MacAddr != macAddr:
		return fmt.Errorf("device MAC changed from %s to %s", macAddr, info.MacAddr)
	case info.Version != supportedAxeOSVersion:
		return fmt.Errorf("unsupported AxeOS version %q", info.Version)
	case info.ASICModel != supportedASICModel:
		return fmt.Errorf("unsupported ASIC model %q", info.ASICModel)
	case info.BoardVersion != supportedBoardVersion:
		return fmt.Errorf("unsupported board version %q", info.BoardVersion)
	default:
		return nil
	}
}

func validateMutationSafety(
	info lib.Info,
	settings lib.Settings,
	kind lib.MutationKind,
) error {
	if info.Temp <= 0 || info.VRTemp <= 0 || info.Power <= 0 {
		return fmt.Errorf("device safety telemetry is incomplete")
	}
	if hasPowerFault(info) {
		return fmt.Errorf("device reports a power fault")
	}
	if info.OverHeatMode != 0 && kind != lib.MutationOverheatRecovery {
		return fmt.Errorf("device is in firmware overheat mode")
	}
	if _, failed := instantaneousSafetyFailure(info, settings); failed {
		return fmt.Errorf("device is outside hard safety limits")
	}
	if kind == lib.MutationOverheatRecovery && !safeToRecover(info, settings) {
		return fmt.Errorf("device is not cool enough for overheat recovery")
	}
	return nil
}

func verifyMutationReadback(request mutationRequest, info lib.Info) error {
	if err := validateMutationSafety(info, request.settings, request.kind); err != nil {
		return fmt.Errorf("post-restart safety verification failed: %w", err)
	}
	switch request.kind {
	case lib.MutationOperatingPoint, lib.MutationOverheatRecovery:
		if operatingPointFromInfo(info) != request.point {
			return fmt.Errorf("post-restart operating point does not match the pending pair")
		}
		if request.kind == lib.MutationOverheatRecovery && info.OverHeatMode != 0 {
			return fmt.Errorf("post-restart firmware overheat flag remains set")
		}
	case lib.MutationMiningConfiguration:
		if operatingPointFromInfo(info) != operatingPointFromInfo(request.info) {
			return fmt.Errorf("post-restart operating point changed during mining configuration")
		}
		if !miningReadbackMatches(info, request.settings.Mining) {
			return fmt.Errorf("post-restart mining readback does not match desired settings")
		}
		if info.IsUsingFallbackStratum != 0 {
			return fmt.Errorf("post-restart miner is using fallback Stratum")
		}
	}
	return nil
}

func mutationStillPending(state lib.MinerState, request mutationRequest) bool {
	if request.kind == lib.MutationMiningConfiguration {
		return state.MiningPending && state.PendingKind == ""
	}
	return state.PendingKind == request.kind && state.PendingPoint() == request.point
}

func safetyMutation(observation *minerObservation) bool {
	if observation == nil {
		return false
	}
	switch observation.state.PendingKind {
	case lib.MutationOverheatRecovery:
		return true
	case lib.MutationOperatingPoint:
		if observation.state.Phase == lib.PhaseCooldown {
			return true
		}
		_, unsafe := instantaneousSafetyFailure(observation.info, observation.settings)
		return unsafe
	default:
		return false
	}
}

func startupHealthy(info lib.Info, settings lib.Settings) bool {
	if hasPowerFault(info) ||
		info.OverHeatMode != 0 ||
		info.HashRate <= 0 ||
		info.Temp <= 0 ||
		info.VRTemp <= 0 ||
		info.Power <= 0 {
		return false
	}
	if _, failed := instantaneousSafetyFailure(info, settings); failed {
		return false
	}
	return !settings.Mining.Enabled || info.IsUsingFallbackStratum == 0
}

func hasPowerFault(info lib.Info) bool {
	return info.PowerFault != nil && *info.PowerFault != ""
}

func miningReadbackMatches(info lib.Info, settings lib.MiningSettings) bool {
	return info.StratumURL == settings.Primary.Host &&
		info.StratumPort == settings.Primary.Port &&
		info.StratumUser == settings.Primary.User &&
		info.FallbackStratumURL == settings.Fallback.Host &&
		info.FallbackStratumPort == settings.Fallback.Port &&
		info.FallbackStratumUser == settings.Fallback.User
}

func sameMiningReadback(left lib.Info, right lib.Info) bool {
	return left.StratumURL == right.StratumURL &&
		left.StratumPort == right.StratumPort &&
		left.StratumUser == right.StratumUser &&
		left.FallbackStratumURL == right.FallbackStratumURL &&
		left.FallbackStratumPort == right.FallbackStratumPort &&
		left.FallbackStratumUser == right.FallbackStratumUser
}

func proveNewBoot(preUptime int, postUptime int, elapsed time.Duration) bool {
	if preUptime < 0 || postUptime < 0 || elapsed < 0 {
		return false
	}
	uninterruptedMinimum := time.Duration(preUptime)*time.Second +
		elapsed -
		rebootUptimeTolerance
	return time.Duration(postUptime)*time.Second < uninterruptedMinimum
}

func sortedObservationMACs(
	observations map[string]*minerObservation,
) []string {
	macAddresses := make([]string, 0, len(observations))
	for macAddr := range observations {
		macAddresses = append(macAddresses, macAddr)
	}
	sort.Strings(macAddresses)
	return macAddresses
}

func redactMutationError(err error, secrets ...string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	message := err.Error()
	for _, secret := range secrets {
		if secret == "" || !strings.Contains(message, secret) {
			continue
		}
		if utf8.RuneCountInString(secret) < 4 {
			return errors.New("mutation failed after resolving secret-bearing configuration")
		}
		message = strings.ReplaceAll(message, secret, "[redacted]")
	}
	return errors.New(message)
}

func cloneBoolMap(values map[string]bool) map[string]bool {
	cloned := make(map[string]bool, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
