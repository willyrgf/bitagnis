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
	resumeHealth       map[string]mutationResumeHealth
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
	id        uint64
	attemptID int64
	kind      lib.MutationKind
	point     lib.OperatingPoint
	normal    bool
	cancel    context.CancelFunc
}

type mutationRequest struct {
	id        uint64
	attemptID int64
	macAddr   string
	hostname  string
	ip        string
	kind      lib.MutationKind
	point     lib.OperatingPoint
	info      lib.Info
	settings  lib.Settings

	primaryPassword  string
	fallbackPassword string
}

type mutationResult struct {
	id        uint64
	attemptID int64
	macAddr   string
	hostname  string
	kind      lib.MutationKind
	point     lib.OperatingPoint
	miner     lib.DiscoveredMiner
	err       error
}

type mutationResumeHealth struct {
	attemptID int64
	count     int
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
		resumeHealth:       make(map[string]mutationResumeHealth),
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
) (bool, error) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()

	appliedResult, err := coordinator.applyResultsLocked()
	if err != nil {
		return false, err
	}
	for macAddr, observation := range observations {
		state, err := coordinator.states.LoadMiner(macAddr)
		if err != nil {
			return false, err
		}
		observation.state = state
	}
	if err := coordinator.advanceMiningResumeLocked(observations, now); err != nil {
		return false, err
	}
	coordinator.cancelSupersededLocked(observations)

	pendingWithoutObservation := false
	safetyBlocked := false
	for _, macAddr := range coordinator.selected {
		state := lib.MinerState{}
		if observation := observations[macAddr]; observation != nil {
			state = observation.state
		} else {
			var err error
			state, err = coordinator.states.LoadMiner(macAddr)
			if err != nil {
				if coordinator.gateOpen {
					return false, err
				}
				continue
			}
		}
		if state.Phase == lib.PhaseOverheat ||
			state.PendingKind == lib.MutationSafetyRollback ||
			state.PendingKind == lib.MutationOverheatRecovery {
			safetyBlocked = true
		}
		if observations[macAddr] == nil &&
			(state.PendingKind != "" || state.MiningPending) {
			pendingWithoutObservation = true
		}
	}
	for _, kind := range []lib.MutationKind{
		lib.MutationOverheatRecovery,
		lib.MutationSafetyRollback,
	} {
		for _, macAddr := range sortedObservationMACs(observations) {
			observation := observations[macAddr]
			if observation.state.PendingKind != kind {
				continue
			}
			if !appliedResult && coordinator.canStartLocked(observation) {
				if err := coordinator.startLocked(ctx, observation, "", ""); err != nil {
					return false, err
				}
			}
		}
	}
	if safetyBlocked {
		return false, nil
	}
	if pendingWithoutObservation {
		return false, nil
	}
	if appliedResult {
		return false, nil
	}
	for _, macAddr := range sortedObservationMACs(observations) {
		observation := observations[macAddr]
		if err := validateMutationIdentity(observation.info, macAddr); err != nil {
			coordinator.startupBlocked = observation.info.Hostname
			coordinator.logger.Printf(
				"Normal mutation blocked for %s because device identity is unsupported",
				observation.info.Hostname,
			)
			return false, nil
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
			return false, nil
		}
	}

	for _, macAddr := range sortedObservationMACs(observations) {
		observation := observations[macAddr]
		if observation.state.PendingKind == "" {
			continue
		}
		if !appliedResult &&
			coordinator.normalActive == "" &&
			coordinator.canStartLocked(observation) {
			if err := coordinator.startLocked(ctx, observation, "", ""); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	if coordinator.normalActive != "" || coordinator.startupBlocked != "" {
		return false, nil
	}

	if !coordinator.gateOpen {
		if err := coordinator.advanceStartupLocked(ctx, observations, now); err != nil {
			return false, err
		}
	}
	return coordinator.gateOpen, nil
}

func (coordinator *mutationCoordinator) advanceMiningResumeLocked(
	observations map[string]*minerObservation,
	now time.Time,
) error {
	for _, macAddr := range sortedObservationMACs(observations) {
		attempt, pending, err := coordinator.states.PendingMutationResume(macAddr)
		if err != nil {
			return err
		}
		if !pending {
			delete(coordinator.resumeHealth, macAddr)
			continue
		}
		observation := observations[macAddr]
		healthy := !now.Before(attempt.CompletedAt) &&
			operatingPointFromInfo(observation.info) ==
				observation.state.CurrentPoint() &&
			observation.state.ObservedCount == 0 &&
			startupHealthy(observation.info, observation.settings)
		health := coordinator.resumeHealth[macAddr]
		if health.attemptID != attempt.ID {
			health = mutationResumeHealth{attemptID: attempt.ID}
		}
		if !healthy {
			health.count = 0
			coordinator.resumeHealth[macAddr] = health
			if now.Sub(attempt.CompletedAt) >= coordinator.healthDeadline {
				if err := coordinator.states.FailMutationAttempt(
					attempt.ID,
					lib.MutationFailureMiningResume,
					now,
				); err != nil {
					return err
				}
				delete(coordinator.resumeHealth, macAddr)
			}
			continue
		}
		health.count++
		if health.count < startupHealthyPolls {
			coordinator.resumeHealth[macAddr] = health
			continue
		}
		if err := coordinator.states.AdvanceMutationAttempt(
			attempt.ID,
			lib.MutationMilestoneMiningResumed,
			now,
		); err != nil {
			return err
		}
		delete(coordinator.resumeHealth, macAddr)
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
		if coordinator.canStartLocked(observation) {
			if err := coordinator.startLocked(
				ctx,
				observation,
				primaryPassword,
				fallbackPassword,
			); err != nil {
				return err
			}
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
	kind := observation.state.PendingKind
	if kind == "" && observation.state.MiningPending {
		kind = lib.MutationMiningConfiguration
	}
	if kind == "" {
		return false
	}
	normal := kind == lib.MutationOperatingPoint ||
		kind == lib.MutationMiningConfiguration
	if normal && coordinator.normalActive != "" {
		return false
	}
	if !completeSafetyTelemetry(observation.info) ||
		hasPowerFault(observation.info) {
		return false
	}
	minimum, err := minimumAdvertisedPoint(observation.asic)
	if err != nil {
		return false
	}
	if validateKindSafety(
		observation.info,
		observation.settings,
		observation.state,
		minimum,
	) != nil {
		return false
	}
	return true
}

func (coordinator *mutationCoordinator) startLocked(
	ctx context.Context,
	observation *minerObservation,
	primaryPassword string,
	fallbackPassword string,
) error {
	kind := observation.state.PendingKind
	point := observation.state.PendingPoint()
	if kind == "" && observation.state.MiningPending {
		kind = lib.MutationMiningConfiguration
	}
	if kind == "" {
		return nil
	}
	startedAt := coordinator.now()
	intentCreatedAt := observation.state.PendingSince
	if intentCreatedAt.IsZero() {
		intentCreatedAt = startedAt
	}
	target := point
	if kind == lib.MutationMiningConfiguration {
		target = lib.OperatingPoint{}
	}
	attempt := lib.MutationAttempt{
		MacAddr:           observation.state.MacAddr,
		Kind:              kind,
		FromFrequency:     observation.info.Frequency,
		FromCoreVoltage:   observation.info.CoreVoltage,
		TargetFrequency:   target.Frequency,
		TargetCoreVoltage: target.CoreVoltage,
		IntentCreatedAt:   intentCreatedAt,
		StartedAt:         startedAt,
	}
	attemptID, err := coordinator.states.StartMutationAttempt(&attempt)
	if err != nil {
		return fmt.Errorf(
			"persist mutation attempt for %s: %w",
			observation.state.Hostname,
			err,
		)
	}
	coordinator.nextMutation++
	flowContext, cancel := context.WithCancel(ctx)
	active := &activeMutation{
		id:        coordinator.nextMutation,
		attemptID: attemptID,
		kind:      kind,
		point:     point,
		normal: kind == lib.MutationOperatingPoint ||
			kind == lib.MutationMiningConfiguration,
		cancel: cancel,
	}
	coordinator.active[observation.state.MacAddr] = active
	if active.normal {
		coordinator.normalActive = observation.state.MacAddr
	}
	request := mutationRequest{
		id:               active.id,
		attemptID:        attemptID,
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
	return nil
}

func (coordinator *mutationCoordinator) execute(
	ctx context.Context,
	request mutationRequest,
) mutationResult {
	result := mutationResult{
		id:        request.id,
		attemptID: request.attemptID,
		macAddr:   request.macAddr,
		hostname:  request.hostname,
		kind:      request.kind,
		point:     request.point,
	}
	fail := func(stage lib.MutationFailureStage, err error) mutationResult {
		if errors.Is(err, context.Canceled) {
			stage = lib.MutationFailureCanceled
		}
		if historyErr := coordinator.states.FailMutationAttempt(
			request.attemptID,
			stage,
			coordinator.now(),
		); historyErr != nil {
			err = fmt.Errorf("%w; persist mutation failure: %v", err, historyErr)
		}
		result.err = redactMutationError(
			err,
			request.primaryPassword,
			request.fallbackPassword,
		)
		return result
	}

	var asic lib.ASICSettings
	if request.kind == lib.MutationOperatingPoint ||
		request.kind == lib.MutationSafetyRollback ||
		request.kind == lib.MutationOverheatRecovery {
		var err error
		asic, err = coordinator.devices.GetASICSettings(ctx, request.ip)
		if err != nil {
			return fail(
				lib.MutationFailurePreflight,
				fmt.Errorf("read advertised operating points: %w", err),
			)
		}
		if asic.ASICModel != supportedASICModel ||
			!operatingPointAdvertised(asic, request.point) {
			return fail(
				lib.MutationFailurePreflight,
				fmt.Errorf("pending operating point is not advertised by the supported ASIC"),
			)
		}
	}
	preflight, err := coordinator.devices.GetSystemInfo(ctx, request.ip)
	if err != nil {
		return fail(
			lib.MutationFailurePreflight,
			fmt.Errorf("pre-PATCH information read failed: %w", err),
		)
	}
	preflightObservedAt := coordinator.now()
	if err := validateMutationIdentity(preflight, request.macAddr); err != nil {
		return fail(lib.MutationFailurePreflight, err)
	}
	if operatingPointFromInfo(preflight) != operatingPointFromInfo(request.info) {
		return fail(
			lib.MutationFailurePreflight,
			fmt.Errorf("operating point changed before PATCH"),
		)
	}
	if request.kind == lib.MutationMiningConfiguration &&
		!sameMiningReadback(preflight, request.info) {
		return fail(
			lib.MutationFailurePreflight,
			fmt.Errorf("readable mining settings changed before PATCH"),
		)
	}
	current, err := coordinator.states.LoadMiner(request.macAddr)
	if err != nil {
		return fail(lib.MutationFailurePreflight, err)
	}
	if !mutationStillPending(current, request) {
		return fail(
			lib.MutationFailurePreflight,
			fmt.Errorf("durable mutation intent changed before PATCH"),
		)
	}
	if request.kind != lib.MutationOverheatRecovery &&
		(preflight.OverHeatMode != 0 || knownFirmwareTripExceeded(preflight)) {
		if len(asic.FrequencyOptions) == 0 || len(asic.VoltageOptions) == 0 {
			asic, err = coordinator.devices.GetASICSettings(ctx, request.ip)
			if err != nil {
				return fail(
					lib.MutationFailurePreflight,
					fmt.Errorf(
						"read advertised operating points for emergency supersession: %w",
						err,
					),
				)
			}
		}
		if asic.ASICModel != supportedASICModel {
			return fail(
				lib.MutationFailurePreflight,
				fmt.Errorf("emergency supersession ASIC identity is unsupported"),
			)
		}
		minimum, err := minimumAdvertisedPoint(asic)
		if err != nil {
			return fail(lib.MutationFailurePreflight, err)
		}
		assessment := assessInstantaneousSafety(
			preflight,
			request.settings,
			operatingPointFromInfo(preflight),
			minimum,
		)
		if _, err := transitionEmergencyState(
			&current,
			preflight,
			asic,
			request.settings,
			coordinator.now(),
			assessment,
			false,
		); err != nil {
			return fail(lib.MutationFailurePreflight, err)
		}
		if err := coordinator.states.SaveMiner(&current); err != nil {
			return fail(
				lib.MutationFailurePreflight,
				fmt.Errorf("persist preflight emergency supersession: %w", err),
			)
		}
		coordinator.reset(request.macAddr)
		return fail(
			lib.MutationFailurePreflight,
			fmt.Errorf("preflight emergency superseded the pending mutation"),
		)
	}
	if err := coordinator.validateMutationPreflight(
		preflight,
		request.settings,
		current,
		asic,
	); err != nil {
		return fail(lib.MutationFailurePreflight, err)
	}
	request.info = preflight
	if err := coordinator.states.AdvanceMutationAttempt(
		request.attemptID,
		lib.MutationMilestonePatchRequested,
		coordinator.now(),
	); err != nil {
		return fail(
			lib.MutationFailurePreflight,
			fmt.Errorf("persist pre-PATCH mutation milestone: %w", err),
		)
	}

	switch request.kind {
	case lib.MutationOperatingPoint,
		lib.MutationSafetyRollback,
		lib.MutationOverheatRecovery:
		if request.kind == lib.MutationOverheatRecovery {
			err = coordinator.devices.PatchOverheatRecovery(ctx, request.point, request.ip)
		} else {
			err = coordinator.devices.PatchOperatingPoint(ctx, request.point, request.ip)
		}
		if err != nil {
			return fail(
				lib.MutationFailurePatch,
				fmt.Errorf("operating-point PATCH failed: %w", err),
			)
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
			return fail(
				lib.MutationFailurePatch,
				fmt.Errorf("mining configuration PATCH failed: %w", err),
			)
		}
	default:
		return fail(
			lib.MutationFailurePreflight,
			fmt.Errorf("unsupported mutation kind %q", request.kind),
		)
	}

	restartRequestedAt := coordinator.now()
	if err := coordinator.states.AdvanceMutationAttempt(
		request.attemptID,
		lib.MutationMilestoneRestartRequested,
		restartRequestedAt,
	); err != nil {
		return fail(
			lib.MutationFailureRestart,
			fmt.Errorf("persist pre-restart mutation milestone: %w", err),
		)
	}
	if err := coordinator.devices.Restart(ctx, request.ip); err != nil {
		return fail(
			lib.MutationFailureRestart,
			fmt.Errorf("restart request was ambiguous: %w", err),
		)
	}
	miner, err := coordinator.waitForVerifiedBoot(
		ctx,
		request,
		preflightObservedAt,
		restartRequestedAt,
	)
	if err != nil {
		return fail(lib.MutationFailureRebootVerification, err)
	}
	if err := coordinator.states.AdvanceMutationAttempt(
		request.attemptID,
		lib.MutationMilestoneRebootVerified,
		coordinator.now(),
	); err != nil {
		return fail(
			lib.MutationFailureRebootVerification,
			fmt.Errorf("persist reboot verification milestone: %w", err),
		)
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
	switch kind {
	case lib.MutationOperatingPoint, lib.MutationMiningConfiguration:
		if !completeSafetyTelemetry(info) {
			return true
		}
		assessment := assessInstantaneousSafety(
			info,
			settings,
			operatingPointFromInfo(info),
			lib.OperatingPoint{},
		)
		return assessment.action == safetyNormal
	case lib.MutationSafetyRollback:
		return info.OverHeatMode == 0
	case lib.MutationOverheatRecovery:
		return true
	default:
		return false
	}
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
				if historyErr := coordinator.states.FailMutationAttempt(
					result.attemptID,
					lib.MutationFailureCompletion,
					coordinator.now(),
				); historyErr != nil {
					err = fmt.Errorf("%w; persist completion failure: %v", err, historyErr)
				}
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
	case lib.MutationOperatingPoint,
		lib.MutationSafetyRollback,
		lib.MutationOverheatRecovery:
		if state.PendingKind != result.kind || state.PendingPoint() != result.point {
			return coordinator.states.FailMutationAttempt(
				result.attemptID,
				lib.MutationFailureCanceled,
				coordinator.now(),
			)
		}
		state.SetCurrentPoint(result.point)
		state.ClearPendingMutation()
		if result.kind == lib.MutationOverheatRecovery {
			state.SetFallbackPoint(lib.OperatingPoint{})
			state.Phase = lib.PhaseCooldown
		}
	case lib.MutationMiningConfiguration:
		if !state.MiningPending {
			return coordinator.states.FailMutationAttempt(
				result.attemptID,
				lib.MutationFailureCanceled,
				coordinator.now(),
			)
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
	if result.kind != lib.MutationSafetyRollback {
		state.PhaseStartedAt = completedAt
	}
	state.ObservedFrequency = 0
	state.ObservedCoreVoltage = 0
	state.ObservedCount = 0
	if err := coordinator.states.CompleteMutationAttempt(
		&state,
		result.attemptID,
		completedAt,
	); err != nil {
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

func validateKindSafety(
	info lib.Info,
	settings lib.Settings,
	state lib.MinerState,
	minimum lib.OperatingPoint,
) error {
	if !completeSafetyTelemetry(info) {
		return fmt.Errorf("device safety telemetry is incomplete")
	}
	if hasPowerFault(info) {
		return fmt.Errorf("device reports a power fault")
	}
	kind := state.PendingKind
	if kind == "" && state.MiningPending {
		kind = lib.MutationMiningConfiguration
	}
	switch kind {
	case lib.MutationOperatingPoint, lib.MutationMiningConfiguration:
		assessment := assessInstantaneousSafety(
			info,
			settings,
			operatingPointFromInfo(info),
			minimum,
		)
		if assessment.action != safetyNormal {
			return fmt.Errorf("device is outside hard safety limits")
		}
	case lib.MutationSafetyRollback:
		if state.PendingPoint() == operatingPointFromInfo(info) {
			return fmt.Errorf("safety target is already the configured live pair")
		}
		switch state.Phase {
		case lib.PhaseCooldown:
			if info.OverHeatMode != 0 {
				return fmt.Errorf("device is in firmware overheat mode")
			}
			if info.Temp >= settings.TempCutoff {
				return fmt.Errorf("ASIC temperature reached the host cutoff")
			}
			if knownFirmwareTripExceeded(info) {
				return fmt.Errorf("telemetry exceeded a known AxeOS firmware trip boundary")
			}
		case lib.PhaseOverheat:
			if info.OverHeatMode != 0 {
				return fmt.Errorf("device is in firmware overheat mode")
			}
			if knownFirmwareTripExceeded(info) {
				return fmt.Errorf("telemetry exceeded a known AxeOS firmware trip boundary")
			}
			if state.PendingPoint() != minimum {
				return fmt.Errorf("host containment target is not the exact advertised minimum")
			}
		default:
			return fmt.Errorf("safety rollback has no durable safety phase")
		}
	case lib.MutationOverheatRecovery:
		if state.Phase != lib.PhaseOverheat {
			return fmt.Errorf("overheat recovery has no durable emergency episode")
		}
		if state.PendingPoint() != minimum {
			return fmt.Errorf("overheat recovery target is not the exact advertised minimum")
		}
		if !safeToRecover(info, settings) {
			return fmt.Errorf("device is not cool enough for overheat recovery")
		}
	default:
		return fmt.Errorf("unsupported mutation kind %q", kind)
	}
	return nil
}

func (coordinator *mutationCoordinator) validateMutationPreflight(
	info lib.Info,
	settings lib.Settings,
	state lib.MinerState,
	asic lib.ASICSettings,
) error {
	minimum := lib.OperatingPoint{}
	if state.PendingKind != "" {
		var err error
		minimum, err = minimumAdvertisedPoint(asic)
		if err != nil {
			return err
		}
	}
	if err := validateKindSafety(info, settings, state, minimum); err != nil {
		return err
	}
	if state.PendingKind != lib.MutationSafetyRollback ||
		state.Phase != lib.PhaseCooldown ||
		state.PendingPoint() == minimum {
		return nil
	}
	records, err := coordinator.states.ListPoints(state.MacAddr)
	if err != nil {
		return fmt.Errorf("read safety rollback evidence: %w", err)
	}
	for _, record := range records {
		if record.Point() == state.PendingPoint() &&
			rollbackRecordEligible(
				record,
				operatingPointFromInfo(info),
				asic,
				settings,
			) {
			return nil
		}
	}
	return fmt.Errorf("safety rollback target lacks current complete validated evidence")
}

func verifyMutationReadback(request mutationRequest, info lib.Info) error {
	if !completeSafetyTelemetry(info) {
		return fmt.Errorf("post-restart safety telemetry is incomplete")
	}
	if hasPowerFault(info) {
		return fmt.Errorf("post-restart device reports a power fault")
	}
	switch request.kind {
	case lib.MutationOperatingPoint,
		lib.MutationSafetyRollback,
		lib.MutationOverheatRecovery:
		if operatingPointFromInfo(info) != request.point {
			return fmt.Errorf("post-restart operating point does not match the pending pair")
		}
		if request.kind == lib.MutationOperatingPoint {
			assessment := assessInstantaneousSafety(
				info,
				request.settings,
				operatingPointFromInfo(info),
				lib.OperatingPoint{},
			)
			if assessment.action != safetyNormal {
				return fmt.Errorf("post-restart device is outside hard safety limits")
			}
		} else if request.kind == lib.MutationSafetyRollback &&
			info.OverHeatMode != 0 {
			return fmt.Errorf("post-restart firmware overheat mode is active")
		} else if request.kind == lib.MutationOverheatRecovery &&
			info.OverHeatMode != 0 {
			return fmt.Errorf("post-restart firmware overheat flag remains set")
		}
	case lib.MutationMiningConfiguration:
		assessment := assessInstantaneousSafety(
			info,
			request.settings,
			operatingPointFromInfo(info),
			lib.OperatingPoint{},
		)
		if assessment.action != safetyNormal {
			return fmt.Errorf("post-restart device is outside hard safety limits")
		}
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

func startupHealthy(info lib.Info, settings lib.Settings) bool {
	if hasPowerFault(info) ||
		info.OverHeatMode != 0 ||
		info.HashRate <= 0 ||
		info.Temp <= 0 ||
		info.VRTemp <= 0 ||
		info.Power <= 0 {
		return false
	}
	assessment := assessInstantaneousSafety(
		info,
		settings,
		operatingPointFromInfo(info),
		lib.OperatingPoint{},
	)
	if assessment.action != safetyNormal {
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
