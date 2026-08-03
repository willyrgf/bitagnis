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
	retuneHost         string
	retuneFirstSeen    time.Time
	retuneHealthyCount int
	retuneRefused      bool

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
	id                   uint64
	attemptID            int64
	macAddr              string
	hostname             string
	ip                   string
	kind                 lib.MutationKind
	point                lib.OperatingPoint
	info                 lib.Info
	settings             lib.Settings
	attempt              lib.MutationAttempt
	reconcileOnly        bool
	bootProofSameProcess bool

	primaryPassword  string
	fallbackPassword string
}

type mutationResult struct {
	id                  uint64
	attemptID           int64
	macAddr             string
	hostname            string
	kind                lib.MutationKind
	point               lib.OperatingPoint
	miner               lib.DiscoveredMiner
	err                 error
	failureStage        lib.MutationFailureStage
	readbackUnavailable bool
	stateReconciled     bool
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
	retuneHost string,
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
		retuneHost:         retuneHost,
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
	if err := coordinator.cancelSupersededLocked(observations, now); err != nil {
		return false, err
	}

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
		if state.Phase == lib.PhaseOverheat || state.Phase == lib.PhaseCooldown ||
			state.PendingKind == lib.MutationSafetyRollback ||
			state.PendingKind == lib.MutationOverheatRecovery {
			safetyBlocked = true
		}
		if observations[macAddr] == nil &&
			(state.PendingKind != "" || state.MiningPending) {
			pendingWithoutObservation = true
		}
		if _, pendingResume, err := coordinator.states.PendingMutationResume(macAddr); err != nil {
			return false, err
		} else if pendingResume {
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
	if coordinator.retuneHost != "" {
		if _, err := coordinator.advanceRetuneLocked(observations, now); err != nil {
			return false, err
		}
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

func (coordinator *mutationCoordinator) advanceRetuneLocked(
	observations map[string]*minerObservation,
	now time.Time,
) (bool, error) {
	var observation *minerObservation
	for _, candidate := range observations {
		if candidate.info.Hostname == coordinator.retuneHost {
			if observation != nil {
				coordinator.logger.Printf("Retune refused for %s: hostname maps to multiple observations", coordinator.retuneHost)
				coordinator.retuneHost = ""
				return false, nil
			}
			observation = candidate
		}
	}
	if observation == nil {
		return false, nil
	}
	if coordinator.retuneFirstSeen.IsZero() {
		coordinator.retuneFirstSeen = now
	}
	if now.Sub(coordinator.retuneFirstSeen) >= 3*time.Minute {
		if !coordinator.retuneRefused {
			coordinator.logger.Printf("Retune refused for %s: qualification deadline expired", coordinator.retuneHost)
			coordinator.retuneRefused = true
		}
		coordinator.retuneHost = ""
		return false, nil
	}
	if coordinator.normalActive != "" || len(coordinator.active) != 0 {
		return false, nil
	}
	state := observation.state
	if state.Phase != lib.PhaseHold || state.HoldReason == lib.HoldBlocked || state.HoldReason == lib.HoldSafety ||
		state.SafetyReason != "" || state.SettledAt.IsZero() ||
		state.PendingKind != "" || state.MiningPending || now.Before(state.CooldownUntil) ||
		state.Phase == lib.PhaseOverheat || state.SafetyReason == lib.SafetyReasonFirmwareOverheat ||
		state.SafetyReason == lib.SafetyReasonFirmwareTrip || state.SafetyReason == lib.SafetyReasonMutationUncertain {
		coordinator.retuneHealthyCount = 0
		return false, nil
	}
	if observation.info.MacAddr != state.MacAddr || operatingPointFromInfo(observation.info) != state.CurrentPoint() ||
		canonicalASICGrid(observation.asic) != nil || !operatingPointAdvertised(observation.asic, operatingPointFromInfo(observation.info)) ||
		!startupHealthy(observation.info, observation.settings) {
		coordinator.retuneHealthyCount = 0
		return false, nil
	}
	attempt, unfinished, err := coordinator.states.UnfinishedMutationAttempt(state.MacAddr)
	if err != nil {
		return false, err
	}
	if unfinished || attempt.ID != 0 {
		coordinator.retuneHealthyCount = 0
		return false, nil
	}
	coordinator.retuneHealthyCount++
	if coordinator.retuneHealthyCount < startupHealthyPolls {
		return false, nil
	}
	// The coordinator mutex serializes this second qualifying observation
	// with all controller state changes; ResetOptimizationPass then rechecks
	// the durable state in its own transaction before deleting point history.
	startedAt := now
	rampUntil := startedAt.Add(observation.settings.RampUpTime)
	deadline := rampUntil.Add(4 * observation.settings.EvaluationWindowTime)
	if err := coordinator.states.ResetOptimizationPass(state.MacAddr, state.CurrentPoint(), startedAt, rampUntil, deadline); err != nil {
		return false, fmt.Errorf("retune %s: %w", coordinator.retuneHost, err)
	}
	coordinator.reset(state.MacAddr)
	coordinator.logger.Printf("Accepted retune for %s; starting a new finite pass", coordinator.retuneHost)
	coordinator.retuneHost = ""
	coordinator.retuneHealthyCount = 0
	coordinator.retuneFirstSeen = time.Time{}
	return true, nil
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
		if observation == nil {
			health := coordinator.resumeHealth[macAddr]
			if health.attemptID != attempt.ID {
				health = mutationResumeHealth{attemptID: attempt.ID}
			}
			health.count = 0
			coordinator.resumeHealth[macAddr] = health
			if now.Sub(attempt.CompletedAt) >= coordinator.healthDeadline {
				state, loadErr := coordinator.states.LoadMiner(macAddr)
				if loadErr != nil {
					return loadErr
				}
				if err := coordinator.handleMutationResumeFailureLocked(
					&minerObservation{state: state},
					attempt,
					now,
				); err != nil {
					return err
				}
				delete(coordinator.resumeHealth, macAddr)
			}
			continue
		}
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
				if err := coordinator.handleMutationResumeFailureLocked(
					observation,
					attempt,
					now,
				); err != nil {
					return err
				}
				delete(coordinator.resumeHealth, macAddr)
			}
			continue
		}
		health.count++
		if attempt.FirstPositiveAt.IsZero() {
			if err := coordinator.states.RecordFirstPositive(attempt.ID, now); err != nil {
				return err
			}
		}
		if health.count < startupHealthyPolls {
			coordinator.resumeHealth[macAddr] = health
			continue
		}
		settings := observation.settings
		rampUntil := now.Add(settings.RampUpTime)
		windowCount := 1
		if observation.state.Phase == lib.PhaseBaseline ||
			observation.state.Phase == lib.PhaseUndervolt ||
			observation.state.Phase == lib.PhaseFrequencyTest ||
			observation.state.Phase == lib.PhaseVoltageTest {
			windowCount = 2
		} else if observation.state.Phase == lib.PhaseHold &&
			(observation.state.HoldReason == lib.HoldOptimized || observation.state.HoldReason == lib.HoldManual) {
			windowCount = 1
		}
		deadlineBase := rampUntil
		if observation.state.CooldownUntil.After(deadlineBase) {
			deadlineBase = observation.state.CooldownUntil
		}
		deadline := deadlineBase.Add(time.Duration(windowCount*2) * settings.EvaluationWindowTime)
		if err := coordinator.states.CompleteMiningResume(
			&observation.state,
			attempt.ID,
			now,
			rampUntil,
			deadline,
		); err != nil {
			return err
		}
		delete(coordinator.resumeHealth, macAddr)
	}
	return nil
}

func (coordinator *mutationCoordinator) handleMutationResumeFailureLocked(
	observation *minerObservation,
	attempt lib.MutationAttempt,
	now time.Time,
) error {
	if observation == nil {
		return nil
	}
	state := &observation.state
	if attempt.Kind == lib.MutationMiningConfiguration {
		state.MiningPending = true
		coordinator.startupBlocked = state.Hostname
		return coordinator.states.FailMutationAndSave(state, attempt.ID, lib.MutationFailureMiningResume, now)
	}
	if attempt.Kind == lib.MutationOperatingPoint {
		records, err := coordinator.states.ListPoints(state.MacAddr)
		if err != nil {
			return err
		}
		if state.Phase == lib.PhaseUndervolt || state.Phase == lib.PhaseFrequencyTest || state.Phase == lib.PhaseVoltageTest {
			if entered, found := findRecord(records, attempt.TargetPoint()); found && entered.EntryAttemptID == attempt.ID {
				measuredAt := now
				if measuredAt.Before(entered.EnteredAt) {
					measuredAt = entered.EnteredAt
				}
				record := entered
				record.Status = lib.PointUnstable
				record.MeasuredAt = measuredAt
				record.MedianHash = 0
				record.ExpectedHash = 0
				record.Attainment = 0
				record.MeanTemp = 0
				record.P95Temp = 0
				record.P95VRTemp = 0
				record.P95Power = 0
				record.ErrorPercent = nil
				record.AcceptedDelta = 0
				record.RejectedDelta = 0
				if err := coordinator.states.FailMutationAndFinalizeTrial(
					state, record, lib.TrialReturn, attempt.ID,
					lib.MutationFailureMiningResume, now, time.Time{}, time.Time{},
				); err != nil {
					return err
				}
				return nil
			}
		}
	}
	if attempt.Kind == lib.MutationSafetyRollback || attempt.Kind == lib.MutationOverheatRecovery {
		state.ClearPendingMutation()
		state.SetFallbackPoint(lib.OperatingPoint{})
		state.Phase = lib.PhaseCooldown
		state.HoldReason = ""
		state.SettledAt = time.Time{}
		state.EvidenceDeadlineAt = time.Time{}
		return coordinator.states.FailMutationAndSave(state, attempt.ID, lib.MutationFailureMiningResume, now)
	}
	state.ClearPendingMutation()
	state.SetFallbackPoint(lib.OperatingPoint{})
	state.Phase = lib.PhaseHold
	state.HoldReason = lib.HoldBlocked
	state.SettledAt = time.Time{}
	state.EvidenceDeadlineAt = time.Time{}
	coordinator.startupBlocked = state.Hostname
	return coordinator.states.FailMutationAndSave(state, attempt.ID, lib.MutationFailureMiningResume, now)
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
			delete(coordinator.reapply, observation.info.Hostname)
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
	_ = now
	for _, macAddr := range coordinator.selected {
		state, err := coordinator.states.LoadMiner(macAddr)
		if err != nil {
			return err
		}
		// Bootstrap, mutation completion, and healthy resumption own the
		// absolute ramp/deadline pair. Opening the fleet gate only authorizes
		// decisions; it must not extend an evidence deadline while another
		// miner is reconciling.
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
	attempt, unfinished, err := coordinator.states.UnfinishedMutationAttempt(macAddr)
	if err != nil {
		return false
	}
	if unfinished {
		if attempt.CompletedAt.IsZero() == false {
			return false
		}
		if attempt.Kind != kind || attempt.TargetPoint() != observation.state.PendingPoint() && kind != lib.MutationMiningConfiguration {
			return false
		}
	} else if kind == lib.MutationMiningConfiguration && observation.state.MiningPending &&
		!coordinator.reapply[observation.info.Hostname] {
		attempts, listErr := coordinator.states.ListMutationAttempts(macAddr)
		if listErr != nil {
			return false
		}
		for index := len(attempts) - 1; index >= 0; index-- {
			candidate := attempts[index]
			if candidate.Kind != lib.MutationMiningConfiguration {
				continue
			}
			if !candidate.FailedAt.IsZero() && candidate.MiningResumedAt.IsZero() {
				return false
			}
			break
		}
	}
	if kind == lib.MutationOperatingPoint || kind == lib.MutationMiningConfiguration ||
		kind == lib.MutationSafetyRollback || kind == lib.MutationOverheatRecovery {
		if canonicalASICGrid(observation.asic) != nil {
			return false
		}
	}
	minimum, err := minimumAdvertisedPoint(observation.asic)
	if err != nil {
		return false
	}
	if validateKindSafety(observation.info, observation.settings, observation.state, minimum) != nil {
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
	attempt, unfinished, err := coordinator.states.UnfinishedMutationAttempt(observation.state.MacAddr)
	if err != nil {
		return fmt.Errorf("load mutation attempt for %s: %w", observation.state.Hostname, err)
	}
	if unfinished {
		if attempt.Kind != kind || (kind != lib.MutationMiningConfiguration && attempt.TargetPoint() != target) {
			return fmt.Errorf("unfinished mutation attempt does not match durable intent for %s", observation.state.Hostname)
		}
	} else {
		from := observation.info.Frequency
		fromVoltage := observation.info.CoreVoltage
		if kind == lib.MutationSafetyRollback || kind == lib.MutationOverheatRecovery {
			from = observation.state.CurrentFrequency
			fromVoltage = observation.state.CurrentCoreVoltage
			if !validLivePoint(lib.OperatingPoint{Frequency: from, CoreVoltage: fromVoltage}) || from == 50 {
				from = observation.info.Frequency
				fromVoltage = observation.info.CoreVoltage
			}
			if !validLivePoint(lib.OperatingPoint{Frequency: from, CoreVoltage: fromVoltage}) || from == 50 {
				if minimum, minimumErr := minimumAdvertisedPoint(observation.asic); minimumErr == nil {
					from = minimum.Frequency
					fromVoltage = minimum.CoreVoltage
				}
			}
		}
		attempt = lib.MutationAttempt{
			MacAddr:                         observation.state.MacAddr,
			Kind:                            kind,
			Reason:                          mutationReason(observation.state, kind),
			FromFrequency:                   from,
			FromCoreVoltage:                 fromVoltage,
			TargetFrequency:                 target.Frequency,
			TargetCoreVoltage:               target.CoreVoltage,
			IntentCreatedAt:                 intentCreatedAt,
			StartedAt:                       startedAt,
			ConfiguredVerifiedUptimeSeconds: -1,
		}
		if attemptID, startErr := coordinator.states.StartMutationAttempt(&attempt); startErr != nil {
			return fmt.Errorf(
				"persist mutation attempt for %s: %w",
				observation.state.Hostname,
				startErr,
			)
		} else {
			attempt.ID = attemptID
		}
	}
	attemptID := attempt.ID
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
		id:                   active.id,
		attemptID:            attemptID,
		macAddr:              observation.state.MacAddr,
		hostname:             observation.state.Hostname,
		ip:                   observation.state.IP,
		kind:                 kind,
		point:                point,
		info:                 observation.info,
		settings:             observation.settings,
		attempt:              attempt,
		reconcileOnly:        !attempt.RebootVerifiedAt.IsZero(),
		bootProofSameProcess: !unfinished,
		primaryPassword:      primaryPassword,
		fallbackPassword:     fallbackPassword,
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

func mutationReason(state lib.MinerState, kind lib.MutationKind) lib.SafetyReason {
	if kind == lib.MutationSafetyRollback || kind == lib.MutationOverheatRecovery {
		if state.SafetyReason != "" {
			return state.SafetyReason
		}
		return lib.SafetyReasonMutationUncertain
	}
	return ""
}

func (coordinator *mutationCoordinator) execute(
	ctx context.Context,
	request mutationRequest,
) mutationResult {
	result := mutationResult{
		id: request.id, attemptID: request.attemptID, macAddr: request.macAddr,
		hostname: request.hostname, kind: request.kind, point: request.point,
	}
	finish := func(stage lib.MutationFailureStage, terminal bool, err error) mutationResult {
		if terminal {
			result.failureStage = stage
		}
		result.err = redactMutationError(err, request.primaryPassword, request.fallbackPassword)
		return result
	}
	open := func(stage lib.MutationFailureStage, err error) mutationResult {
		return finish(stage, false, err)
	}
	terminal := func(stage lib.MutationFailureStage, err error) mutationResult {
		return finish(stage, true, err)
	}
	if err := ctx.Err(); err != nil {
		return open(lib.MutationFailurePreflight, err)
	}
	if request.reconcileOnly {
		result.miner = lib.DiscoveredMiner{IP: request.ip, Info: request.info}
		return result
	}

	var asic lib.ASICSettings
	// Once PATCH is durable, preflight is complete. The device may be
	// unavailable while configured readback or the already-issued restart is
	// reconciled, so each durable stage must own its absolute deadline.
	prePatchStage := request.attempt.PatchRequestedAt.IsZero()
	if prePatchStage && (request.kind == lib.MutationOperatingPoint || request.kind == lib.MutationSafetyRollback || request.kind == lib.MutationOverheatRecovery) {
		var err error
		asic, err = coordinator.devices.GetASICSettings(ctx, request.ip)
		if err != nil {
			if coordinator.entryPreflightExpired(request) {
				return terminal(lib.MutationFailurePreflight, fmt.Errorf("entry preflight deadline expired: %w", err))
			}
			return open(lib.MutationFailurePreflight, fmt.Errorf("read advertised operating points: %w", err))
		}
		if err := canonicalASICGrid(asic); err != nil {
			return terminal(lib.MutationFailurePreflight, err)
		}
		if !operatingPointAdvertised(asic, request.point) {
			return terminal(lib.MutationFailurePreflight, fmt.Errorf("pending operating point is not advertised by the supported ASIC"))
		}
	}
	if prePatchStage {
		preflight, err := coordinator.devices.GetSystemInfo(ctx, request.ip)
		if err != nil {
			if coordinator.entryPreflightExpired(request) {
				return terminal(lib.MutationFailurePreflight, fmt.Errorf("entry preflight deadline expired: %w", err))
			}
			return open(lib.MutationFailurePreflight, fmt.Errorf("pre-PATCH information read failed: %w", err))
		}
		if err := validateMutationIdentity(preflight, request.macAddr); err != nil {
			return terminal(lib.MutationFailurePreflight, err)
		}
		current, err := coordinator.states.LoadMiner(request.macAddr)
		if err != nil {
			return open(lib.MutationFailurePreflight, err)
		}
		if !mutationStillPending(current, request) {
			return terminal(lib.MutationFailurePreflight, fmt.Errorf("durable mutation intent changed before PATCH"))
		}
		if request.kind == lib.MutationMiningConfiguration && !sameMiningReadback(preflight, request.info) {
			return terminal(lib.MutationFailurePreflight, fmt.Errorf("readable mining settings changed before PATCH"))
		}
		if err := coordinator.validateMutationPreflight(preflight, request.settings, current, asic); err != nil {
			if coordinator.preflightNeedsSafetySupersession(request, current, preflight, asic) {
				if safetyErr := coordinator.supersedeReadback(request, preflight, asic); safetyErr != nil {
					return open(lib.MutationFailurePreflight, fmt.Errorf("persist preflight safety supersession: %w", safetyErr))
				}
			}
			return terminal(lib.MutationFailurePreflight, err)
		}
		if request.kind == lib.MutationOperatingPoint && request.attempt.PatchRequestedAt.IsZero() &&
			operatingPointFromInfo(preflight) != request.attempt.FromPoint() {
			if coordinator.safeExternalPreflight(request, current, preflight, asic) {
				if current.ObservedFrequency == preflight.Frequency && current.ObservedCoreVoltage == preflight.CoreVoltage {
					current.ObservedCount++
				} else {
					current.ObservedFrequency = preflight.Frequency
					current.ObservedCoreVoltage = preflight.CoreVoltage
					current.ObservedCount = 1
				}
				if current.ObservedCount < manualConfirmationPolls {
					if err := coordinator.states.SaveMiner(&current); err != nil {
						return open(lib.MutationFailurePreflight, fmt.Errorf("persist external operating-point observation: %w", err))
					}
					return open(lib.MutationFailurePreflight, fmt.Errorf("awaiting second safe external operating-point observation"))
				}
				rampUntil := coordinator.now().Add(request.settings.RampUpTime)
				deadline := rampUntil.Add(2 * request.settings.EvaluationWindowTime)
				if err := coordinator.states.AdoptExternalPoint(
					&current,
					operatingPointFromInfo(preflight),
					request.attemptID,
					coordinator.now(),
					rampUntil,
					deadline,
				); err != nil {
					return open(lib.MutationFailurePreflight, fmt.Errorf("adopt external operating point: %w", err))
				}
				result.stateReconciled = true
				result.miner = lib.DiscoveredMiner{IP: request.ip, Info: preflight}
				return terminal(lib.MutationFailurePreflight, fmt.Errorf("external operating point adopted without hardware mutation"))
			}
			return terminal(lib.MutationFailurePreflight, fmt.Errorf("operating point changed before PATCH"))
		}
		request.info = preflight
	}

	if prePatchStage {
		patchAt := coordinator.now()
		if err := coordinator.states.AdvanceMutationAttempt(request.attemptID, lib.MutationMilestonePatchRequested, patchAt); err != nil {
			return open(lib.MutationFailurePreflight, fmt.Errorf("persist PATCH milestone: %w", err))
		}
		request.attempt.PatchRequestedAt = patchAt
		var patchErr error
		switch request.kind {
		case lib.MutationOperatingPoint, lib.MutationSafetyRollback:
			patchErr = coordinator.devices.PatchOperatingPoint(ctx, request.point, request.ip)
		case lib.MutationOverheatRecovery:
			patchErr = coordinator.devices.PatchOverheatRecovery(ctx, request.point, request.ip)
		case lib.MutationMiningConfiguration:
			patchErr = coordinator.devices.PatchMiningConfiguration(ctx, request.settings.Mining, request.primaryPassword, request.fallbackPassword, request.ip)
		default:
			return terminal(lib.MutationFailurePreflight, fmt.Errorf("unsupported mutation kind %q", request.kind))
		}
		if errors.Is(patchErr, context.Canceled) || errors.Is(patchErr, context.DeadlineExceeded) {
			return open(lib.MutationFailureConfiguredVerification, patchErr)
		}
	}

	if request.attempt.ConfiguredVerifiedAt.IsZero() {
		configured, terminalReadback, err := coordinator.waitForConfiguredReadback(ctx, request, request.attempt.PatchRequestedAt)
		if err != nil {
			if terminalReadback {
				result.readbackUnavailable = configured.MacAddr == "" || configured.MacAddr != request.macAddr
				if coordinator.readbackNeedsSafetySupersession(request, configured, asic) {
					if safetyErr := coordinator.supersedeReadback(request, configured, asic); safetyErr != nil {
						return open(lib.MutationFailureConfiguredVerification, fmt.Errorf("persist configured safety supersession: %w", safetyErr))
					}
				}
				return terminal(lib.MutationFailureConfiguredVerification, err)
			}
			return open(lib.MutationFailureConfiguredVerification, err)
		}
		verifiedAt := coordinator.now()
		if err := coordinator.states.RecordConfiguredVerification(request.attemptID, verifiedAt, configured.UpTimeSeconds); err != nil {
			return open(lib.MutationFailureConfiguredVerification, fmt.Errorf("persist configured verification: %w", err))
		}
		request.attempt.ConfiguredVerifiedAt = verifiedAt
		request.attempt.ConfiguredVerifiedUptimeSeconds = configured.UpTimeSeconds
		request.info = configured
	}
	if request.attempt.RestartRequestedAt.IsZero() {
		final, terminalReadback, err := coordinator.waitForConfiguredReadback(ctx, request, request.attempt.ConfiguredVerifiedAt)
		if err != nil {
			if terminalReadback {
				result.readbackUnavailable = final.MacAddr == "" || final.MacAddr != request.macAddr
				if coordinator.readbackNeedsSafetySupersession(request, final, asic) {
					if safetyErr := coordinator.supersedeReadback(request, final, asic); safetyErr != nil {
						return open(lib.MutationFailureConfiguredVerification, fmt.Errorf("persist final safety supersession: %w", safetyErr))
					}
				}
				return terminal(lib.MutationFailureConfiguredVerification, err)
			}
			return open(lib.MutationFailureConfiguredVerification, err)
		}
		request.info = final
		restartAt := coordinator.now()
		if err := coordinator.states.AdvanceMutationAttempt(request.attemptID, lib.MutationMilestoneRestartRequested, restartAt); err != nil {
			return open(lib.MutationFailureRebootVerification, fmt.Errorf("persist restart milestone: %w", err))
		}
		request.attempt.RestartRequestedAt = restartAt
		if err := coordinator.devices.Restart(ctx, request.ip); err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return open(lib.MutationFailureRebootVerification, err)
		}
	}
	miner, rediscoveredASIC, err := coordinator.waitForVerifiedBoot(ctx, request, request.attempt.ConfiguredVerifiedUptimeSeconds, request.attempt.ConfiguredVerifiedAt, request.attempt.RestartRequestedAt)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return open(lib.MutationFailureRebootVerification, err)
		}
		result.readbackUnavailable = miner.Info.MacAddr == "" || miner.Info.MacAddr != request.macAddr
		if coordinator.readbackNeedsSafetySupersession(request, miner.Info, rediscoveredASIC) {
			if safetyErr := coordinator.supersedeReadback(request, miner.Info, rediscoveredASIC); safetyErr != nil {
				return open(lib.MutationFailureRebootVerification, fmt.Errorf("persist reboot safety supersession: %w", safetyErr))
			}
		}
		return terminal(lib.MutationFailureRebootVerification, err)
	}
	if request.attempt.RebootVerifiedAt.IsZero() {
		if err := coordinator.states.AdvanceMutationAttempt(request.attemptID, lib.MutationMilestoneRebootVerified, coordinator.now()); err != nil {
			return open(lib.MutationFailureRebootVerification, fmt.Errorf("persist reboot verification: %w", err))
		}
	}
	result.miner = miner
	return result
}

func (coordinator *mutationCoordinator) entryPreflightExpired(request mutationRequest) bool {
	if request.kind != lib.MutationOperatingPoint || !request.attempt.PatchRequestedAt.IsZero() ||
		request.attempt.StartedAt.IsZero() || coordinator.now().Before(request.attempt.StartedAt.Add(defaultRebootDeadline)) {
		return false
	}
	state, err := coordinator.states.LoadMiner(request.macAddr)
	if err != nil {
		return false
	}
	return (state.Phase == lib.PhaseUndervolt || state.Phase == lib.PhaseFrequencyTest || state.Phase == lib.PhaseVoltageTest) &&
		state.PendingKind == lib.MutationOperatingPoint && state.CurrentPoint() == state.FallbackPoint() &&
		state.PendingPoint() != state.FallbackPoint()
}

func (coordinator *mutationCoordinator) safeExternalPreflight(
	request mutationRequest,
	state lib.MinerState,
	info lib.Info,
	asic lib.ASICSettings,
) bool {
	if request.kind != lib.MutationOperatingPoint || info.MacAddr != request.macAddr ||
		!validLivePoint(operatingPointFromInfo(info)) || info.Frequency == 50 ||
		canonicalASICGrid(asic) != nil || !supportedSafetyIdentity(info) ||
		!completeSafetyTelemetry(info) || hasPowerFault(info) || state.SafetyReason != "" ||
		state.Phase == lib.PhaseCooldown || state.Phase == lib.PhaseOverheat {
		return false
	}
	minimum, err := minimumAdvertisedPoint(asic)
	if err != nil {
		return false
	}
	return assessInstantaneousSafety(info, request.settings, operatingPointFromInfo(info), minimum).action == safetyNormal
}

func (coordinator *mutationCoordinator) waitForConfiguredReadback(
	ctx context.Context,
	request mutationRequest,
	startedAt time.Time,
) (lib.Info, bool, error) {
	deadline := startedAt.Add(defaultRebootDeadline)
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return lib.Info{}, false, err
		}
		if !coordinator.now().Before(deadline) {
			if lastErr != nil {
				return lib.Info{}, true, fmt.Errorf("configured readback deadline expired: %w", lastErr)
			}
			return lib.Info{}, true, fmt.Errorf("configured readback deadline expired")
		}
		info, err := coordinator.devices.GetSystemInfo(ctx, request.ip)
		if err == nil {
			if !coordinator.now().Before(deadline) {
				return info, true, fmt.Errorf("configured readback deadline expired after response")
			}
			if identityErr := validateMutationIdentity(info, request.macAddr); identityErr != nil {
				return info, true, identityErr
			}
			if verifyErr := verifyConfiguredReadback(request, info); verifyErr != nil {
				return info, true, verifyErr
			}
			return info, true, nil
		}
		lastErr = err
		if !coordinator.now().Before(deadline) {
			return lib.Info{}, true, fmt.Errorf("configured readback deadline expired: %w", lastErr)
		}
		if err := coordinator.waitMutationRetry(ctx); err != nil {
			return lib.Info{}, false, err
		}
	}
}

func (coordinator *mutationCoordinator) waitMutationRetry(ctx context.Context) error {
	timer := time.NewTimer(coordinator.rediscoveryDelay)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func verifyConfiguredReadback(request mutationRequest, info lib.Info) error {
	if !completeSafetyTelemetry(info) || hasPowerFault(info) {
		return fmt.Errorf("configured readback has incomplete or faulted safety telemetry")
	}
	switch request.kind {
	case lib.MutationOperatingPoint:
		if operatingPointFromInfo(info) != request.point {
			return fmt.Errorf("configured operating point does not match the pending pair")
		}
		if assessInstantaneousSafety(info, request.settings, operatingPointFromInfo(info), lib.OperatingPoint{}).action != safetyNormal {
			return fmt.Errorf("configured operating point is outside normal safety limits")
		}
	case lib.MutationSafetyRollback:
		if operatingPointFromInfo(info) != request.point {
			return fmt.Errorf("configured safety rollback pair does not match the pending pair")
		}
		if info.OverHeatMode != 0 || info.Frequency == 50 || knownFirmwareTripExceeded(info) {
			return fmt.Errorf("configured safety rollback readback is not safety-continuable")
		}
	case lib.MutationOverheatRecovery:
		if operatingPointFromInfo(info) != request.point || info.OverHeatMode != 0 || info.Frequency == 50 || !safeToRecover(info, request.settings) {
			return fmt.Errorf("configured overheat recovery readback is not recovery-safe")
		}
	case lib.MutationMiningConfiguration:
		if operatingPointFromInfo(info) != operatingPointFromInfo(request.info) ||
			!miningReadbackMatches(info, request.settings.Mining) || info.IsUsingFallbackStratum != 0 {
			return fmt.Errorf("configured mining readback does not match the requested safe configuration")
		}
		if assessInstantaneousSafety(info, request.settings, operatingPointFromInfo(info), lib.OperatingPoint{}).action != safetyNormal {
			return fmt.Errorf("configured mining readback is outside normal safety limits")
		}
	default:
		return fmt.Errorf("unsupported mutation kind %q", request.kind)
	}
	return nil
}

func (coordinator *mutationCoordinator) readbackNeedsSafetySupersession(
	request mutationRequest,
	observed lib.Info,
	asic lib.ASICSettings,
) bool {
	if observed.MacAddr == "" || observed.MacAddr != request.macAddr {
		// A timeout, disappearance, or malformed empty response is an
		// unavailable verification boundary, not readable unsafe evidence. A
		// different MAC is equally unusable: it cannot authorize actuation for
		// this request.
		return false
	}
	switch request.kind {
	case lib.MutationOperatingPoint:
		return true
	case lib.MutationMiningConfiguration:
		if observed.MacAddr == "" {
			return false
		}
		assessment := assessInstantaneousSafety(
			observed,
			request.settings,
			operatingPointFromInfo(observed),
			lib.OperatingPoint{},
		)
		return assessment.action != safetyNormal && assessment.action != safetyUnavailable
	case lib.MutationSafetyRollback, lib.MutationOverheatRecovery:
		if observed.MacAddr == "" || canonicalASICGrid(asic) != nil || !supportedSafetyIdentity(observed) ||
			!completeSafetyTelemetry(observed) || hasPowerFault(observed) {
			return observed.MacAddr != ""
		}
		if observed.OverHeatMode != 0 || observed.Frequency == 50 {
			return request.kind == lib.MutationSafetyRollback
		}
		return knownFirmwareTripExceeded(observed) ||
			assessInstantaneousSafety(observed, request.settings, operatingPointFromInfo(observed), lib.OperatingPoint{}).action == safetyEmergencyHold
	default:
		return true
	}
}

func (coordinator *mutationCoordinator) waitForVerifiedBoot(
	ctx context.Context,
	request mutationRequest,
	configuredUptime int,
	configuredAt time.Time,
	restartAt time.Time,
) (lib.DiscoveredMiner, lib.ASICSettings, error) {
	deadline := restartAt.Add(coordinator.rebootDeadline)
	var lastErr error
	var lastMiner lib.DiscoveredMiner
	var lastASIC lib.ASICSettings
	for {
		if err := ctx.Err(); err != nil {
			return lib.DiscoveredMiner{}, lib.ASICSettings{}, err
		}
		if !coordinator.now().Before(deadline) {
			if lastErr != nil {
				return lastMiner, lastASIC, fmt.Errorf("reboot verification deadline expired: %w", lastErr)
			}
			return lastMiner, lastASIC, fmt.Errorf("reboot verification deadline expired")
		}
		miner, err := coordinator.discover(ctx, request.macAddr)
		if err == nil {
			lastMiner = miner
			if identityErr := validateMutationIdentity(miner.Info, request.macAddr); identityErr != nil {
				return miner, lib.ASICSettings{}, identityErr
			}
			if request.kind == lib.MutationOperatingPoint || request.kind == lib.MutationSafetyRollback || request.kind == lib.MutationOverheatRecovery {
				rediscoveredASIC, asicErr := coordinator.devices.GetASICSettings(ctx, miner.IP)
				if asicErr != nil {
					lastErr = fmt.Errorf("read advertised operating points after rediscovery: %w", asicErr)
					if !coordinator.now().Before(deadline) {
						return lib.DiscoveredMiner{}, lib.ASICSettings{}, fmt.Errorf("reboot verification deadline expired: %w", lastErr)
					}
					if err := coordinator.waitMutationRetry(ctx); err != nil {
						return lib.DiscoveredMiner{}, lib.ASICSettings{}, err
					}
					continue
				}
				lastASIC = rediscoveredASIC
				if err := canonicalASICGrid(rediscoveredASIC); err != nil {
					return miner, rediscoveredASIC, fmt.Errorf("rediscovered ASIC grid is unsupported: %w", err)
				}
				if !operatingPointAdvertised(rediscoveredASIC, request.point) {
					return miner, rediscoveredASIC, fmt.Errorf("rediscovered target operating point is no longer advertised")
				}
			}
			elapsed := coordinator.now().Sub(configuredAt)
			booted := false
			if request.bootProofSameProcess {
				booted = proveNewBoot(configuredUptime, miner.Info.UpTimeSeconds, elapsed)
			} else if configuredUptime > int(rebootUptimeTolerance/time.Second) {
				booted = time.Duration(miner.Info.UpTimeSeconds)*time.Second <
					time.Duration(configuredUptime)*time.Second-rebootUptimeTolerance
			}
			if booted {
				if !coordinator.now().Before(deadline) {
					return miner, lastASIC, fmt.Errorf("reboot verification deadline expired after boot proof")
				}
				if verifyErr := verifyConfiguredReadback(request, miner.Info); verifyErr != nil {
					return miner, lastASIC, verifyErr
				}
				if !coordinator.now().Before(deadline) {
					return miner, lastASIC, fmt.Errorf("reboot verification deadline expired after readback")
				}
				return miner, lastASIC, nil
			}
			lastErr = fmt.Errorf("new boot proof is not established")
		} else {
			lastErr = err
		}
		if !coordinator.now().Before(deadline) {
			return lastMiner, lastASIC, fmt.Errorf("reboot verification deadline expired: %w", lastErr)
		}
		if err := coordinator.waitMutationRetry(ctx); err != nil {
			return lib.DiscoveredMiner{}, lib.ASICSettings{}, err
		}
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
			attempts, historyErr := coordinator.states.ListMutationAttempts(result.macAddr)
			if historyErr != nil {
				return processed, historyErr
			}
			var storedAttempt lib.MutationAttempt
			for _, attempt := range attempts {
				if attempt.ID == result.attemptID {
					storedAttempt = attempt
					break
				}
			}
			if storedAttempt.ID == 0 {
				return processed, fmt.Errorf("mutation result references missing attempt %d", result.attemptID)
			}
			if result.err != nil {
				if result.failureStage != "" && !result.stateReconciled {
					if terminalErr := coordinator.handleTerminalMutationFailureLocked(result, storedAttempt); terminalErr != nil {
						return processed, terminalErr
					}
				}
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

func (coordinator *mutationCoordinator) handleTerminalMutationFailureLocked(
	result mutationResult,
	attempt lib.MutationAttempt,
) error {
	if result.readbackUnavailable &&
		result.kind != lib.MutationSafetyRollback && result.kind != lib.MutationOverheatRecovery {
		state, err := coordinator.states.LoadMiner(result.macAddr)
		if err != nil {
			return err
		}
		settings, err := coordinator.settings.ForHost(state.Hostname)
		if err != nil {
			return err
		}
		if state.Phase != lib.PhaseOverheat {
			state.OverheatCount = incrementOverheatCount(state.OverheatCount)
			state.CooldownUntil = coordinator.now().Add(overheatCooldown(settings, state.OverheatCount))
			state.PhaseStartedAt = coordinator.now()
		}
		state.ClearPendingMutation()
		state.SetFallbackPoint(lib.OperatingPoint{})
		state.Phase = lib.PhaseOverheat
		state.HoldReason = ""
		state.SettledAt = time.Time{}
		state.EvidenceDeadlineAt = time.Time{}
		state.SafetyReason = escalateSafetyReason(state.SafetyReason, lib.SafetyReasonMutationUncertain)
		return coordinator.states.QuarantineMutation(&state, result.attemptID, result.failureStage, coordinator.now())
	}
	if attempt.FailureStage == lib.MutationFailureSafetySuperseded {
		// Safety arbitration already replaced the normal state and finalized
		// any entered candidate in one store transaction. A late worker result
		// must not overwrite that stronger durable outcome.
		return nil
	}
	state, err := coordinator.states.LoadMiner(result.macAddr)
	if err != nil {
		return err
	}
	now := coordinator.now()
	settings, err := coordinator.settings.ForHost(state.Hostname)
	if err != nil {
		return err
	}
	if result.kind == lib.MutationOperatingPoint {
		if (state.Phase == lib.PhaseUndervolt || state.Phase == lib.PhaseFrequencyTest || state.Phase == lib.PhaseVoltageTest) &&
			state.CurrentPoint() == state.FallbackPoint() && state.PendingKind == result.kind {
			records, listErr := coordinator.states.ListPoints(result.macAddr)
			if listErr != nil {
				return listErr
			}
			if entered, found := findRecord(records, result.point); found && entered.EntryAttemptID == attempt.ID {
				measuredAt := now
				if measuredAt.Before(entered.EnteredAt) {
					measuredAt = entered.EnteredAt
				}
				record := entered
				record.Status = lib.PointUnobservable
				record.MeasuredAt = measuredAt
				record.MedianHash = 0
				record.ExpectedHash = 0
				record.Attainment = 0
				record.MeanTemp = 0
				record.P95Temp = 0
				record.P95VRTemp = 0
				record.P95Power = 0
				record.ErrorPercent = nil
				record.AcceptedDelta = 0
				record.RejectedDelta = 0
				return coordinator.states.FailMutationAndFinalizeTrial(
					&state, record, lib.TrialReturn, result.attemptID, result.failureStage,
					now, now.Add(settings.RampUpTime),
					now.Add(settings.RampUpTime+4*settings.EvaluationWindowTime),
				)
			}
		}
	}
	if result.kind == lib.MutationMiningConfiguration {
		state.MiningPending = true
		coordinator.startupBlocked = state.Hostname
		return coordinator.states.FailMutationAndSave(&state, result.attemptID, result.failureStage, now)
	}
	if result.kind == lib.MutationSafetyRollback || result.kind == lib.MutationOverheatRecovery {
		// A safety attempt that reaches a readable same-obligation mismatch or
		// an availability deadline keeps its typed safety intent. The failed
		// history row is terminal, but a later coordinator pass may create the
		// next recorded safety attempt after fresh preflight.
		state.SetFallbackPoint(lib.OperatingPoint{})
		state.EvidenceDeadlineAt = time.Time{}
		state.SettledAt = time.Time{}
		state.HoldReason = ""
		return coordinator.states.FailMutationAndSave(&state, result.attemptID, result.failureStage, now)
	}
	state.ClearPendingMutation()
	state.SetFallbackPoint(lib.OperatingPoint{})
	state.EvidenceDeadlineAt = time.Time{}
	if result.kind == lib.MutationSafetyRollback || result.kind == lib.MutationOverheatRecovery {
		if state.Phase != lib.PhaseOverheat {
			state.Phase = lib.PhaseCooldown
		}
		state.HoldReason = ""
	} else {
		state.Phase = lib.PhaseHold
		state.HoldReason = lib.HoldBlocked
		state.SettledAt = time.Time{}
	}
	return coordinator.states.FailMutationAndSave(&state, result.attemptID, result.failureStage, now)
}

func (coordinator *mutationCoordinator) completeMutationLocked(
	result mutationResult,
) error {
	state, err := coordinator.states.LoadMiner(result.macAddr)
	if err != nil {
		return err
	}
	attempts, err := coordinator.states.ListMutationAttempts(result.macAddr)
	if err != nil {
		return err
	}
	var attempt lib.MutationAttempt
	for _, candidate := range attempts {
		if candidate.ID == result.attemptID {
			attempt = candidate
			break
		}
	}
	if attempt.ID == 0 {
		return fmt.Errorf("complete mutation: attempt %d does not exist", result.attemptID)
	}
	completedAt := coordinator.now()
	settings, err := coordinator.settings.ForHost(state.Hostname)
	if err != nil {
		return err
	}
	if !attempt.CompletedAt.IsZero() {
		if result.miner.IP != "" {
			state.IP = result.miner.IP
		}
		coordinator.routes[result.macAddr] = result.miner
		return coordinator.states.CompleteMutationAttempt(&state, result.attemptID, attempt.CompletedAt)
	}

	if result.miner.IP != "" {
		state.IP = result.miner.IP
	}
	state.RampUntil = completedAt.Add(settings.RampUpTime)
	if err := coordinator.states.CompleteMutationAttempt(&state, result.attemptID, completedAt); err != nil {
		return fmt.Errorf("complete mutation for %s: %w", state.Hostname, err)
	}
	coordinator.routes[result.macAddr] = result.miner
	coordinator.startupHealth[result.macAddr] = 0
	delete(coordinator.startupHealthSince, result.macAddr)
	coordinator.reset(result.macAddr)
	coordinator.logger.Printf("Mutation %s verified after restart for %s", result.kind, result.hostname)
	return nil
}

func (coordinator *mutationCoordinator) cancelSupersededLocked(
	observations map[string]*minerObservation,
	now time.Time,
) error {
	for macAddr, active := range coordinator.active {
		if !active.normal {
			continue
		}
		observation := observations[macAddr]
		if observation == nil {
			continue
		}
		superseded := false
		if active.kind == lib.MutationMiningConfiguration {
			if !observation.state.MiningPending ||
				observation.state.PendingKind != "" {
				superseded = true
			}
		} else if observation.state.PendingKind != active.kind ||
			observation.state.PendingPoint() != active.point {
			superseded = true
		}
		if !superseded {
			continue
		}
		attempts, err := coordinator.states.ListMutationAttempts(macAddr)
		if err != nil {
			return fmt.Errorf("load superseded mutation %d: %w", active.attemptID, err)
		}
		alreadyFailed := false
		for _, attempt := range attempts {
			if attempt.ID == active.attemptID && !attempt.FailedAt.IsZero() {
				alreadyFailed = true
				break
			}
		}
		if alreadyFailed {
			active.cancel()
			continue
		}
		if err := coordinator.states.SupersedeMutation(
			&observation.state,
			&observation.state,
			active.attemptID,
			now,
		); err != nil {
			return fmt.Errorf("close superseded mutation %d: %w", active.attemptID, err)
		}
		active.cancel()
	}
	return nil
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
		if state.PendingPoint() == operatingPointFromInfo(info) &&
			state.SafetyReason != lib.SafetyReasonFirmwareTrip &&
			state.SafetyReason != lib.SafetyReasonMutationUncertain {
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

func (coordinator *mutationCoordinator) preflightNeedsSafetySupersession(
	request mutationRequest,
	state lib.MinerState,
	info lib.Info,
	asic lib.ASICSettings,
) bool {
	if request.kind == lib.MutationMiningConfiguration {
		return false
	}
	if canonicalASICGrid(asic) != nil {
		return true
	}
	minimum, err := minimumAdvertisedPoint(asic)
	if err != nil {
		return true
	}
	assessment := assessInstantaneousSafety(info, request.settings, operatingPointFromInfo(info), minimum)
	return assessment.action == safetyRollback ||
		assessment.action == safetyHostContainment ||
		assessment.action == safetyFirmwareRecovery ||
		assessment.action == safetyEmergencyHold ||
		(request.kind == lib.MutationSafetyRollback && state.PendingPoint() == operatingPointFromInfo(info))
}

func (coordinator *mutationCoordinator) supersedeReadback(
	request mutationRequest,
	observed lib.Info,
	asic lib.ASICSettings,
) error {
	if observed.MacAddr != request.macAddr {
		return fmt.Errorf("safety supersession readback belongs to MAC %s, not %s", observed.MacAddr, request.macAddr)
	}
	state, err := coordinator.states.LoadMiner(request.macAddr)
	if err != nil {
		return err
	}
	expected := state
	now := coordinator.now()
	settings := request.settings
	if observed.MacAddr == "" || canonicalASICGrid(asic) != nil {
		state.ClearPendingMutation()
		state.SetFallbackPoint(lib.OperatingPoint{})
		state.Phase = lib.PhaseOverheat
		state.HoldReason = ""
		state.SettledAt = time.Time{}
		state.EvidenceDeadlineAt = time.Time{}
		state.SafetyReason = lib.SafetyReasonMutationUncertain
		state.OverheatCount = incrementOverheatCount(state.OverheatCount)
		state.CooldownUntil = now.Add(overheatCooldown(settings, state.OverheatCount))
		return coordinator.states.SupersedeMutation(&expected, &state, request.attemptID, now)
	}
	minimum, err := minimumAdvertisedPoint(asic)
	if err != nil || !supportedSafetyIdentity(observed) || !operatingPointAdvertised(asic, minimum) {
		state.ClearPendingMutation()
		state.SetFallbackPoint(lib.OperatingPoint{})
		state.Phase = lib.PhaseOverheat
		state.HoldReason = ""
		state.SettledAt = time.Time{}
		state.EvidenceDeadlineAt = time.Time{}
		state.SafetyReason = lib.SafetyReasonMutationUncertain
		state.OverheatCount = incrementOverheatCount(state.OverheatCount)
		state.CooldownUntil = now.Add(overheatCooldown(settings, state.OverheatCount))
		return coordinator.states.SupersedeMutation(&expected, &state, request.attemptID, now)
	}
	assessment := assessInstantaneousSafety(observed, settings, operatingPointFromInfo(observed), minimum)
	if assessment.action == safetyNormal || assessment.action == safetyUnavailable {
		assessment = safetyAssessment{
			action:  safetyEmergencyHold,
			failure: safetyFailure{status: lib.PointThermal, reason: "mutation configuration became uncertain"},
		}
	}
	if assessment.action == safetyRollback {
		target := minimum
		records, listErr := coordinator.states.ListPoints(state.MacAddr)
		if listErr != nil {
			return fmt.Errorf("read safety supersession evidence: %w", listErr)
		}
		failed := request.attempt.FromPoint()
		for _, record := range records {
			if rollbackRecordEligible(record, failed, asic, settings) &&
				(target == minimum || record.MedianHash > pointHash(records, target)) {
				target = record.Point()
			}
		}
		state.ClearPendingMutation()
		state.SetFallbackPoint(lib.OperatingPoint{})
		state.SafetyReason = reasonForSafetyFailure(assessment.failure)
		state.Phase = lib.PhaseCooldown
		state.HoldReason = ""
		state.SettledAt = time.Time{}
		state.EvidenceDeadlineAt = time.Time{}
		if target == operatingPointFromInfo(observed) {
			state.Phase = lib.PhaseOverheat
			state.SafetyReason = lib.SafetyReasonMutationUncertain
		} else {
			state.SetPendingMutation(lib.MutationSafetyRollback, target, now)
		}
		return coordinator.states.SupersedeMutation(&expected, &state, request.attemptID, now)
	}
	if _, err := transitionEmergencyState(&state, observed, asic, settings, now, assessment, false); err != nil {
		return err
	}
	return coordinator.states.SupersedeMutation(&expected, &state, request.attemptID, now)
}

func pointHash(records []lib.OperatingPointRecord, point lib.OperatingPoint) float64 {
	for _, record := range records {
		if record.Point() == point {
			return record.MedianHash
		}
	}
	return -1
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
