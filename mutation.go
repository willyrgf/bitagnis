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
	id              uint64
	attemptID       int64
	macAddr         string
	hostname        string
	kind            lib.MutationKind
	point           lib.OperatingPoint
	miner           lib.DiscoveredMiner
	err             error
	failureStage    lib.MutationFailureStage
	stateReconciled bool
}

type rebootReadbackDisposition uint8

const (
	rebootReadbackVerified rebootReadbackDisposition = iota + 1
	rebootReadbackUnsafe
	rebootReadbackFirmwareReduction
	rebootReadbackExternalPoint
)

// rebootReadback is the closed result of post-boot verification. An ordinary
// error means that verification remains incomplete and the durable attempt
// must stay open; only one of these dispositions may consume its authority.
type rebootReadback struct {
	miner       lib.DiscoveredMiner
	asic        lib.ASICSettings
	disposition rebootReadbackDisposition
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

// RecordRetuneDiscovery starts the one-shot qualification clock from the
// successful named discovery that selected this coordinator. Metrics polling
// may be delayed or blocked; it must not extend the operator's deadline.
func (coordinator *mutationCoordinator) RecordRetuneDiscovery(at time.Time) {
	if coordinator == nil || at.IsZero() {
		return
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.retuneHost == "" || !coordinator.retuneFirstSeen.IsZero() {
		return
	}
	for _, miner := range coordinator.routes {
		if miner.Info.Hostname == coordinator.retuneHost {
			coordinator.retuneFirstSeen = at
			return
		}
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
	coordinator.trackRetuneDeadlineLocked(observations, now)

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
		if state.Phase == lib.PhaseEmergency || state.Phase == lib.PhaseCooldown ||
			state.PendingKind == lib.MutationSafetyRollback ||
			state.PendingKind == lib.MutationFirmwareRecovery {
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
		lib.MutationFirmwareRecovery,
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
		coordinator.retuneHealthyCount = 0
		return false, nil
	}
	if pendingWithoutObservation {
		coordinator.retuneHealthyCount = 0
		return false, nil
	}
	if appliedResult {
		coordinator.retuneHealthyCount = 0
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
	coordinator.trackRetuneDeadlineLocked(observations, now)
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
		coordinator.retuneHealthyCount = 0
		return false, nil
	}
	if coordinator.normalActive != "" || len(coordinator.active) != 0 {
		coordinator.retuneHealthyCount = 0
		return false, nil
	}
	state := observation.state
	if !qualifiesSettledObservation(
		coordinator.states, state, observation.info, observation.asic, observation.settings, now, true,
	) || !startupHealthy(observation.info, observation.settings) {
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
	if _, err := coordinator.states.Apply(lib.StartPass{
		MacAddr: state.MacAddr, Point: state.CurrentPoint(), Trigger: lib.PassOperator,
	}, startedAt); err != nil {
		return false, fmt.Errorf("retune %s: %w", coordinator.retuneHost, err)
	}
	coordinator.reset(state.MacAddr)
	coordinator.logger.Printf("Accepted retune for %s; starting a new finite pass", coordinator.retuneHost)
	coordinator.retuneHost = ""
	coordinator.retuneHealthyCount = 0
	coordinator.retuneFirstSeen = time.Time{}
	return true, nil
}

func (coordinator *mutationCoordinator) trackRetuneDeadlineLocked(
	observations map[string]*minerObservation,
	now time.Time,
) {
	if coordinator.retuneHost == "" {
		return
	}
	var matched int
	for _, observation := range observations {
		if observation.info.Hostname == coordinator.retuneHost {
			matched++
		}
	}
	if matched > 1 {
		coordinator.logger.Printf("Retune refused for %s: hostname maps to multiple observations", coordinator.retuneHost)
		coordinator.retuneHost = ""
		coordinator.retuneHealthyCount = 0
		return
	}
	if coordinator.retuneFirstSeen.IsZero() && matched == 1 {
		coordinator.retuneFirstSeen = now
	}
	if !coordinator.retuneFirstSeen.IsZero() &&
		!now.Before(coordinator.retuneFirstSeen.Add(3*time.Minute)) {
		if !coordinator.retuneRefused {
			coordinator.logger.Printf("Retune refused for %s: qualification deadline expired", coordinator.retuneHost)
			coordinator.retuneRefused = true
		}
		coordinator.retuneHost = ""
		coordinator.retuneHealthyCount = 0
		coordinator.retuneFirstSeen = time.Time{}
	}
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
		// CompleteResume now derives the epoch it opens (purpose and required-window count) from
		// whatever phase and hold reason the completed mutation already left durable
		// from durable state, so the caller no longer precomputes a window count or deadline.
		result, err := coordinator.states.Apply(lib.CompleteResume{
			MacAddr: observation.state.MacAddr, AttemptID: attempt.ID,
		}, now)
		if err != nil {
			return err
		}
		observation.state = result.State
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
		result, err := coordinator.states.Apply(lib.FailMutation{State: *state, AttemptID: attempt.ID, Stage: lib.MutationFailureMiningResume}, now)
		if err != nil {
			return err
		}
		*state = result.State
		return nil
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
				// This candidate never reached CompleteResume (that is what would have opened its
				// trial epoch, once two healthy polls proved it was the running configuration), so
				// there is no epoch to close here — the zero-value Epoch tells finalizeTrialTx that.
				result, err := coordinator.states.Apply(lib.FailMutationFinalizeTrial{
					State: *state, Record: record, Decision: lib.TrialReturn, AttemptID: attempt.ID,
					Stage: lib.MutationFailureMiningResume,
				}, now)
				if err != nil {
					return err
				}
				*state = result.State
				return nil
			}
		}
	}
	if attempt.Kind == lib.MutationSafetyRollback || attempt.Kind == lib.MutationFirmwareRecovery {
		state.ClearPendingMutation()
		state.SetFallbackPoint(lib.OperatingPoint{})
		state.Phase = lib.PhaseCooldown
		state.MonitorReason = ""
		state.MonitorReferenceEpochID = 0
		state.SettledAt = time.Time{}
		result, err := coordinator.states.Apply(lib.FailMutation{State: *state, AttemptID: attempt.ID, Stage: lib.MutationFailureMiningResume}, now)
		if err != nil {
			return err
		}
		*state = result.State
		return nil
	}
	// This attempt already completed (PATCH + restart + readback verified), so the device is
	// confirmed running at this exact configuration; it simply never produced a positive hash within
	// the health deadline. That is itself a real, measured conclusion about this configuration —
	// mirroring the trial-phase branch above, which reaches the identical no-positive-hash outcome
	// via PointUnstable/TrialReturn — so this enters rejected continuous monitoring.
	state.ClearPendingMutation()
	state.SetFallbackPoint(lib.OperatingPoint{})
	state.Phase = lib.PhaseMonitor
	state.MonitorReason = lib.MonitorRejected
	state.SettledAt = time.Time{}
	coordinator.startupBlocked = state.Hostname
	result, err := coordinator.states.Apply(lib.FailMutation{State: *state, AttemptID: attempt.ID, Stage: lib.MutationFailureMiningResume}, now)
	if err != nil {
		return err
	}
	*state = result.State
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
			result, err := coordinator.states.Apply(lib.SaveState{State: observation.state}, now)
			if err != nil {
				return fmt.Errorf(
					"persist mining configuration obligation for %s: %w",
					observation.info.Hostname,
					err,
				)
			}
			observation.state = result.State
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

	// Ramp completion is now a settled-sample count against the miner's open evidence epoch rather
	// than a wall-clock deadline. A miner with no open epoch (for example EMERGENCY at startup) has
	// nothing to ramp toward, so it is not held back here.
	rampIncomplete := false
	if epoch, open, err := coordinator.states.OpenEvidenceEpochFor(macAddr); err != nil {
		return err
	} else if open && epoch.Progress.SettledSamples() < rampSamples(observation.settings) {
		rampIncomplete = true
	}
	if rampIncomplete || !startupHealthy(observation.info, observation.settings) {
		coordinator.startupHealth[macAddr] = 0
		if rampIncomplete {
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
		// Bootstrap, mutation completion, and healthy resumption own the
		// absolute ramp/deadline pair. Opening the fleet gate only authorizes
		// decisions; it must not extend an evidence deadline while another
		// miner is reconciling.
		state.ObservedFrequency = 0
		state.ObservedCoreVoltage = 0
		state.ObservedCount = 0
		if _, err := coordinator.states.Apply(lib.SaveState{State: state}, now); err != nil {
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
		kind == lib.MutationSafetyRollback || kind == lib.MutationFirmwareRecovery {
		if canonicalASICGrid(observation.asic) != nil {
			return false
		}
	}
	minimum, err := minimumAdvertisedPoint(observation.asic)
	if err != nil {
		return false
	}
	if validateKindSafety(observation.info, observation.settings, observation.state, observation.asic, minimum) != nil {
		if kind != lib.MutationSafetyRollback && kind != lib.MutationFirmwareRecovery {
			return false
		}
		request := mutationRequest{
			macAddr: observation.state.MacAddr, kind: kind,
			point: observation.state.PendingPoint(), settings: observation.settings,
		}
		if coordinator.safetyIntentAlreadySatisfied(request, observation.info, observation.asic) {
			return true
		}
		return coordinator.preflightNeedsSafetySupersession(
			request,
			observation.info,
			observation.asic,
		)
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
		if kind == lib.MutationSafetyRollback || kind == lib.MutationFirmwareRecovery {
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
	if kind == lib.MutationSafetyRollback || kind == lib.MutationFirmwareRecovery {
		return state.SafetyReason
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
	// Once PATCH is durable, preflight is complete. Later workers resume the
	// recorded milestone without replaying hardware; their deadlines bound
	// execution only and never become durable state authority.
	prePatchStage := request.attempt.PatchRequestedAt.IsZero()
	if prePatchStage && (request.kind == lib.MutationOperatingPoint || request.kind == lib.MutationSafetyRollback || request.kind == lib.MutationFirmwareRecovery) {
		var err error
		asic, err = coordinator.devices.GetASICSettings(ctx, request.ip)
		if err != nil {
			return open(lib.MutationFailurePreflight, fmt.Errorf("read advertised operating points: %w", err))
		}
		if err := canonicalASICGrid(asic); err != nil {
			return open(lib.MutationFailurePreflight, err)
		}
		if !operatingPointAdvertised(asic, request.point) {
			return terminal(lib.MutationFailurePreflight, fmt.Errorf("pending operating point is not advertised by the supported ASIC"))
		}
	}
	if prePatchStage {
		preflight, err := coordinator.devices.GetSystemInfo(ctx, request.ip)
		if err != nil {
			return open(lib.MutationFailurePreflight, fmt.Errorf("pre-PATCH information read failed: %w", err))
		}
		if err := validateMutationIdentity(preflight, request.macAddr); err != nil {
			return open(lib.MutationFailurePreflight, err)
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
			if coordinator.safetyIntentAlreadySatisfied(request, preflight, asic) {
				if reconcileErr := coordinator.reconcileSatisfiedSafetyIntent(request, current, preflight); reconcileErr != nil {
					return open(lib.MutationFailurePreflight, fmt.Errorf("reconcile satisfied safety intent: %w", reconcileErr))
				}
				result.stateReconciled = true
				result.miner = lib.DiscoveredMiner{IP: request.ip, Info: preflight}
				return terminal(lib.MutationFailurePreflight, fmt.Errorf("safety intent was already satisfied without hardware mutation"))
			}
			if coordinator.preflightNeedsSafetySupersession(request, preflight, asic) {
				if safetyErr := coordinator.supersedeReadback(request, preflight, asic); safetyErr != nil {
					return open(lib.MutationFailurePreflight, fmt.Errorf("persist preflight safety supersession: %w", safetyErr))
				}
			}
			if !completeSafetyTelemetry(preflight) || !supportedSafetyIdentity(preflight) ||
				((request.kind == lib.MutationSafetyRollback || request.kind == lib.MutationFirmwareRecovery) &&
					!safeToRecover(preflight, request.settings)) {
				return open(lib.MutationFailurePreflight, err)
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
					if _, err := coordinator.states.Apply(lib.SaveState{State: current}, coordinator.now()); err != nil {
						return open(lib.MutationFailurePreflight, fmt.Errorf("persist external operating-point observation: %w", err))
					}
					return open(lib.MutationFailurePreflight, fmt.Errorf("awaiting second safe external operating-point observation"))
				}
				if _, err := coordinator.states.Apply(lib.AdoptExternalPoint{
					State: current, Point: operatingPointFromInfo(preflight), AttemptID: request.attemptID,
					Stage: lib.MutationFailurePreflight,
				}, coordinator.now()); err != nil {
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
		case lib.MutationFirmwareRecovery:
			patchErr = coordinator.devices.PatchFirmwareRecovery(ctx, request.point, request.ip)
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
		configured, unsafeReadback, err := coordinator.waitForConfiguredReadback(ctx, request)
		if err != nil {
			if unsafeReadback {
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
		final, unsafeReadback, err := coordinator.waitForConfiguredReadback(ctx, request)
		if err != nil {
			if unsafeReadback {
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
	readback, err := coordinator.waitForVerifiedBoot(ctx, request, request.attempt.ConfiguredVerifiedUptimeSeconds, request.attempt.ConfiguredVerifiedAt)
	if err != nil {
		return open(lib.MutationFailureRebootVerification, err)
	}
	switch readback.disposition {
	case rebootReadbackUnsafe:
		safetyRequest := request
		safetyRequest.ip = readback.miner.IP
		if safetyErr := coordinator.supersedeReadback(safetyRequest, readback.miner.Info, readback.asic); safetyErr != nil {
			return open(lib.MutationFailureRebootVerification, fmt.Errorf("persist reboot safety supersession: %w", safetyErr))
		}
		result.stateReconciled = true
		result.miner = readback.miner
		return terminal(lib.MutationFailureRebootVerification, fmt.Errorf("post-boot readback proved an unsafe device state"))
	case rebootReadbackFirmwareReduction:
		if reconcileErr := coordinator.reconcileFirmwareReduction(request, readback); reconcileErr != nil {
			return open(lib.MutationFailureRebootVerification, fmt.Errorf("reconcile AxeOS firmware reduction: %w", reconcileErr))
		}
		result.stateReconciled = true
		result.miner = readback.miner
		return terminal(lib.MutationFailureRebootVerification, fmt.Errorf("AxeOS firmware reduction superseded the requested point"))
	case rebootReadbackExternalPoint:
		state, loadErr := coordinator.states.LoadMiner(request.macAddr)
		if loadErr != nil {
			return open(lib.MutationFailureRebootVerification, fmt.Errorf("load external-point state: %w", loadErr))
		}
		state.IP = readback.miner.IP
		if _, adoptErr := coordinator.states.Apply(lib.AdoptExternalPoint{
			State: state, Point: operatingPointFromInfo(readback.miner.Info), AttemptID: request.attemptID,
			Stage: lib.MutationFailureRebootVerification,
		}, coordinator.now()); adoptErr != nil {
			return open(lib.MutationFailureRebootVerification, fmt.Errorf("adopt post-boot external operating point: %w", adoptErr))
		}
		result.stateReconciled = true
		result.miner = readback.miner
		return terminal(lib.MutationFailureRebootVerification, fmt.Errorf("stable post-boot external operating point was adopted"))
	case rebootReadbackVerified:
		// Continue into the durable reboot milestone below.
	default:
		return open(lib.MutationFailureRebootVerification, fmt.Errorf("post-boot verification returned an invalid disposition"))
	}
	if request.attempt.RebootVerifiedAt.IsZero() {
		if err := coordinator.states.AdvanceMutationAttempt(request.attemptID, lib.MutationMilestoneRebootVerified, coordinator.now()); err != nil {
			return open(lib.MutationFailureRebootVerification, fmt.Errorf("persist reboot verification: %w", err))
		}
	}
	result.miner = readback.miner
	return result
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
		state.Phase == lib.PhaseCooldown || state.Phase == lib.PhaseEmergency {
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
) (lib.Info, bool, error) {
	// This deadline bounds one worker invocation; it is not state authority.
	// A later coordinator pass resumes the same persisted milestone without
	// replaying PATCH, so temporary telemetry gaps cannot become a fabricated
	// terminal mutation failure merely because wall time elapsed.
	deadline := coordinator.now().Add(defaultRebootDeadline)
	var lastErr error
	var lastInfo lib.Info
	for {
		if err := ctx.Err(); err != nil {
			return lib.Info{}, false, err
		}
		if !coordinator.now().Before(deadline) {
			if lastErr != nil {
				return lastInfo, false, fmt.Errorf("configured readback worker deadline expired: %w", lastErr)
			}
			return lastInfo, false, fmt.Errorf("configured readback worker deadline expired")
		}
		info, err := coordinator.devices.GetSystemInfo(ctx, request.ip)
		if err == nil {
			lastInfo = info
			if !coordinator.now().Before(deadline) {
				return info, false, fmt.Errorf("configured readback worker deadline expired after response")
			}
			if identityErr := validateMutationIdentity(info, request.macAddr); identityErr != nil {
				lastErr = identityErr
			} else if verifyErr := verifyConfiguredReadback(request, info); verifyErr != nil {
				if coordinator.readbackNeedsSafetySupersession(request, info, lib.ASICSettings{}) {
					return info, true, verifyErr
				}
				lastErr = verifyErr
			} else {
				return info, false, nil
			}
		} else {
			lastErr = err
		}
		if !coordinator.now().Before(deadline) {
			return lastInfo, false, fmt.Errorf("configured readback worker deadline expired: %w", lastErr)
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
	case lib.MutationFirmwareRecovery:
		if operatingPointFromInfo(info) != request.point || info.OverHeatMode != 0 || info.Frequency == 50 || !safeToRecover(info, request.settings) {
			return fmt.Errorf("configured firmware recovery readback is not recovery-safe")
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
	_, unsafe := provenUnsafeReadback(request, observed, asic)
	return unsafe
}

func provenUnsafeReadback(
	request mutationRequest,
	observed lib.Info,
	asic lib.ASICSettings,
) (safetyAssessment, bool) {
	if observed.MacAddr == "" || observed.MacAddr != request.macAddr {
		return safetyAssessment{}, false
	}
	if hasPowerFault(observed) {
		return safetyAssessment{
			action:  safetyEmergencyHold,
			failure: safetyFailure{status: string(lib.PointPower), reason: "device reports a power fault"},
		}, true
	}
	minimum := lib.OperatingPoint{}
	if canonicalASICGrid(asic) == nil {
		minimum, _ = minimumAdvertisedPoint(asic)
	}
	assessment := assessInstantaneousSafety(
		observed,
		request.settings,
		operatingPointFromInfo(observed),
		minimum,
	)
	switch assessment.action {
	case safetyRollback, safetyHostContainment, safetyFirmwareRecovery, safetyEmergencyHold:
		return assessment, true
	default:
		return assessment, false
	}
}

func (coordinator *mutationCoordinator) waitForVerifiedBoot(
	ctx context.Context,
	request mutationRequest,
	configuredUptime int,
	configuredAt time.Time,
) (rebootReadback, error) {
	deadline := coordinator.now().Add(coordinator.rebootDeadline)
	var lastErr error
	var lastMiner lib.DiscoveredMiner
	var lastASIC lib.ASICSettings
	var mismatch lib.OperatingPoint
	mismatchCount := 0
	for {
		if err := ctx.Err(); err != nil {
			return rebootReadback{}, err
		}
		if !coordinator.now().Before(deadline) {
			if lastErr != nil {
				return rebootReadback{miner: lastMiner, asic: lastASIC}, fmt.Errorf("reboot verification worker deadline expired: %w", lastErr)
			}
			return rebootReadback{miner: lastMiner, asic: lastASIC}, fmt.Errorf("reboot verification worker deadline expired")
		}
		miner, err := coordinator.discover(ctx, request.macAddr)
		if err == nil {
			lastMiner = miner
			if _, unsafe := provenUnsafeReadback(request, miner.Info, lastASIC); unsafe {
				return rebootReadback{miner: miner, asic: lastASIC, disposition: rebootReadbackUnsafe}, nil
			}
			if identityErr := validateMutationIdentity(miner.Info, request.macAddr); identityErr != nil {
				lastErr = identityErr
				goto retry
			}
			if request.kind == lib.MutationOperatingPoint || request.kind == lib.MutationSafetyRollback || request.kind == lib.MutationFirmwareRecovery {
				rediscoveredASIC, asicErr := coordinator.devices.GetASICSettings(ctx, miner.IP)
				if asicErr != nil {
					lastErr = fmt.Errorf("read advertised operating points after rediscovery: %w", asicErr)
					goto retry
				}
				lastASIC = rediscoveredASIC
				if _, unsafe := provenUnsafeReadback(request, miner.Info, lastASIC); unsafe {
					return rebootReadback{miner: miner, asic: lastASIC, disposition: rebootReadbackUnsafe}, nil
				}
				if err := canonicalASICGrid(rediscoveredASIC); err != nil {
					lastErr = fmt.Errorf("rediscovered ASIC grid is unsupported: %w", err)
					goto retry
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
					return rebootReadback{miner: miner, asic: lastASIC}, fmt.Errorf("reboot verification worker deadline expired after boot proof")
				}
				if verifyErr := verifyConfiguredReadback(request, miner.Info); verifyErr != nil {
					lastErr = verifyErr
					if !completeSafetyTelemetry(miner.Info) || hasPowerFault(miner.Info) ||
						!validLivePoint(operatingPointFromInfo(miner.Info)) {
						goto retry
					}
					assessment := assessInstantaneousSafety(
						miner.Info, request.settings, operatingPointFromInfo(miner.Info), lib.OperatingPoint{},
					)
					if assessment.action != safetyNormal {
						goto retry
					}
					observed := operatingPointFromInfo(miner.Info)
					if observed == mismatch {
						mismatchCount++
					} else {
						mismatch = observed
						mismatchCount = 1
					}
					if mismatchCount >= manualConfirmationPolls && request.kind == lib.MutationOperatingPoint {
						disposition := rebootReadbackExternalPoint
						if axeOSReducedPoint(request.point, observed) {
							disposition = rebootReadbackFirmwareReduction
						}
						return rebootReadback{miner: miner, asic: lastASIC, disposition: disposition}, nil
					}
					goto retry
				}
				if !coordinator.now().Before(deadline) {
					return rebootReadback{miner: miner, asic: lastASIC}, fmt.Errorf("reboot verification worker deadline expired after readback")
				}
				return rebootReadback{miner: miner, asic: lastASIC, disposition: rebootReadbackVerified}, nil
			}
			lastErr = fmt.Errorf("new boot proof is not established")
		} else {
			lastErr = err
		}
	retry:
		if !coordinator.now().Before(deadline) {
			return rebootReadback{miner: lastMiner, asic: lastASIC}, fmt.Errorf("reboot verification worker deadline expired: %w", lastErr)
		}
		if err := coordinator.waitMutationRetry(ctx); err != nil {
			return rebootReadback{}, err
		}
	}
}

func axeOSReducedPoint(requested, observed lib.OperatingPoint) bool {
	const reduction = 100
	return requested.Frequency > reduction && requested.CoreVoltage > reduction &&
		observed.Frequency == requested.Frequency-reduction &&
		observed.CoreVoltage == requested.CoreVoltage-reduction
}

// firmwareRecoveryTarget normalizes AxeOS's autonomous paired reduction onto
// the advertised complete-pair grid without increasing either component when
// a dominated advertised pair exists. The exact minimum is the only safe
// fallback when the firmware reduced below every advertised component.
func firmwareRecoveryTarget(
	asic lib.ASICSettings,
	reduced lib.OperatingPoint,
) (lib.OperatingPoint, error) {
	if err := canonicalASICGrid(asic); err != nil {
		return lib.OperatingPoint{}, err
	}
	if operatingPointAdvertised(asic, reduced) {
		return reduced, nil
	}
	frequency, frequencyOK := floorAdvertisedOption(asic.FrequencyOptions, reduced.Frequency)
	voltage, voltageOK := floorAdvertisedOption(asic.VoltageOptions, reduced.CoreVoltage)
	if frequencyOK && voltageOK {
		return lib.OperatingPoint{Frequency: frequency, CoreVoltage: voltage}, nil
	}
	return minimumAdvertisedPoint(asic)
}

func floorAdvertisedOption(options []int, maximum int) (int, bool) {
	index := sort.Search(len(options), func(index int) bool { return options[index] > maximum })
	if index == 0 {
		return 0, false
	}
	return options[index-1], true
}

func (coordinator *mutationCoordinator) reconcileFirmwareReduction(
	request mutationRequest,
	readback rebootReadback,
) error {
	observed := operatingPointFromInfo(readback.miner.Info)
	if request.kind != lib.MutationOperatingPoint ||
		readback.miner.Info.MacAddr != request.macAddr ||
		readback.miner.Info.OverHeatMode != 0 ||
		!axeOSReducedPoint(request.point, observed) ||
		!completeSafetyTelemetry(readback.miner.Info) || hasPowerFault(readback.miner.Info) {
		return fmt.Errorf("post-boot readback is not a safe AxeOS paired reduction")
	}
	if assessment := assessInstantaneousSafety(
		readback.miner.Info, request.settings, observed, lib.OperatingPoint{},
	); assessment.action != safetyNormal {
		return fmt.Errorf("post-boot AxeOS reduction is not inside normal safety limits")
	}
	target, err := firmwareRecoveryTarget(readback.asic, observed)
	if err != nil {
		return fmt.Errorf("select advertised firmware recovery pair: %w", err)
	}
	state, err := coordinator.states.LoadMiner(request.macAddr)
	if err != nil {
		return err
	}
	expected := state
	now := coordinator.now()
	if readback.miner.IP != "" {
		state.IP = readback.miner.IP
	}
	if state.Phase != lib.PhaseEmergency {
		state.EmergencyCount = incrementEmergencyCount(state.EmergencyCount)
	}
	state.SetCurrentPoint(observed)
	state.SetFallbackPoint(lib.OperatingPoint{})
	state.ClearPendingMutation()
	state.Phase = lib.PhaseEmergency
	state.PhaseStartedAt = now
	state.MonitorReason = ""
	state.MonitorReferenceEpochID = 0
	state.SettledAt = time.Time{}
	state.SafetyReason = lib.SafetyReasonFirmwareOverheat
	state.RecoveryHealthyCount = 0
	state.UnreadablePollCount = 0
	state.ObservedFrequency = 0
	state.ObservedCoreVoltage = 0
	state.ObservedCount = 0
	if target == observed {
		state.Phase = lib.PhaseCooldown
	} else if safeToRecover(readback.miner.Info, request.settings) {
		state.SetPendingMutation(lib.MutationFirmwareRecovery, target, now)
	}
	_, err = coordinator.states.Apply(lib.SupersedeMutation{
		Expected: expected, State: state, AttemptID: request.attemptID,
	}, now)
	return err
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
			if result.stateReconciled && result.miner.IP != "" {
				coordinator.routes[result.macAddr] = result.miner
			}
			if storedAttempt.FailureStage == lib.MutationFailureSafetySuperseded {
				// Safety arbitration already consumed this worker's authority. Its late result is the
				// same terminal fact, not a second mutation failure and not an operator-facing error.
				continue
			}
			if result.err != nil {
				if result.failureStage != "" && !result.stateReconciled {
					if terminalErr := coordinator.handleTerminalMutationFailureLocked(result, storedAttempt); terminalErr != nil {
						return processed, terminalErr
					}
				}
				if result.kind == lib.MutationMiningConfiguration && result.failureStage != "" &&
					!errors.Is(result.err, context.Canceled) {
					coordinator.startupBlocked = result.hostname
				}
				if !errors.Is(result.err, context.Canceled) {
					if result.failureStage == "" {
						coordinator.logger.Printf(
							"Mutation %s remains incomplete for %s and will retry: %s",
							result.kind,
							result.hostname,
							result.err,
						)
					} else {
						coordinator.logger.Printf(
							"Mutation %s failed for %s: %s",
							result.kind,
							result.hostname,
							result.err,
						)
					}
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
	state, err := coordinator.states.LoadMiner(result.macAddr)
	if err != nil {
		return err
	}
	now := coordinator.now()
	if _, err := coordinator.settings.ForHost(state.Hostname); err != nil {
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
				// A readback-unavailable failure this early means the mutation never reached
				// CompleteMutation, let alone CompleteResume, so this candidate never had a trial
				// epoch to close (zero-value Epoch).
				_, err := coordinator.states.Apply(lib.FailMutationFinalizeTrial{
					State: state, Record: record, Decision: lib.TrialReturn, AttemptID: result.attemptID,
					Stage: result.failureStage,
				}, now)
				return err
			}
		}
	}
	if result.kind == lib.MutationMiningConfiguration {
		state.MiningPending = true
		coordinator.startupBlocked = state.Hostname
		_, err := coordinator.states.Apply(lib.FailMutation{State: state, AttemptID: result.attemptID, Stage: result.failureStage}, now)
		return err
	}
	if result.kind == lib.MutationSafetyRollback || result.kind == lib.MutationFirmwareRecovery {
		// A safety attempt that reaches a readable same-obligation mismatch or
		// an availability deadline keeps its typed safety intent. The failed
		// history row is terminal, but a later coordinator pass may create the
		// next recorded safety attempt after fresh preflight.
		state.SetFallbackPoint(lib.OperatingPoint{})
		state.SettledAt = time.Time{}
		state.MonitorReason = ""
		state.MonitorReferenceEpochID = 0
		_, err := coordinator.states.Apply(lib.FailMutation{State: state, AttemptID: result.attemptID, Stage: result.failureStage}, now)
		return err
	}
	state.ClearPendingMutation()
	state.SetFallbackPoint(lib.OperatingPoint{})
	// Only an operating-point attempt reaches this fallback: mining and safety kinds return above.
	// The pipeline never proved the target active, and no evidence epoch exists from which a
	// starvation successor could reconstruct the interrupted trial phase, so this is a rejected
	// point requiring an explicit retune.
	state.Phase = lib.PhaseMonitor
	state.MonitorReason = lib.MonitorRejected
	state.SettledAt = time.Time{}
	_, err = coordinator.states.Apply(lib.FailMutation{State: state, AttemptID: result.attemptID, Stage: result.failureStage}, now)
	return err
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
	if _, err := coordinator.settings.ForHost(state.Hostname); err != nil {
		return err
	}
	if !attempt.CompletedAt.IsZero() {
		if result.miner.IP != "" {
			state.IP = result.miner.IP
		}
		coordinator.routes[result.macAddr] = result.miner
		_, err := coordinator.states.Apply(lib.CompleteMutation{
			MacAddr: state.MacAddr, IP: state.IP, AttemptID: result.attemptID,
		}, attempt.CompletedAt)
		return err
	}

	if result.miner.IP != "" {
		state.IP = result.miner.IP
	}
	if _, err := coordinator.states.Apply(lib.CompleteMutation{
		MacAddr: state.MacAddr, IP: state.IP, AttemptID: result.attemptID,
	}, completedAt); err != nil {
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
		result, err := coordinator.states.Apply(lib.SupersedeMutation{
			Expected: observation.state, State: observation.state, AttemptID: active.attemptID,
		}, now)
		if err != nil {
			return fmt.Errorf("close superseded mutation %d: %w", active.attemptID, err)
		}
		observation.state = result.State
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
	asic lib.ASICSettings,
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
			(state.CurrentPoint() == operatingPointFromInfo(info) || !safeToRecover(info, settings)) {
			return fmt.Errorf("safety rollback target is already the configured live pair")
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
		case lib.PhaseEmergency:
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
	case lib.MutationFirmwareRecovery:
		if state.Phase != lib.PhaseEmergency {
			return fmt.Errorf("firmware recovery has no durable emergency episode")
		}
		if state.SafetyReason != lib.SafetyReasonFirmwareOverheat {
			return fmt.Errorf("firmware recovery has no verified firmware-overheat cause")
		}
		target, err := firmwareRecoveryTarget(asic, state.CurrentPoint())
		if err != nil || state.PendingPoint() != target {
			return fmt.Errorf("firmware recovery target is not the normalized advertised pair")
		}
		if !safeToRecover(info, settings) {
			return fmt.Errorf("device is not cool enough for firmware recovery")
		}
		if info.OverHeatMode == 0 && info.Frequency != 50 &&
			operatingPointFromInfo(info) == target && state.CurrentPoint() == target {
			return fmt.Errorf("firmware recovery is already complete at the advertised pair")
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
	if err := validateKindSafety(info, settings, state, asic, minimum); err != nil {
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
	info lib.Info,
	asic lib.ASICSettings,
) bool {
	_, unsafe := provenUnsafeReadback(request, info, asic)
	return unsafe
}

func (coordinator *mutationCoordinator) safetyIntentAlreadySatisfied(
	request mutationRequest,
	info lib.Info,
	asic lib.ASICSettings,
) bool {
	if request.kind != lib.MutationSafetyRollback && request.kind != lib.MutationFirmwareRecovery {
		return false
	}
	if canonicalASICGrid(asic) != nil || operatingPointFromInfo(info) != request.point ||
		info.OverHeatMode != 0 || info.Frequency == 50 || hasPowerFault(info) ||
		!completeSafetyTelemetry(info) {
		return false
	}
	assessment := assessInstantaneousSafety(info, request.settings, request.point, lib.OperatingPoint{})
	return assessment.action == safetyNormal
}

func (coordinator *mutationCoordinator) reconcileSatisfiedSafetyIntent(
	request mutationRequest,
	state lib.MinerState,
	info lib.Info,
) error {
	expected := state
	now := coordinator.now()
	state.SetCurrentPoint(operatingPointFromInfo(info))
	state.SetFallbackPoint(lib.OperatingPoint{})
	state.ClearPendingMutation()
	state.Phase = lib.PhaseCooldown
	state.PhaseStartedAt = now
	state.MonitorReason = ""
	state.MonitorReferenceEpochID = 0
	state.SettledAt = time.Time{}
	state.RecoveryHealthyCount = 0
	state.ObservedFrequency = 0
	state.ObservedCoreVoltage = 0
	state.ObservedCount = 0
	_, err := coordinator.states.Apply(lib.SupersedeMutation{
		Expected: expected, State: state, AttemptID: request.attemptID,
	}, now)
	return err
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
	if request.ip != "" {
		state.IP = request.ip
	}
	assessment, unsafe := provenUnsafeReadback(request, observed, asic)
	if !unsafe {
		return fmt.Errorf("readback does not prove an unsafe device state")
	}
	minimum, minimumErr := minimumAdvertisedPoint(asic)
	if canonicalASICGrid(asic) != nil || minimumErr != nil || !supportedSafetyIdentity(observed) ||
		!operatingPointAdvertised(asic, minimum) {
		state.ClearPendingMutation()
		state.SetFallbackPoint(lib.OperatingPoint{})
		if state.Phase != lib.PhaseEmergency {
			state.EmergencyCount = incrementEmergencyCount(state.EmergencyCount)
			state.PhaseStartedAt = now
		}
		state.Phase = lib.PhaseEmergency
		state.MonitorReason = ""
		state.MonitorReferenceEpochID = 0
		state.SettledAt = time.Time{}
		state.SafetyReason = escalateSafetyReason(state.SafetyReason, reasonForSafetyFailure(assessment.failure))
		state.RecoveryHealthyCount = 0
		state.UnreadablePollCount = 0
		_, err := coordinator.states.Apply(lib.SupersedeMutation{Expected: expected, State: state, AttemptID: request.attemptID}, now)
		return err
	}
	if assessment.action == safetyRollback {
		target := minimum
		records, listErr := coordinator.states.ListPoints(state.MacAddr)
		if listErr != nil {
			return fmt.Errorf("read safety supersession evidence: %w", listErr)
		}
		failed := operatingPointFromInfo(observed)
		if selected, found := selectRollbackPoint(records, failed, asic, settings); found {
			target = selected
		}
		state.ClearPendingMutation()
		state.SetFallbackPoint(lib.OperatingPoint{})
		state.SafetyReason = escalateSafetyReason(state.SafetyReason, reasonForSafetyFailure(assessment.failure))
		state.Phase = lib.PhaseCooldown
		state.MonitorReason = ""
		state.MonitorReferenceEpochID = 0
		state.SettledAt = time.Time{}
		state.RecoveryHealthyCount = 0
		state.SetPendingMutation(lib.MutationSafetyRollback, target, now)
		_, err := coordinator.states.Apply(lib.SupersedeMutation{Expected: expected, State: state, AttemptID: request.attemptID}, now)
		return err
	}
	if _, err := transitionEmergencyState(&state, observed, asic, settings, now, assessment); err != nil {
		return err
	}
	_, err = coordinator.states.Apply(lib.SupersedeMutation{Expected: expected, State: state, AttemptID: request.attemptID}, now)
	return err
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
