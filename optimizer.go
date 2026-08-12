package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/willyrgf/bitagnis/lib"
)

const (
	manualConfirmationPolls = 2
	minimumHashGain         = 0.02
	undervoltHashTolerance  = 0.02
	minimumErrorImprovement = 1.0
	powerHeadroom           = 2.0
	vrExplorationFactor     = 0.90
	axeOSASICTripTemp       = 75.0
	axeOSVRTripTemp         = 105.0

	// maxRejectedWindows bounds the rejected-window budget for one evidence epoch (RFC "The budget
	// is a count"). It is asserted in the RFC, not derived: commit 0's per-cycle attribution was
	// meant to calibrate it against a measured clean-interval rate, but no real hardware measurement
	// exists yet. Exhausting it ends a baseline epoch as starved (starveBaseline) or a trial
	// candidate as PointStarved (finalizeTrial); monitor and safety_validation keep trying
	// regardless of it (see the exhaustion handling in controlMinerAfterSafety for why). Left at the
	// RFC's stated value pending real measurement.
	maxRejectedWindows = 6
)

// readablePoll is a poll whose identity, ASIC grid, and telemetry all validated. Only a readable
// poll can advance optimizer progress; construction is the proof, so no progress path re-checks it.
// Safety assessment deliberately does not require one: enforceMinerSafety keeps taking the weaker
// lib.Info/lib.ASICSettings pair so a thermal emergency accompanied by a malformed ASIC grid is
// still assessed and escalated on the same poll that observed it.
type readablePoll struct {
	info  lib.Info
	asic  lib.ASICSettings
	point lib.OperatingPoint
}

// newReadablePoll returns false when the canonical-ASIC-grid check fails, the supported-safety-
// identity check fails, the complete-safety-telemetry check fails, the live point is numerically
// invalid (or the firmware emergency sentinel), or any hash/error telemetry field is non-finite or
// out of range. An unreadable poll constructs no value, so no progress-advancing function can be
// called with it.
//
// Deliberately NOT a construction gate: operatingPointAdvertised. AGENTS.md's Thermal-Control
// invariants require that "off-grid manual points may be observed, but must never become automated
// request targets" — observability and automation eligibility are different facts. Gating
// readability on the live point matching the advertised grid would make an off-grid manual point
// permanently unreadable, silently breaking manual-point observation entirely. Automation call
// sites (baseline candidate selection, requestOperatingPoint, ...) already check
// operatingPointAdvertised themselves before targeting a point with hardware.
func newReadablePoll(info lib.Info, asic lib.ASICSettings) (readablePoll, bool) {
	if canonicalASICGrid(asic) != nil {
		return readablePoll{}, false
	}
	if !supportedSafetyIdentity(info) {
		return readablePoll{}, false
	}
	if !completeSafetyTelemetry(info) {
		return readablePoll{}, false
	}
	point := operatingPointFromInfo(info)
	if !validLivePoint(point) || point.Frequency == 50 {
		return readablePoll{}, false
	}
	if !finite(info.HashRate) || info.HashRate < 0 ||
		!finite(info.ExpectedHashRate) || info.ExpectedHashRate < 0 ||
		(info.ErrorPercentage != nil &&
			(!finite(*info.ErrorPercentage) || *info.ErrorPercentage < 0 || *info.ErrorPercentage > 100)) {
		return readablePoll{}, false
	}
	return readablePoll{info: info, asic: asic, point: point}, true
}

func (poll readablePoll) Info() lib.Info            { return poll.info }
func (poll readablePoll) ASIC() lib.ASICSettings    { return poll.asic }
func (poll readablePoll) Point() lib.OperatingPoint { return poll.point }

type telemetrySample struct {
	scheduledAt   time.Time
	point         lib.OperatingPoint
	phase         lib.OptimizerPhase
	hashRate      float64
	expectedHash  float64
	temp          float64
	vrTemp        float64
	power         float64
	errorPercent  *float64
	acceptedShare uint64
	rejectedShare uint64
}

// minerRuntime holds only in-memory, per-poll-cycle state: the current window's sample buffer and
// jitter accounting, and the small accounting-classification cache. It has no authority over
// durable evidence progress: that lives entirely in evidence_epochs, reachable only through
// lib.OptimizerStore.Apply. resetRuntime accordingly only ever clears these fields.
type minerRuntime struct {
	samples      []telemetrySample
	maxGap       time.Duration
	missed       int
	lastSampleAt time.Time
	lastPoint    lib.OperatingPoint
	lastPhase    lib.OptimizerPhase
	accounting   *accountingSample
	recovery     *recoverySample
}

// recoverySample is logRecoveryInstrumentation's in-memory-only memory of the previous
// COOLDOWN/EMERGENCY poll, never persisted or acted upon: it exists purely to log a safeToRecover
// transition or a temperature slope. The durable recovery predicate itself is
// recoveryHealthyPolls/RecoveryHealthyCount in controlMinerAfterSafety, a separate mechanism.
type recoverySample struct {
	at   time.Time
	temp float64
	safe bool
}

type accountingSample struct {
	at             time.Time
	point          lib.OperatingPoint
	phase          lib.OptimizerPhase
	referenceHash  float64
	hashRate       float64
	validHash      bool
	settled        bool
	classification accountingClassification
	state          lib.MinerState
}

// accountingClassification is the durable/runtime state that determines how
// a healthy interval is credited. A same-phase transition is still unknown
// until a poll establishes the new classification.
type accountingClassification struct {
	phase             lib.OptimizerPhase
	point             lib.OperatingPoint
	fallback          lib.OperatingPoint
	pendingKind       lib.MutationKind
	pendingPoint      lib.OperatingPoint
	miningPending     bool
	monitorReason     lib.MonitorReason
	safetyReason      lib.SafetyReason
	evidencePending   bool
	settled           bool
	referenceHash     float64
	passReferenceHash float64
}

func classifyAccountingState(
	state lib.MinerState,
	referenceHash float64,
	settled bool,
	hasOpenEpoch bool,
) accountingClassification {
	return accountingClassification{
		phase:             state.Phase,
		point:             state.CurrentPoint(),
		fallback:          state.FallbackPoint(),
		pendingKind:       state.PendingKind,
		pendingPoint:      state.PendingPoint(),
		miningPending:     state.MiningPending,
		monitorReason:     state.MonitorReason,
		safetyReason:      state.SafetyReason,
		evidencePending:   hasOpenEpoch,
		settled:           settled,
		referenceHash:     referenceHash,
		passReferenceHash: state.PassReferenceHash,
	}
}

func accountingSamplesCompatible(
	previous *accountingSample,
	current accountingSample,
	cursor time.Time,
	maxGap time.Duration,
) bool {
	return current.validHash && previous != nil && previous.validHash &&
		previous.at.Equal(cursor) && current.at.After(previous.at) &&
		current.at.Sub(previous.at) <= maxGap &&
		previous.classification == current.classification
}

type safetyFailure struct {
	status string
	reason string
}

type safetyAction int

const (
	safetyNormal safetyAction = iota
	safetyRollback
	safetyHostContainment
	safetyFirmwareRecovery
	safetyEmergencyHold
	safetyUnavailable
)

type safetyAssessment struct {
	action  safetyAction
	failure safetyFailure
}

// saveMinerState persists state through Apply's SaveState transition and refreshes state from the
// result. It replaces the direct SaveMiner call at every optimizer call site.
func (minerController *controller) saveMinerState(state *lib.MinerState, at time.Time) error {
	result, err := minerController.states.Apply(lib.SaveState{State: *state}, at)
	if err != nil {
		return err
	}
	*state = result.State
	return nil
}

// openEpochTransition opens an evidence epoch for state's current point and refreshes state from
// the result. It is the one call site every standalone (non-embedded) epoch open goes through.
func (minerController *controller) openEpochTransition(
	state *lib.MinerState,
	purpose lib.EpochPurpose,
	requiredWindows int,
	at time.Time,
) error {
	result, err := minerController.states.Apply(lib.OpenEpoch{
		State: *state, Purpose: purpose, Point: state.CurrentPoint(), RequiredWindows: requiredWindows,
	}, at)
	if err != nil {
		return err
	}
	*state = result.State
	return nil
}

func (minerController *controller) controlMiner(
	ctx context.Context,
	state *lib.MinerState,
	info lib.Info,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) error {
	handled, err := minerController.enforceMinerSafety(ctx, state, info, asic, settings, now)
	if err != nil || handled {
		return err
	}
	poll, ok := newReadablePoll(info, asic)
	if !ok {
		// enforceMinerSafety owns unreadable-poll accounting. Anything still rejected here is a
		// non-event for evidence: it cannot advance a phase or an epoch.
		return nil
	}
	return minerController.controlMinerAfterSafety(ctx, state, poll, settings, now, true)
}

// enforceMinerSafety runs on whatever validated: a readablePoll is not required, because splitting
// grid-canonicality from telemetry-validity is exactly what lets a device reporting dangerous
// telemetry alongside a malformed ASIC grid still escalate on this same poll.
func (minerController *controller) enforceMinerSafety(
	ctx context.Context,
	state *lib.MinerState,
	info lib.Info,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) (bool, error) {
	if state == nil {
		return true, fmt.Errorf("control miner: state is nil")
	}
	minerController.logRecoveryInstrumentation(*state, info, settings, now)
	if err := canonicalASICGrid(asic); err != nil {
		firmwareOverheat := info.OverHeatMode != 0 || info.Frequency == 50
		firmwareTrip := knownFirmwareTripExceeded(info)
		assessment := assessInstantaneousSafety(info, settings, operatingPointFromInfo(info), lib.OperatingPoint{})
		// Grid canonicality and telemetry validity are independent facts about one response. The
		// firmwareOverheat/firmwareTrip branch and the final unsafe-assessment branch below survive
		// unchanged because info.OverHeatMode, info.Frequency, and assessInstantaneousSafety's other
		// inputs do not depend on the grid — a device reporting a real emergency with a malformed
		// ASIC grid is a thermal emergency, not a read failure, and must never be suppressed. Only
		// the case where NEITHER fires is genuinely uninformative; that case is a non-event below.
		if firmwareOverheat || firmwareTrip || (assessment.action != safetyNormal && assessment.action != safetyUnavailable) {
			expected := *state
			state.ClearPendingMutation()
			state.SetFallbackPoint(lib.OperatingPoint{})
			state.SettledAt = time.Time{}
			if firmwareOverheat || firmwareTrip {
				if state.Phase != lib.PhaseEmergency {
					state.EmergencyCount = incrementEmergencyCount(state.EmergencyCount)
					state.PhaseStartedAt = now
				}
				state.Phase = lib.PhaseEmergency
				state.MonitorReason = ""
				state.MonitorReferenceEpochID = 0
				if firmwareOverheat {
					state.SafetyReason = escalateSafetyReason(state.SafetyReason, lib.SafetyReasonFirmwareOverheat)
				} else {
					state.SafetyReason = escalateSafetyReason(state.SafetyReason, lib.SafetyReasonFirmwareTrip)
				}
			} else {
				if state.Phase != lib.PhaseEmergency {
					state.EmergencyCount = incrementEmergencyCount(state.EmergencyCount)
					state.PhaseStartedAt = now
				}
				state.Phase = lib.PhaseEmergency
				state.MonitorReason = ""
				state.MonitorReferenceEpochID = 0
				state.SafetyReason = escalateSafetyReason(state.SafetyReason, reasonForSafetyFailure(assessment.failure))
			}
			state.UnreadablePollCount = 0
			// A fresh or continuing EMERGENCY episode never carries a COOLDOWN dwell count forward:
			// RecoveryHealthyCount only advances while Phase is COOLDOWN, so any nonzero value here
			// belongs to a dwell this new emergency has already invalidated, not to this episode.
			state.RecoveryHealthyCount = 0
			minerController.resetRuntime(state.MacAddr)
			if attempt, unfinished, attemptErr := minerController.states.UnfinishedMutationAttempt(state.MacAddr); attemptErr != nil {
				return true, fmt.Errorf("unsupported ASIC grid: %w; load mutation authority: %v", err, attemptErr)
			} else if unfinished {
				supersedeResult, supersedeErr := minerController.states.Apply(lib.SupersedeMutation{
					Expected: expected, State: *state, AttemptID: attempt.ID,
				}, now)
				if supersedeErr != nil {
					return true, fmt.Errorf("unsupported ASIC grid: %w; supersede mutation: %v", err, supersedeErr)
				}
				*state = supersedeResult.State
				return true, nil
			}
			result, saveErr := minerController.states.Apply(lib.SafetyTransition{
				Expected: expected, State: *state,
			}, now)
			if saveErr != nil {
				return true, fmt.Errorf("unsupported ASIC grid: %w; persist block: %v", err, saveErr)
			}
			*state = result.State
			return true, nil
		}
		// Non-event: the grid failed validation, there is no firmware emergency, and the assessment
		// over whatever telemetry did validate is not unsafe. It produces no phase transition, no
		// supersession, no ClearPendingMutation, no SetFallbackPoint, and no clearing of settled,
		// ramp, or epoch state — a poll that yields no information causes no transition. Escalation
		// is by a durable count, not by elapsed time or by the controller's own prior state.
		return true, minerController.recordUnreadablePoll(state, settings, now)
	}
	minimum, err := minimumAdvertisedPoint(asic)
	if err != nil {
		return true, err
	}
	live := operatingPointFromInfo(info)
	assessment := assessInstantaneousSafety(info, settings, live, minimum)
	// A canonical grid does not make incomplete telemetry informative. In particular, AxeOS 600-
	// series boards cannot report ASIC temperature while firmware has powered the ASIC down, and a
	// host-requested reboot has the same transient shape. An unavailable poll may not revoke typed
	// safety authority; the emergency transition below also preserves a neutral poll's unchanged
	// disposition byte-for-byte.
	if assessment.action == safetyUnavailable {
		return true, nil
	}
	if assessment.action == safetyNormal &&
		(state.PendingKind == lib.MutationSafetyRollback || state.PendingKind == lib.MutationFirmwareRecovery) {
		if _, unfinished, err := minerController.states.UnfinishedMutationAttempt(state.MacAddr); err != nil {
			return true, fmt.Errorf("preserve neutral safety mutation: %w", err)
		} else if unfinished {
			return true, nil
		}
	}
	if state.Phase == lib.PhaseEmergency ||
		assessment.action == safetyHostContainment ||
		assessment.action == safetyFirmwareRecovery ||
		assessment.action == safetyEmergencyHold {
		return true, minerController.handleEmergency(state, info, asic, settings, now, assessment)
	}
	if assessment.action == safetyRollback {
		return true, minerController.rollbackForSafety(
			ctx, state, live, info, asic, settings, now, assessment.failure, true,
		)
	}
	if state.PendingKind != "" || state.MiningPending {
		return true, nil
	}
	return false, nil
}

// unreadablePollLimit bounds how many consecutive unreadable polls the optimizer tolerates before
// escalating to a safety-unknown episode. It must exceed the longest expected legitimate unreadable
// interval, which is a reboot; deriving it from defaultRebootDeadline keeps the two bounds tied to
// one number instead of two independently-tunable constants that could silently drift apart.
func unreadablePollLimit(settings lib.Settings) int {
	if settings.MetricsTime <= 0 {
		return 1
	}
	return max(1, int(math.Ceil(float64(defaultRebootDeadline)/float64(settings.MetricsTime))))
}

// recoveryDwellTime is the physical dwell recoveryHealthyPolls requires before COOLDOWN exits. It
// is derived from, not guessed independently of, AxeOS's own autonomous overheat recovery
// (esp-miner power_management_task.c): once the firmware itself detects an overheat, it stops
// mining and loops until board temperature has been at or below its own 45C safe threshold for at
// least six consecutive 5-second checks (30s), resetting that count on any reading above 45C,
// before it re-powers the ASIC. That is firmware's own bar for "safe to run again after being
// fully powered down," a harder case than Bitagnis's more common gentler rollback (ASIC never
// stopped). recoveryDwellTime is set to twice that firmware minimum: Bitagnis's own poll cadence is
// not tied to the actual thermal event the way firmware's internal 5-second loop is, and the poll
// loop itself is known to lose ticks (see RFC_STATE_LINEAR_PROGRESS.md), so a poll-count-based
// dwell needs headroom a tightly-coupled hardware loop does not.
const recoveryDwellTime = 60 * time.Second

// recoveryHealthyPolls is the number of consecutive satisfying safeToRecover polls COOLDOWN
// requires before it exits — a physical dwell, not a timer: it is reset by any non-satisfying poll
// and not advanced by an unreadable one (readablePoll construction already excludes those from ever
// reaching this check), so a degraded poll yield makes recovery take longer, never falsely sooner.
func recoveryHealthyPolls(settings lib.Settings) int {
	if settings.MetricsTime <= 0 {
		return 1
	}
	return max(1, int(math.Ceil(float64(recoveryDwellTime)/float64(settings.MetricsTime))))
}

// recordUnreadablePoll advances the durable count of consecutive polls the optimizer could not
// assess (grid invalid, no firmware emergency, no unsafe reading over whatever telemetry did
// validate). Only unreadablePollLimit consecutive unreadable polls escalate to a safety-unknown
// episode — the legitimate residue of the deleted safetyOwned branch: a safety-owned miner that goes
// unreadable and stays unreadable is escalated by the count, not by its own history. An unfinished
// mutation attempt with a restart already requested and no reboot proof yet suppresses escalation
// entirely for its duration: the ledger already states the device is expected to be unreadable, and
// this counter must not contradict it. This is the specific change that would have prevented the
// mineira supersession.
func (minerController *controller) recordUnreadablePoll(
	state *lib.MinerState,
	settings lib.Settings,
	now time.Time,
) error {
	attempt, unfinished, err := minerController.states.UnfinishedMutationAttempt(state.MacAddr)
	if err != nil {
		return fmt.Errorf("record unreadable poll: load mutation authority: %w", err)
	}
	if unfinished && !attempt.RestartRequestedAt.IsZero() && attempt.RebootVerifiedAt.IsZero() {
		return nil
	}
	count := state.UnreadablePollCount + 1
	if count < unreadablePollLimit(settings) {
		state.UnreadablePollCount = count
		return minerController.saveMinerState(state, now)
	}
	expected := *state
	state.ClearPendingMutation()
	state.SetFallbackPoint(lib.OperatingPoint{})
	state.SettledAt = time.Time{}
	if state.Phase != lib.PhaseEmergency {
		state.EmergencyCount = incrementEmergencyCount(state.EmergencyCount)
		state.PhaseStartedAt = now
	}
	state.Phase = lib.PhaseEmergency
	state.MonitorReason = ""
	state.MonitorReferenceEpochID = 0
	state.SafetyReason = escalateSafetyReason(state.SafetyReason, lib.SafetyReasonTelemetryUnavailable)
	state.UnreadablePollCount = 0
	// See the matching reset in enforceMinerSafety: a dwell count from a prior COOLDOWN belongs to
	// that episode, not this new escalation.
	state.RecoveryHealthyCount = 0
	minerController.resetRuntime(state.MacAddr)
	if unfinished {
		supersedeResult, supersedeErr := minerController.states.Apply(lib.SupersedeMutation{
			Expected: expected, State: *state, AttemptID: attempt.ID,
		}, now)
		if supersedeErr != nil {
			return fmt.Errorf("record unreadable poll: supersede mutation: %w", supersedeErr)
		}
		*state = supersedeResult.State
		return nil
	}
	result, err := minerController.states.Apply(lib.SafetyTransition{
		Expected: expected, State: *state,
	}, now)
	if err != nil {
		return err
	}
	*state = result.State
	return nil
}

// controlMinerAfterSafety implements the per-poll evidence-epoch lifecycle. Safety-owned phases
// and pending authority are handled before live-point drift; ordinary reconciliation then precedes
// the shared evidence-epoch lifecycle.
func (minerController *controller) controlMinerAfterSafety(
	ctx context.Context,
	state *lib.MinerState,
	poll readablePoll,
	settings lib.Settings,
	now time.Time,
	allowOptimization bool,
) error {
	if state == nil {
		return fmt.Errorf("control miner after safety: state is nil")
	}
	asic := poll.ASIC()
	livePoint := poll.Point()
	if !validLivePoint(state.CurrentPoint()) {
		if !validLivePoint(livePoint) || livePoint.Frequency == 50 {
			return fmt.Errorf("%s reported invalid normal operating point %d MHz/%d mV", state.Hostname, livePoint.Frequency, livePoint.CoreVoltage)
		}
		state.SetCurrentPoint(livePoint)
		// A newly usable live point enters continuous monitoring. An unadvertised pair remains
		// observable but cannot become an automated target unless a later canonical AxeOS grid
		// advertises that exact pair.
		if canonicalASICGrid(asic) != nil || !operatingPointAdvertised(asic, livePoint) {
			state.Phase = lib.PhaseMonitor
			state.MonitorReason = lib.MonitorOffGrid
			state.MonitorReferenceEpochID = 0
			state.SettledAt = time.Time{}
			minerController.resetRuntime(state.MacAddr)
			return minerController.saveMinerState(state, now)
		}
		state.Phase = lib.PhaseMonitor
		state.MonitorReason = lib.MonitorRejected
		state.PhaseStartedAt = now
		minerController.resetRuntime(state.MacAddr)
		return minerController.saveMinerState(state, now)
	}
	// Live-point reconciliation is an input to every phase, not a phase itself: it must not precede
	// the PendingKind/Overheat/Cooldown recovery handling immediately below — the RFC's own cited
	// range (optimizer.go:311-333 in the pre-cutover file) covers exactly that block, not the
	// monitor handling further down — or the satisfied recovery predicate below becomes unreachable
	// whenever the live point also differs, exactly the ordering defect that left mineira deadlocked
	// in COOLDOWN. Reconciliation is deliberately positioned before monitoring
	// so a selected or manual miner's live-point drift is still reconciled — moving it later would
	// stop reconciling drift for any settled miner, a regression this fix does not intend. The
	// ObservedCount reset stays here: it is independent of phase, and it must run before a phase
	// early-return either way, since it only fires when the live point already matches durable
	// current.
	if livePoint == state.CurrentPoint() && state.ObservedCount != 0 {
		state.ObservedCount = 0
		state.ObservedFrequency = 0
		state.ObservedCoreVoltage = 0
		if err := minerController.saveMinerState(state, now); err != nil {
			return err
		}
	}
	if state.PendingKind != "" || state.MiningPending {
		return nil
	}
	if state.Phase == lib.PhaseEmergency {
		return nil
	}
	// COOLDOWN's exit predicate is a durable count of consecutive polls proving safeToRecover holds
	// — a physical dwell, not a timer (see recoveryHealthyPolls). This runs before reconciliation is
	// even attempted (see the ordering note above), so a satisfied recovery predicate is evaluated
	// on this same poll before any live-point mismatch can short-circuit it.
	if state.Phase == lib.PhaseCooldown {
		_, open, err := minerController.states.OpenEvidenceEpochFor(state.MacAddr)
		if err != nil {
			return err
		}
		if !open {
			// Still counting toward the recovery threshold; no epoch exists yet to fall through to.
			safe := safeToRecover(poll.Info(), settings)
			if !safe {
				if state.RecoveryHealthyCount == 0 {
					return nil
				}
				state.RecoveryHealthyCount = 0
				return minerController.saveMinerState(state, now)
			}
			if state.RecoveryHealthyCount+1 < recoveryHealthyPolls(settings) {
				state.RecoveryHealthyCount++
				return minerController.saveMinerState(state, now)
			}
			// The threshold is met on this poll. Retiring SafetyReason here, in the same transaction
			// that proves recovery, is deliberate: this is the one place "the device has now proven
			// itself safe for recoveryHealthyPolls consecutive polls" is decided, so it is the
			// correct place to retire the cause rather than leaving it a permanent, operator-only
			// flag (see the RFC's material uncertainty on this exact question).
			state.RecoveryHealthyCount = 0
			state.SafetyReason = ""
			return minerController.openEpochTransition(state, lib.EpochSafetyValidation, 1, now)
		}
		// The recovery threshold was already met on an earlier poll and a safety_validation epoch is
		// open: fall through to the shared epoch lifecycle below.
	}
	if livePoint != state.CurrentPoint() {
		return minerController.observeExternalPoint(state, poll, settings, now)
	}
	// MONITOR is an active evidence phase. It deliberately falls through to the shared epoch
	// lifecycle even after a point has been selected and settled.

	epoch, open, err := minerController.states.OpenEvidenceEpochFor(state.MacAddr)
	if err != nil {
		return err
	}
	if open && epoch.Point != state.CurrentPoint() {
		// Contradiction: the epoch's subject changed underneath it (RFC "Contradiction ends the
		// epoch"). Close it; CloseEpoch derives the exact normal successor from durable state in the
		// same transaction when this phase still requires evidence authority.
		result, err := minerController.states.Apply(lib.CloseEpoch{
			State: *state, Epoch: epoch, Outcome: lib.EpochContradicted,
		}, now)
		if err != nil {
			return err
		}
		*state = result.State
		minerController.resetRuntime(state.MacAddr)
		return nil
	}
	if !open {
		// Nothing accumulates without an epoch. Every phase reachable here already had its epoch
		// opened by the transition that put the miner in it (Bootstrap, StartPass, CompleteResume,
		// AdoptManualPoint/AdoptExternalPoint, CompleteBaseline, or starveBaseline).
		return nil
	}

	runtime := minerController.runtimeFor(state.MacAddr)
	missedSincePrevious := pollGapMissed(runtime, settings, now)

	// The first assessment at a new monitor point uses the ordinary hardware-settling ramp. Stable
	// successor assessments reuse the same proven point and therefore begin collecting their wider
	// two-window evidence immediately.
	if epoch.Progress.SettledSamples() < rampSamples(settings) &&
		!(epoch.Purpose == lib.EpochMonitor && state.MonitorReferenceEpochID > 0) {
		progress := epoch.Progress
		progress.ObserveSample(missedSincePrevious)
		return minerController.advanceEpoch(state, epoch, progress, now)
	}
	// The fleet gate blocks normal mutation authority, not evidence itself. A baseline may durably
	// close its first of two windows while another miner owns emergency recovery; it pauses before
	// the second window whose completion can admit a candidate. Trials pause immediately because
	// even their first window may trigger an early return mutation. Safety and hold evidence never
	// creates frontier authority and remains independent of this gate.
	if !allowOptimization {
		pause := epoch.Purpose == lib.EpochTrial ||
			epoch.Purpose == lib.EpochBaseline && epoch.Progress.ClosedWindows()+1 >= epoch.RequiredWindows
		if pause {
			// The paused interval is coordinator policy, not missing telemetry inside an evaluation
			// window. Drop only the in-memory partial window so resumption starts with a clean gap budget.
			minerController.resetRuntime(state.MacAddr)
			return nil
		}
	}

	window, closedWindow := minerController.addSample(poll, *state, settings, now)
	if !closedWindow {
		progress := epoch.Progress
		progress.ObserveSample(missedSincePrevious)
		return minerController.advanceEpoch(state, epoch, progress, now)
	}
	aggregate, admitted := window.admit(settings)
	if !admitted {
		progress := epoch.Progress
		progress.ObserveSample(missedSincePrevious)
		if err := progress.CloseWindow(false, aggregate); err != nil {
			return err
		}
		if progress.RejectedWindows() >= maxRejectedWindows {
			// Exhausted baseline evidence moves into continuous starved monitoring at the same point.
			// A starved trial is returned like any other failed candidate. Monitor and safety evidence
			// never become absorbing: rejected windows remain in the same open epoch until a complete
			// assessment can be made, while instantaneous safety still runs on every poll.
			switch epoch.Purpose {
			case lib.EpochBaseline:
				return minerController.starveBaseline(state, epoch, now)
			case lib.EpochTrial:
				return minerController.finalizeTrial(state, epoch, aggregate, lib.PointStarved, lib.TrialReturn, settings, now)
			}
		}
		return minerController.advanceEpoch(state, epoch, progress, now)
	}
	if epoch.Progress.ClosedWindows()+1 < epoch.RequiredWindows {
		if epoch.Purpose == lib.EpochTrial {
			records, err := minerController.states.ListPoints(state.MacAddr)
			if err != nil {
				return err
			}
			entered, found := findRecord(records, epoch.Point)
			if !found || entered.EntryAttemptID <= 0 {
				return fmt.Errorf("trial %d/%d has no entry authority", epoch.Point.Frequency, epoch.Point.CoreVoltage)
			}
			prior, _ := findRecord(records, state.FallbackPoint())
			if status, ok := minerController.trialWindowAdmissible(state, aggregate, entered, prior, settings); !ok {
				return minerController.finalizeTrial(state, epoch, aggregate, status, lib.TrialReturn, settings, now)
			}
		}
		progress := epoch.Progress
		if err := progress.CloseWindow(true, aggregate); err != nil {
			return err
		}
		return minerController.advanceEpoch(state, epoch, progress, now)
	}

	var combined lib.WindowAggregate
	if stored, ok := epoch.Progress.ClosedWindow(); ok {
		combined, err = stored.Combine(aggregate)
		if err != nil {
			return err
		}
	} else {
		combined = aggregate
	}
	return minerController.evaluateWindow(ctx, state, epoch, combined, asic, settings, now)
}

// advanceEpoch applies AdvanceEpoch and refreshes state from the result. It is the one call site
// every ramp/window-accumulation step (2.8's steps 6-9) goes through, so exactly one transition is
// ever applied per poll for those steps.
func (minerController *controller) advanceEpoch(
	state *lib.MinerState,
	epoch lib.EvidenceEpoch,
	progress lib.EpochProgress,
	now time.Time,
) error {
	result, err := minerController.states.Apply(lib.AdvanceEpoch{Epoch: epoch, Progress: progress}, now)
	if err != nil {
		return err
	}
	*state = result.State
	return nil
}

// pollGapMissed reports how many poll-cycle-sized gaps were skipped since the runtime's last
// observed sample. It only ever feeds EpochProgress.ObserveSample's diagnostic counter; nothing
// reads it to gate a decision.
func pollGapMissed(runtime *minerRuntime, settings lib.Settings, now time.Time) int {
	if runtime.lastSampleAt.IsZero() || settings.MetricsTime <= 0 {
		return 0
	}
	gap := now.Sub(runtime.lastSampleAt)
	if gap <= settings.MetricsTime {
		return 0
	}
	missed := int(gap/settings.MetricsTime) - 1
	if missed < 0 {
		return 0
	}
	return missed
}

func (minerController *controller) observeExternalPoint(
	state *lib.MinerState,
	poll readablePoll,
	settings lib.Settings,
	now time.Time,
) error {
	livePoint := poll.Point()
	asic := poll.ASIC()
	if !validLivePoint(livePoint) || livePoint.Frequency == 50 {
		return fmt.Errorf("%s reported invalid external operating point %d MHz/%d mV", state.Hostname, livePoint.Frequency, livePoint.CoreVoltage)
	}
	if state.Phase == lib.PhaseEmergency || state.Phase == lib.PhaseCooldown ||
		state.SafetyReason != "" || state.PendingKind != "" {
		return nil
	}
	if canonicalASICGrid(asic) != nil || !operatingPointAdvertised(asic, livePoint) {
		if state.ObservedFrequency == livePoint.Frequency && state.ObservedCoreVoltage == livePoint.CoreVoltage {
			state.ObservedCount++
		} else {
			state.ObservedFrequency = livePoint.Frequency
			state.ObservedCoreVoltage = livePoint.CoreVoltage
			state.ObservedCount = 1
		}
		if state.ObservedCount < manualConfirmationPolls {
			return minerController.saveMinerState(state, now)
		}
		// A live point that resolves to off-grid or unadvertised, after two confirming polls, is a
		// real current observation. It remains continuously monitored, but never becomes an
		// automated target while absent from the canonical advertised grid.
		state.SetCurrentPoint(livePoint)
		state.ObservedFrequency = 0
		state.ObservedCoreVoltage = 0
		state.ObservedCount = 0
		state.Phase = lib.PhaseMonitor
		state.MonitorReason = lib.MonitorOffGrid
		state.MonitorReferenceEpochID = 0
		state.SettledAt = time.Time{}
		minerController.resetRuntime(state.MacAddr)
		return minerController.saveMinerState(state, now)
	}
	if state.ObservedFrequency == livePoint.Frequency && state.ObservedCoreVoltage == livePoint.CoreVoltage {
		state.ObservedCount++
	} else {
		state.ObservedFrequency = livePoint.Frequency
		state.ObservedCoreVoltage = livePoint.CoreVoltage
		state.ObservedCount = 1
	}
	if state.ObservedCount < manualConfirmationPolls {
		return minerController.saveMinerState(state, now)
	}
	oldPoint := state.CurrentPoint()
	minerController.resetRuntime(state.MacAddr)
	adoptResult, err := minerController.states.Apply(lib.AdoptManualPoint{
		MacAddr: state.MacAddr, Point: livePoint,
	}, now)
	if err != nil {
		return fmt.Errorf("adopt external operating point for %s: %w", state.Hostname, err)
	}
	*state = adoptResult.State
	if attempts, listErr := minerController.states.ListMutationAttempts(state.MacAddr); listErr == nil {
		if attempt, found := ledgerReconciledAttempt(attempts, livePoint); found {
			minerController.logf(
				"Reconciled operating point for %s from the controller's own ledger (attempt %d, %s): %d/%d -> %d MHz/%d mV",
				state.Hostname, attempt.ID, attempt.FailureStage, oldPoint.Frequency, oldPoint.CoreVoltage, livePoint.Frequency, livePoint.CoreVoltage,
			)
			return nil
		}
	}
	minerController.logf("Adopted external operating point for %s: %d/%d -> %d MHz/%d mV", state.Hostname, oldPoint.Frequency, oldPoint.CoreVoltage, livePoint.Frequency, livePoint.CoreVoltage)
	return nil
}

// ledgerReconciledAttempt reports the most recent failed or superseded mutation attempt whose PATCH
// was verified against the point now observed live. A live point differing from durable current is
// not automatically an external manual change: the live observation is the authority for what the
// device is running, but the ledger supplies only the cause, deciding whether the change is
// controller-owned or foreign. ConfiguredVerifiedAt is never treated as proof of the currently
// running configuration — it is a pre-restart NVS readback, and the AxeOS Mutation Constraint
// forbids reading it that way; only the live poll proves the change held. This changes what a
// confirmed observation is attributed to, not how many observations are needed: the caller's own
// manualConfirmationPolls confirmation count is unchanged either way.
//
// This does NOT, by itself, cover mineira's actual scenario. observeExternalPoint's own guard
// refuses to run at all while SafetyReason is non-empty, and every automated safety-recovery
// completion path (deriveCompletedMutationState, backing MutationSafetyRollback and
// MutationFirmwareRecovery) leaves SafetyReason set while the mutation itself resolves. SafetyReason
// does now clear automatically — controlMinerAfterSafety's COOLDOWN recovery predicate
// (recoveryHealthyPolls) clears it once the device proves itself safe for enough consecutive polls
// — but that happens in COOLDOWN's own handling, upstream of and independent from this lookup;
// observeExternalPoint's guard here is unchanged, and still refuses to run while SafetyReason is
// still non-empty. Bypassing that guard for a ledger-confirmed point was deliberately not done
// here: it would change what SafetyReason means for this specific lookup (from "wait for the
// recovery predicate or an operator" to "clearable early by a verified ledger match alone"), a
// distinct architectural question from the recovery predicate's own, and not a call to make
// unilaterally inside this function. What this lookup does cover, and is tested for, is a live
// point differing from durable current after a non-safety mutation failure (e.g. a verified PATCH
// followed by a failed post-restart health check) — a real, currently-reachable case, just not the
// RFC's headline one.
func ledgerReconciledAttempt(attempts []lib.MutationAttempt, target lib.OperatingPoint) (lib.MutationAttempt, bool) {
	for _, attempt := range attempts {
		if attempt.TargetPoint() == target && !attempt.ConfiguredVerifiedAt.IsZero() && !attempt.FailedAt.IsZero() {
			return attempt, true
		}
	}
	return lib.MutationAttempt{}, false
}

// evaluateWindow dispatches a fully-combined evidence-epoch outcome to the handler matching the
// epoch's purpose. Every branch ends in exactly one transition that closes the epoch (and, where the
// purpose produces a measurement, writes its operating_points record).
func (minerController *controller) evaluateWindow(
	ctx context.Context,
	state *lib.MinerState,
	epoch lib.EvidenceEpoch,
	combined lib.WindowAggregate,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) error {
	point := state.CurrentPoint()
	if failure, failed := windowSafetyFailure(combined, settings); failed {
		return minerController.rollbackForSafety(ctx, state, point, lib.Info{
			MacAddr: state.MacAddr, Hostname: state.Hostname, Frequency: point.Frequency,
			CoreVoltage: point.CoreVoltage, HashRate: combined.MedianHash(),
			ExpectedHashRate: combined.ExpectedHash(), Temp: combined.P95Temp(),
			VRTemp: combined.P95VRTemp(), Power: combined.P95Power(),
		}, asic, settings, now, failure, true)
	}
	switch epoch.Purpose {
	case lib.EpochTrial:
		return minerController.evaluateTrial(ctx, state, epoch, combined, settings, now)
	case lib.EpochSafetyValidation:
		return minerController.finishSafetyValidation(state, epoch, combined, settings, now)
	case lib.EpochMonitor:
		return minerController.evaluateMonitor(ctx, state, epoch, combined, asic, settings, now)
	case lib.EpochBaseline:
		return minerController.evaluateBaseline(ctx, state, epoch, combined, asic, settings, now)
	default:
		return fmt.Errorf("evaluate window: unreachable evidence-epoch purpose %q", epoch.Purpose)
	}
}

func (minerController *controller) evaluateMonitor(
	ctx context.Context,
	state *lib.MinerState,
	epoch lib.EvidenceEpoch,
	combined lib.WindowAggregate,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	progress := epoch.Progress
	if err := progress.CloseWindow(true, combined); err != nil {
		return err
	}
	decision := lib.MonitorContinue
	nextReason := state.MonitorReason
	gridReady := canonicalASICGrid(asic) == nil && operatingPointAdvertised(asic, state.CurrentPoint())
	healthy := qualityHealthy(combined, settings)

	switch state.MonitorReason {
	case lib.MonitorSelected:
		if state.MonitorReferenceEpochID == 0 {
			if !healthy {
				nextReason = lib.MonitorRejected
			}
			break
		}
		reference, err := minerController.monitorReference(*state)
		if err != nil {
			return err
		}
		records, err := minerController.states.ListPoints(state.MacAddr)
		if err != nil {
			return err
		}
		if monitorEvidenceChanged(reference, combined, records, state.CurrentPoint(), asic, settings) {
			decision = lib.MonitorStartPass
		}
	case lib.MonitorManual:
		if !gridReady {
			nextReason = lib.MonitorOffGrid
		} else if healthy {
			decision = lib.MonitorStartPass
		} else {
			nextReason = lib.MonitorRejected
		}
	case lib.MonitorRejected:
		if !gridReady {
			nextReason = lib.MonitorOffGrid
		} else if healthy {
			if state.MonitorReferenceEpochID == 0 {
				decision = lib.MonitorStartPass
			} else {
				reference, err := minerController.monitorReference(*state)
				if err != nil {
					return err
				}
				if !qualityHealthy(reference, settings) || monitorAggregateChanged(reference, combined, settings) {
					decision = lib.MonitorStartPass
				} else {
					// A selected point whose quality temporarily failed can become selected again
					// after a full healthy assessment matches its durable reference. No fresh pass
					// or hardware work is justified when the original optimum has simply recovered.
					nextReason = lib.MonitorSelected
				}
			}
		}
	case lib.MonitorStarved:
		if !gridReady {
			nextReason = lib.MonitorOffGrid
		} else if healthy {
			decision = lib.MonitorStartPass
		} else {
			nextReason = lib.MonitorRejected
		}
	case lib.MonitorOffGrid:
		if gridReady && healthy {
			// The exact live point is now advertised, so beginning a pass does not replace or request
			// the formerly off-grid pair.
			decision = lib.MonitorStartPass
		}
	default:
		return fmt.Errorf("monitor has invalid reason %q", state.MonitorReason)
	}

	result, err := minerController.states.Apply(lib.CompleteMonitor{
		State: *state, Epoch: epoch, Progress: progress, Aggregate: combined,
		Decision: decision, NextReason: nextReason,
	}, now)
	if err != nil {
		return err
	}
	*state = result.State
	minerController.resetRuntime(state.MacAddr)
	return nil
}

func (minerController *controller) monitorReference(state lib.MinerState) (lib.WindowAggregate, error) {
	epoch, err := minerController.states.EvidenceEpochByID(state.MonitorReferenceEpochID)
	if err != nil {
		return lib.WindowAggregate{}, err
	}
	if epoch.MacAddr != state.MacAddr || epoch.Point != state.CurrentPoint() || epoch.ClosedAt.IsZero() ||
		epoch.Progress.ClosedWindows() != epoch.RequiredWindows {
		return lib.WindowAggregate{}, fmt.Errorf("monitor reference epoch %d does not prove a complete current-point assessment", epoch.ID)
	}
	return minerController.states.MonitorReference(epoch.ID)
}

func monitorAggregateChanged(reference, current lib.WindowAggregate, settings lib.Settings) bool {
	if qualityHealthy(reference, settings) != qualityHealthy(current, settings) ||
		hasExplorationHeadroom(reference, settings) != hasExplorationHeadroom(current, settings) {
		return true
	}
	low := reference.MedianHash() * (1 - minimumHashGain)
	high := reference.MedianHash() * (1 + minimumHashGain)
	return current.MedianHash() < low || current.MedianHash() >= high
}

func monitorEvidenceChanged(
	reference, current lib.WindowAggregate,
	records []lib.OperatingPointRecord,
	incumbent lib.OperatingPoint,
	asic lib.ASICSettings,
	settings lib.Settings,
) bool {
	if monitorAggregateChanged(reference, current, settings) {
		return true
	}
	tempDelta := current.P95Temp() - reference.P95Temp()
	vrDelta := current.P95VRTemp() - reference.P95VRTemp()
	powerDelta := current.P95Power() - reference.P95Power()
	for _, record := range records {
		if !safetyPointStatus(record.Status) || !operatingPointAdvertised(asic, record.Point()) ||
			(record.Frequency <= incumbent.Frequency && record.CoreVoltage <= incumbent.CoreVoltage) {
			continue
		}
		projectedTemp := record.P95Temp + tempDelta
		projectedVR := record.P95VRTemp + vrDelta
		projectedPower := record.P95Power + powerDelta
		if projectedTemp < settings.TargetTemp && projectedPower > 0 &&
			projectedPower <= settings.MaxPower-powerHeadroom &&
			(projectedVR <= 0 || projectedVR <= settings.VRTempHigh*vrExplorationFactor) {
			return true
		}
	}
	return false
}

// evaluateBaseline closes a root or recovery baseline and commits its next finite-pass authority in
// one transaction. Existing terminal point rows remain consumed; only a PointEntered baseline row
// is finalized by the new measurement.
func (minerController *controller) evaluateBaseline(
	ctx context.Context,
	state *lib.MinerState,
	epoch lib.EvidenceEpoch,
	combined lib.WindowAggregate,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	point := state.CurrentPoint()
	records, err := minerController.states.ListPoints(state.MacAddr)
	if err != nil {
		return err
	}
	entered, found := findRecord(records, point)
	if !found {
		return fmt.Errorf("baseline point %d/%d has no durable entry", point.Frequency, point.CoreVoltage)
	}
	record := lib.OperatingPointRecord{
		MacAddr: state.MacAddr, Frequency: point.Frequency, CoreVoltage: point.CoreVoltage,
		Status: lib.PointValidated, MedianHash: combined.MedianHash(),
		ExpectedHash: combined.ExpectedHash(), Attainment: combined.Attainment(),
		MeanTemp: combined.MeanTemp(), P95Temp: combined.P95Temp(), P95VRTemp: combined.P95VRTemp(),
		P95Power: combined.P95Power(), ErrorPercent: combined.ErrorPercent(),
		AcceptedDelta: uint64(combined.AcceptedDelta()), RejectedDelta: uint64(combined.RejectedDelta()),
		MeasuredAt: now, EnteredAt: entered.EnteredAt, EntryAttemptID: entered.EntryAttemptID,
		ReferenceHash: entered.ReferenceHash,
	}
	if !qualityHealthy(combined, settings) {
		record.Status = lib.PointUnstable
	}
	prospective := records
	if entered.Status == lib.PointEntered {
		prospective = replacePointRecord(records, record)
	}
	transition := lib.CompleteBaseline{
		State: *state, Record: record, Epoch: epoch, Decision: lib.BaselineReject,
	}
	if record.Status == lib.PointValidated {
		best, selectable := selectBestPoint(prospective, asic, settings)
		frontierValid := true
		if selectable {
			candidate, phase, found, candidateErr := nextCandidate(prospective, point, asic)
			if candidateErr != nil {
				frontierValid = false
			} else if found && (phase == lib.PhaseUndervolt || hasExplorationHeadroom(combined, settings)) {
				transition.Decision = lib.BaselineContinue
				transition.Candidate = candidate
				transition.CandidatePhase = phase
				transition.ReferenceHash = best.MedianHash
			}
		}
		if transition.Decision == lib.BaselineReject && selectable && frontierValid {
			selected, selectedOK := selectFinalPoint(prospective, asic, settings)
			if selectedOK {
				transition.Decision = lib.BaselinePlace
				transition.Selected = selected.Point()
				transition.Best = best.Point()
				transition.BestHashRate = best.MedianHash
			}
		}
	}
	result, err := minerController.states.Apply(transition, now)
	if err != nil {
		return err
	}
	*state = result.State
	minerController.resetRuntime(state.MacAddr)
	return nil
}

func replacePointRecord(records []lib.OperatingPointRecord, replacement lib.OperatingPointRecord) []lib.OperatingPointRecord {
	updated := append([]lib.OperatingPointRecord(nil), records...)
	for index := range updated {
		if updated[index].Point() == replacement.Point() {
			updated[index] = replacement
			return updated
		}
	}
	return append(updated, replacement)
}

// starveBaseline closes an exhausted baseline without manufacturing a measurement and opens
// continuous monitoring at the same point. Two later healthy monitor windows start a fresh
// environmental pass; unusable evidence therefore slows recovery but can never stop observation.
func (minerController *controller) starveBaseline(
	state *lib.MinerState,
	epoch lib.EvidenceEpoch,
	now time.Time,
) error {
	state.SetBestPoint(lib.OperatingPoint{})
	state.BestHashRate = 0
	state.Phase = lib.PhaseMonitor
	state.MonitorReason = lib.MonitorStarved
	state.SettledAt = time.Time{}
	result, err := minerController.states.Apply(lib.CloseEpoch{
		State: *state, Epoch: epoch, Outcome: lib.EpochStarved,
		Successor: &lib.OpenEpoch{State: *state, Purpose: lib.EpochMonitor, Point: epoch.Point, RequiredWindows: 2},
	}, now)
	if err != nil {
		return err
	}
	*state = result.State
	minerController.resetRuntime(state.MacAddr)
	return nil
}

func trialWindowPredicate(
	phase lib.OptimizerPhase,
	window lib.WindowAggregate,
	prior lib.OperatingPointRecord,
	reference float64,
) bool {
	switch phase {
	case lib.PhaseUndervolt:
		return undervoltUseful(window, prior, reference)
	case lib.PhaseFrequencyTest, lib.PhaseVoltageTest:
		return performanceWinner(window, reference)
	default:
		return false
	}
}

// trialWindowAdmissible runs the per-window predicates at step 9 of the epoch lifecycle, before the
// first window is stored durably. A failure ends the trial without waiting for a second window,
// exactly as the pre-epoch code did.
func (minerController *controller) trialWindowAdmissible(
	state *lib.MinerState,
	window lib.WindowAggregate,
	entered, prior lib.OperatingPointRecord,
	settings lib.Settings,
) (lib.PointStatus, bool) {
	if !qualityHealthy(window, settings) {
		return lib.PointUnstable, false
	}
	if !trialWindowPredicate(state.Phase, window, prior, entered.ReferenceHash) {
		return lib.PointNoGain, false
	}
	return "", true
}

// evaluateTrial runs only on the combined aggregate (the per-window predicate already ran, in
// controlMinerAfterSafety, via trialWindowAdmissible) and always ends in exactly one transition.
func (minerController *controller) evaluateTrial(
	ctx context.Context,
	state *lib.MinerState,
	epoch lib.EvidenceEpoch,
	combined lib.WindowAggregate,
	settings lib.Settings,
	now time.Time,
) error {
	records, err := minerController.states.ListPoints(state.MacAddr)
	if err != nil {
		return err
	}
	point := state.CurrentPoint()
	entered, found := findRecord(records, point)
	if !found || entered.EntryAttemptID <= 0 {
		return fmt.Errorf("trial %d/%d has no entry authority", point.Frequency, point.CoreVoltage)
	}
	prior, _ := findRecord(records, state.FallbackPoint())
	if !qualityHealthy(combined, settings) {
		return minerController.finalizeTrial(state, epoch, combined, lib.PointUnstable, lib.TrialReturn, settings, now)
	}
	if !trialWindowPredicate(state.Phase, combined, prior, entered.ReferenceHash) {
		return minerController.finalizeTrial(state, epoch, combined, lib.PointNoGain, lib.TrialReturn, settings, now)
	}
	winner := false
	switch state.Phase {
	case lib.PhaseUndervolt:
		winner = undervoltUseful(combined, prior, entered.ReferenceHash)
	case lib.PhaseFrequencyTest, lib.PhaseVoltageTest:
		winner = performanceWinner(combined, entered.ReferenceHash) &&
			minerController.entryMarginPositive(state, entered, combined.MedianHash(), settings, now)
	}
	if winner {
		return minerController.finalizeTrial(state, epoch, combined, lib.PointValidated, lib.TrialPromote, settings, now)
	}
	return minerController.finalizeTrial(state, epoch, combined, lib.PointNoGain, lib.TrialReturn, settings, now)
}

// finalizeTrial closes the trial epoch, writes its terminal record, and opens any immediate
// BASELINE successor in one store transition.
func (minerController *controller) finalizeTrial(
	state *lib.MinerState,
	epoch lib.EvidenceEpoch,
	aggregate lib.WindowAggregate,
	status lib.PointStatus,
	decision lib.TrialDecision,
	settings lib.Settings,
	now time.Time,
) error {
	point := state.CurrentPoint()
	records, err := minerController.states.ListPoints(state.MacAddr)
	if err != nil {
		return err
	}
	entered, found := findRecord(records, point)
	if !found {
		return fmt.Errorf("trial point is not entered")
	}
	record := lib.OperatingPointRecord{
		MacAddr: state.MacAddr, Frequency: point.Frequency, CoreVoltage: point.CoreVoltage,
		Status: status, MedianHash: aggregate.MedianHash(),
		ExpectedHash: aggregate.ExpectedHash(), Attainment: aggregate.Attainment(),
		MeanTemp: aggregate.MeanTemp(), P95Temp: aggregate.P95Temp(),
		P95VRTemp: aggregate.P95VRTemp(), P95Power: aggregate.P95Power(),
		ErrorPercent: aggregate.ErrorPercent(), AcceptedDelta: uint64(aggregate.AcceptedDelta()),
		RejectedDelta: uint64(aggregate.RejectedDelta()), MeasuredAt: now,
		EnteredAt: entered.EnteredAt, EntryAttemptID: entered.EntryAttemptID,
		ReferenceHash: entered.ReferenceHash,
	}
	if status == lib.PointStarved {
		// Starved carries no measurement (the durable evidence-epoch contract): aggregate here is the
		// last individually-rejected window, not a real measurement of the point, so it must not be
		// persisted as one.
		record.MedianHash, record.ExpectedHash, record.Attainment = 0, 0, 0
		record.MeanTemp, record.P95Temp, record.P95VRTemp, record.P95Power = 0, 0, 0, 0
		record.ErrorPercent = nil
		record.AcceptedDelta, record.RejectedDelta = 0, 0
	}
	result, err := minerController.states.Apply(lib.FinalizeTrial{
		State: *state, Record: record, Decision: decision, Epoch: epoch,
	}, now)
	if err != nil {
		return err
	}
	*state = result.State
	minerController.resetRuntime(state.MacAddr)
	return nil
}

func nextCandidate(
	records []lib.OperatingPointRecord,
	incumbent lib.OperatingPoint,
	asic lib.ASICSettings,
) (lib.OperatingPoint, lib.OptimizerPhase, bool, error) {
	if !operatingPointAdvertised(asic, incumbent) || canonicalASICGrid(asic) != nil {
		return lib.OperatingPoint{}, "", false, fmt.Errorf("incumbent is not on the advertised operating-point grid")
	}
	if lower, ok := nextLowerOption(asic.VoltageOptions, incumbent.CoreVoltage); ok {
		candidate := lib.OperatingPoint{Frequency: incumbent.Frequency, CoreVoltage: lower}
		if _, found := findRecord(records, candidate); !found {
			return candidate, lib.PhaseUndervolt, true, nil
		}
	}
	if higher, ok := nextHigherOption(asic.VoltageOptions, incumbent.CoreVoltage); ok {
		candidate := lib.OperatingPoint{Frequency: incumbent.Frequency, CoreVoltage: higher}
		if _, found := findRecord(records, candidate); !found {
			if prior, found := findRecord(records, incumbent); found && prior.Status == lib.PointValidated {
				return candidate, lib.PhaseVoltageTest, true, nil
			}
		}
	}
	for _, frequency := range asic.FrequencyOptions {
		if frequency <= incumbent.Frequency {
			continue
		}
		candidate, phase, stop, ok := nextSweepCandidate(records, frequency, asic.VoltageOptions)
		if stop {
			break
		}
		if ok {
			return candidate, phase, true, nil
		}
	}
	return lib.OperatingPoint{}, "", false, nil
}

func (minerController *controller) entryMarginPositive(
	state *lib.MinerState,
	entry lib.OperatingPointRecord,
	measuredHash float64,
	settings lib.Settings,
	now time.Time,
) bool {
	if entry.EntryAttemptID <= 0 {
		return true
	}
	if !finite(entry.ReferenceHash) || entry.ReferenceHash <= 0 ||
		!finite(measuredHash) || measuredHash <= 0 {
		return false
	}
	attempts, err := minerController.states.ListMutationAttempts(state.MacAddr)
	if err != nil {
		return false
	}
	var attempt lib.MutationAttempt
	for _, candidate := range attempts {
		if candidate.ID == entry.EntryAttemptID {
			attempt = candidate
			break
		}
	}
	return mutationMarginPositive(measuredHash, entry.ReferenceHash, attempt, settings, now)
}

func mutationMarginPositive(
	measuredHash, referenceHash float64,
	attempt lib.MutationAttempt,
	settings lib.Settings,
	now time.Time,
) bool {
	if !finite(measuredHash) || measuredHash <= 0 || !finite(referenceHash) || referenceHash <= 0 ||
		attempt.ID == 0 || attempt.MiningResumedAt.IsZero() || attempt.PatchRequestedAt.IsZero() {
		return false
	}
	horizon := (168 * time.Hour).Seconds()
	delay := attempt.MiningResumedAt.Sub(attempt.PatchRequestedAt).Seconds() + settings.RampUpTime.Seconds()
	margin := (measuredHash-referenceHash)*horizon - measuredHash*delay
	return measuredHash >= referenceHash*(1+minimumHashGain) && delay >= 0 && delay < horizon && margin > 0 && !now.Before(attempt.MiningResumedAt)
}

func (minerController *controller) requestOperatingPoint(
	ctx context.Context,
	state *lib.MinerState,
	target lib.OperatingPoint,
	phase lib.OptimizerPhase,
	kind lib.MutationKind,
	now time.Time,
) error {
	if !validLivePoint(target) || target.Frequency == 50 {
		return fmt.Errorf("request operating point for %s: invalid target %d MHz/%d mV", state.Hostname, target.Frequency, target.CoreVoltage)
	}
	if kind != lib.MutationOperatingPoint && kind != lib.MutationSafetyRollback && kind != lib.MutationFirmwareRecovery {
		return fmt.Errorf("request operating point for %s: invalid mutation kind %q", state.Hostname, kind)
	}
	state.SetPendingMutation(kind, target, now)
	state.Phase = phase
	state.PhaseStartedAt = now
	state.SettledAt = time.Time{}
	state.ObservedFrequency = 0
	state.ObservedCoreVoltage = 0
	state.ObservedCount = 0
	if phase == lib.PhaseMonitor {
		state.MonitorReason = lib.MonitorSelected
		state.MonitorReferenceEpochID = 0
	} else {
		state.MonitorReason = ""
		state.MonitorReferenceEpochID = 0
	}
	minerController.resetRuntime(state.MacAddr)
	return minerController.saveMinerState(state, now)
}

func (minerController *controller) requestRollback(
	ctx context.Context,
	state *lib.MinerState,
	target lib.OperatingPoint,
	now time.Time,
	reason string,
) error {
	if !validLivePoint(target) || target == state.CurrentPoint() {
		return fmt.Errorf("%s has no valid de-escalating rollback target", state.Hostname)
	}
	minerController.logf("Rolling back %s to %d MHz/%d mV: %s", state.Hostname, target.Frequency, target.CoreVoltage, reason)
	state.SetFallbackPoint(lib.OperatingPoint{})
	return minerController.requestOperatingPoint(ctx, state, target, lib.PhaseBaseline, lib.MutationOperatingPoint, now)
}

func (minerController *controller) rollbackForSafety(
	ctx context.Context,
	state *lib.MinerState,
	failedPoint lib.OperatingPoint,
	info lib.Info,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
	failure safetyFailure,
	recordFailure bool,
) error {
	minerController.resetRuntime(state.MacAddr)
	expected := *state
	var safetyRecord *lib.OperatingPointRecord
	var err error
	if recordFailure && validLivePoint(failedPoint) && failedPoint.Frequency != 50 {
		safetyRecord, err = minerController.safetyPointRecord(state, failedPoint, info, failure, now)
		if err != nil {
			return err
		}
	}
	target, err := minerController.bestRollbackPoint(state, failedPoint, asic, settings)
	if err != nil {
		return err
	}
	if target == failedPoint || !validLivePoint(target) {
		if _, err := transitionEmergencyState(state, info, asic, settings, now,
			safetyAssessment{action: safetyEmergencyHold, failure: failure}); err != nil {
			return err
		}
		result, err := minerController.states.Apply(lib.SafetyTransition{Expected: expected, State: *state, Record: safetyRecord}, now)
		if err != nil {
			return err
		}
		*state = result.State
		return nil
	}
	if state.PendingKind == lib.MutationSafetyRollback && state.PendingPoint() == target && safetyRecord == nil {
		return minerController.saveMinerState(state, now)
	}
	state.SafetyReason = escalateSafetyReason(state.SafetyReason, reasonForSafetyFailure(failure))
	state.SetFallbackPoint(lib.OperatingPoint{})
	state.SetPendingMutation(lib.MutationSafetyRollback, target, now)
	state.Phase = lib.PhaseCooldown
	state.PhaseStartedAt = now
	state.MonitorReason = ""
	state.MonitorReferenceEpochID = 0
	state.SettledAt = time.Time{}
	// This is a new, distinct rollback trip (the pending-target-match early return above already
	// covers "same rollback still pending"), possibly reached while a prior COOLDOWN dwell was mid-
	// count: that count proved recovery from a different failure and must not carry over to this one.
	state.RecoveryHealthyCount = 0
	result, err := minerController.states.Apply(lib.SafetyTransition{Expected: expected, State: *state, Record: safetyRecord}, now)
	if err != nil {
		return err
	}
	*state = result.State
	return nil
}

func (minerController *controller) safetyPointRecord(
	state *lib.MinerState,
	point lib.OperatingPoint,
	info lib.Info,
	failure safetyFailure,
	now time.Time,
) (*lib.OperatingPointRecord, error) {
	records, err := minerController.states.ListPoints(state.MacAddr)
	if err != nil {
		return nil, err
	}
	if existing, found := findRecord(records, point); found {
		if existing.Status != lib.PointEntered || existing.EntryAttemptID <= 0 {
			return nil, nil
		}
		status := lib.PointStatus(failure.status)
		if status != lib.PointThermal && status != lib.PointPower && status != lib.PointVRHot {
			status = lib.PointThermal
		}
		hashRate := info.HashRate
		if !finite(hashRate) || hashRate < 0 {
			hashRate = 0
		}
		expectedHash := info.ExpectedHashRate
		if !finite(expectedHash) || expectedHash < 0 {
			expectedHash = 0
		}
		temp := info.Temp
		if !finite(temp) || temp < 0 {
			temp = 0
		}
		vrTemp := info.VRTemp
		if !finite(vrTemp) || vrTemp < 0 {
			vrTemp = 0
		}
		power := info.Power
		if !finite(power) || power < 0 {
			power = 0
		}
		record := lib.OperatingPointRecord{
			MacAddr: state.MacAddr, Frequency: point.Frequency, CoreVoltage: point.CoreVoltage,
			Status: status, MedianHash: hashRate,
			ExpectedHash: expectedHash, Attainment: 0,
			MeanTemp: temp, P95Temp: temp,
			P95VRTemp: vrTemp, P95Power: power,
			MeasuredAt: now, EnteredAt: existing.EnteredAt,
			EntryAttemptID: existing.EntryAttemptID, ReferenceHash: existing.ReferenceHash,
		}
		if record.ExpectedHash > 0 {
			record.Attainment = record.MedianHash / record.ExpectedHash
		}
		return &record, nil
	}
	return nil, nil
}

func (minerController *controller) bestRollbackPoint(
	state *lib.MinerState,
	failedPoint lib.OperatingPoint,
	asic lib.ASICSettings,
	settings lib.Settings,
) (lib.OperatingPoint, error) {
	records, err := minerController.states.ListPoints(state.MacAddr)
	if err != nil {
		return lib.OperatingPoint{}, err
	}
	if target, found := selectRollbackPoint(records, failedPoint, asic, settings); found {
		return target, nil
	}
	return minimumAdvertisedPoint(asic)
}

// selectRollbackPoint chooses the closest already-validated complete pair
// with full thermal, VR, power, and quality headroom. A point lowering both
// components is preferred whenever one exists; no unmeasured adjacent pair is
// ever promoted into safety authority.
func selectRollbackPoint(
	records []lib.OperatingPointRecord,
	failedPoint lib.OperatingPoint,
	asic lib.ASICSettings,
	settings lib.Settings,
) (lib.OperatingPoint, bool) {
	type candidate struct {
		record     lib.OperatingPointRecord
		steps      int
		lowersBoth bool
	}
	var selected candidate
	found := false
	for _, record := range records {
		if !rollbackRecordEligible(record, failedPoint, asic, settings) {
			continue
		}
		point := record.Point()
		current := candidate{
			record: record,
			steps: optionStepDistance(asic.FrequencyOptions, failedPoint.Frequency, point.Frequency) +
				optionStepDistance(asic.VoltageOptions, failedPoint.CoreVoltage, point.CoreVoltage),
			lowersBoth: point.Frequency < failedPoint.Frequency && point.CoreVoltage < failedPoint.CoreVoltage,
		}
		if !found || rollbackCandidateCloser(current, selected) {
			selected = current
			found = true
		}
	}
	if !found {
		return lib.OperatingPoint{}, false
	}
	return selected.record.Point(), true
}

func optionStepDistance(options []int, from, to int) int {
	fromIndex := sort.Search(len(options), func(index int) bool { return options[index] > from }) - 1
	toIndex := sort.SearchInts(options, to)
	return max(0, fromIndex-toIndex)
}

func rollbackCandidateCloser(left, right struct {
	record     lib.OperatingPointRecord
	steps      int
	lowersBoth bool
}) bool {
	leftPoint := left.record.Point()
	rightPoint := right.record.Point()
	switch {
	case left.lowersBoth != right.lowersBoth:
		return left.lowersBoth
	case left.steps != right.steps:
		return left.steps < right.steps
	case leftPoint.Frequency != rightPoint.Frequency:
		return leftPoint.Frequency > rightPoint.Frequency
	case leftPoint.CoreVoltage != rightPoint.CoreVoltage:
		return leftPoint.CoreVoltage > rightPoint.CoreVoltage
	case left.record.MedianHash != right.record.MedianHash:
		return left.record.MedianHash > right.record.MedianHash
	default:
		return left.record.P95Temp < right.record.P95Temp
	}
}

func (minerController *controller) handleEmergency(
	state *lib.MinerState,
	info lib.Info,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
	assessment safetyAssessment,
) error {
	minerController.resetRuntime(state.MacAddr)
	expected := *state
	point := state.CurrentPoint()
	if !validLivePoint(point) || point.Frequency == 50 {
		point = operatingPointFromInfo(info)
	}
	var safetyRecord *lib.OperatingPointRecord
	var recordErr error
	if validLivePoint(point) && point.Frequency != 50 {
		safetyRecord, recordErr = minerController.safetyPointRecord(state, point, info, assessment.failure, now)
		if recordErr != nil {
			return recordErr
		}
	}
	event, err := transitionEmergencyState(state, info, asic, settings, now, assessment)
	if err != nil {
		return err
	}
	result, err := minerController.states.Apply(lib.SafetyTransition{Expected: expected, State: *state, Record: safetyRecord}, now)
	if err != nil {
		return fmt.Errorf("persist emergency episode for %s: %w", state.Hostname, err)
	}
	*state = result.State
	if event == "started" {
		minerController.logf("EMERGENCY episode started on %s; preserving optimizer history", state.Hostname)
	}
	return nil
}

// transitionEmergencyState computes the emergency-episode consequence of one safety assessment. It
// does not touch a durable cooldown-expiry clock — that column and the wall-clock cooldown
// (overheatCooldown/Settings.OverheatCooldownMins) it fed were both deleted; COOLDOWN's exit is a
// durable count of consecutive healthy polls instead (see recoveryHealthyPolls in
// controlMinerAfterSafety). Stable live firmware evidence decides whether the reduced pair can be
// adopted directly or must be normalized downward onto the advertised grid; SafetyReason retains
// the strongest observed cause but cannot manufacture a redundant same-pair rollback. EmergencyCount still increments here
// — it remains a durable diagnostic — but nothing computes or stores a cooldown duration from it.
func transitionEmergencyState(
	state *lib.MinerState,
	info lib.Info,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
	assessment safetyAssessment,
) (string, error) {
	minimum, err := minimumAdvertisedPoint(asic)
	if err != nil {
		return "", err
	}
	newEpisode := state.Phase != lib.PhaseEmergency
	if newEpisode {
		state.EmergencyCount = incrementEmergencyCount(state.EmergencyCount)
		state.Phase = lib.PhaseEmergency
		state.PhaseStartedAt = now
		state.MonitorReason = ""
		state.MonitorReferenceEpochID = 0
		state.SettledAt = time.Time{}
		state.SetFallbackPoint(lib.OperatingPoint{})
		// A new episode invalidates any COOLDOWN dwell in progress: RecoveryHealthyCount must start
		// this episode's recovery proof from zero, never resume a prior episode's partial count.
		state.RecoveryHealthyCount = 0
	}
	priorReason := state.SafetyReason
	if assessment.failure.reason != "" || assessment.failure.status != "" {
		state.SafetyReason = escalateSafetyReason(state.SafetyReason, reasonForSafetyFailure(assessment.failure))
	}
	live := operatingPointFromInfo(info)
	firmwareActive := info.OverHeatMode != 0 || live.Frequency == 50 || assessment.action == safetyFirmwareRecovery
	recovered := safeToRecover(info, settings)
	setPending := func(kind lib.MutationKind, target lib.OperatingPoint) {
		// Re-observing the same disposition is not a new authority. Keeping PendingSince stable lets
		// the mutation attempt and the pending state remain one identity across ordinary poll ticks.
		if state.PendingKind == kind && state.PendingPoint() == target && priorReason == state.SafetyReason {
			return
		}
		state.SetPendingMutation(kind, target, now)
	}
	confirmLive := func() bool {
		if state.CurrentPoint() == live {
			state.ObservedFrequency = 0
			state.ObservedCoreVoltage = 0
			state.ObservedCount = 0
			return true
		}
		if state.ObservedFrequency == live.Frequency && state.ObservedCoreVoltage == live.CoreVoltage {
			state.ObservedCount++
		} else {
			state.ObservedFrequency = live.Frequency
			state.ObservedCoreVoltage = live.CoreVoltage
			state.ObservedCount = 1
		}
		if state.ObservedCount < manualConfirmationPolls {
			return false
		}
		state.SetCurrentPoint(live)
		state.ObservedFrequency = 0
		state.ObservedCoreVoltage = 0
		state.ObservedCount = 0
		return true
	}
	switch {
	case firmwareActive:
		// AxeOS owns its powered-down cooling loop and paired reduction. Host-side PATCH/restart while
		// that loop is active would race firmware NVS writes, so observation is the only legal action.
		state.ClearPendingMutation()
		state.ObservedFrequency = 0
		state.ObservedCoreVoltage = 0
		state.ObservedCount = 0
	case state.SafetyReason == lib.SafetyReasonFirmwareOverheat:
		if !validLivePoint(live) || !completeSafetyTelemetry(info) || hasPowerFault(info) ||
			assessInstantaneousSafety(info, settings, live, minimum).action != safetyNormal || !confirmLive() {
			state.ClearPendingMutation()
			break
		}
		target, targetErr := firmwareRecoveryTarget(asic, live)
		if targetErr != nil {
			return "", targetErr
		}
		if target == live {
			state.ClearPendingMutation()
			state.Phase = lib.PhaseCooldown
			state.PhaseStartedAt = now
		} else if recovered {
			setPending(lib.MutationFirmwareRecovery, target)
		} else {
			state.ClearPendingMutation()
		}
	case state.SafetyReason == lib.SafetyReasonTelemetryUnavailable && assessment.action == safetyNormal:
		// Verification uncertainty never authorizes containment. Once two complete safe reads agree
		// on an advertised pair, adopt the fact and let the ordinary recovery proof rebaseline it.
		if operatingPointAdvertised(asic, live) && confirmLive() {
			state.ClearPendingMutation()
			state.Phase = lib.PhaseCooldown
			state.PhaseStartedAt = now
		} else {
			state.ClearPendingMutation()
		}
	case live == minimum && !recovered:
		state.ClearPendingMutation()
	case live == minimum && state.CurrentPoint() == minimum:
		state.ClearPendingMutation()
		state.Phase = lib.PhaseCooldown
		state.PhaseStartedAt = now
	case knownFirmwareTripExceeded(info):
		state.ClearPendingMutation()
	case assessment.action == safetyHostContainment || assessment.action == safetyRollback:
		setPending(lib.MutationSafetyRollback, minimum)
	case !recovered:
		state.ClearPendingMutation()
	default:
		setPending(lib.MutationSafetyRollback, minimum)
	}
	return map[bool]string{true: "started", false: ""}[newEpisode], nil
}

// finishSafetyValidation either returns an unhealthy window to COOLDOWN or atomically resumes the
// same finite pass at a fresh baseline. It never creates a terminal safety hold.
func (minerController *controller) finishSafetyValidation(
	state *lib.MinerState,
	epoch lib.EvidenceEpoch,
	window lib.WindowAggregate,
	settings lib.Settings,
	now time.Time,
) error {
	if !qualityHealthy(window, settings) {
		state.MonitorReason = ""
		state.MonitorReferenceEpochID = 0
		state.SettledAt = time.Time{}
		result, err := minerController.states.Apply(lib.CloseEpoch{State: *state, Epoch: epoch, Outcome: lib.EpochRejected}, now)
		if err != nil {
			return err
		}
		*state = result.State
		return nil
	}
	result, err := minerController.states.Apply(lib.ResumePassAfterSafety{State: *state, Epoch: epoch}, now)
	if err != nil {
		return err
	}
	*state = result.State
	minerController.resetRuntime(state.MacAddr)
	return nil
}

// rampSamples is the ramp-completion threshold: consecutive settled samples at the epoch's exact
// operating point since it opened. ceil(RampUpTime / MetricsTime), 6 at defaults.
func rampSamples(settings lib.Settings) int {
	if settings.MetricsTime <= 0 {
		return 1
	}
	return max(1, int(math.Ceil(float64(settings.RampUpTime)/float64(settings.MetricsTime))))
}

// windowMinSamples is the minimum admitted sample count for a closed window: ceil(0.8 *
// targetSampleCount), 24 at defaults. Asserted by the RFC, not derived; see the "windowMinSamples
// and measurement confidence" material uncertainty — it awaits real hardware measurement.
func windowMinSamples(settings lib.Settings) int {
	return max(1, int(math.Ceil(0.8*float64(targetSampleCount(settings)))))
}

// windowMaxSpan is the span backstop that closes a window even when the sample count has not been
// reached: 2 * EvaluationWindowTime, 600s at defaults.
func windowMaxSpan(settings lib.Settings) time.Duration {
	return 2 * settings.EvaluationWindowTime
}

// windowMaxGap is the largest single inter-sample gap a window may contain and still be admitted:
// 3 * MetricsTime, 30s at defaults.
func windowMaxGap(settings lib.Settings) time.Duration {
	return 3 * settings.MetricsTime
}

// closedWindow is the whole sample buffer at closure: a window is no longer exactly
// targetSampleCount samples spaced exactly MetricsTime apart, so its quality is now a property
// checked at admission, not a precondition of collection.
type closedWindow struct {
	samples []telemetrySample
	span    time.Duration
	maxGap  time.Duration
	missed  int
}

// admit reports whether a closed window carries usable evidence: the two-bound admission predicate
// (RFC "Window closure is a predicate over accumulated samples").
func (window closedWindow) admit(settings lib.Settings) (lib.WindowAggregate, bool) {
	if len(window.samples) < windowMinSamples(settings) || window.maxGap > windowMaxGap(settings) {
		return lib.WindowAggregate{}, false
	}
	aggregate, err := summarizeWindow(window.samples)
	if err != nil {
		return lib.WindowAggregate{}, false
	}
	return aggregate, true
}

// addSample appends one readable sample and reports a closed window when either bound is reached.
// Jitter is a data-quality attribute of a window, not a fatal event between two samples: a gap
// advances the diagnostic counters and is remembered for the admission predicate, but it never
// discards the buffer. Only a genuine contradiction (a different point, phase, or a non-advancing
// clock) does that.
func (minerController *controller) addSample(
	poll readablePoll,
	state lib.MinerState,
	settings lib.Settings,
	now time.Time,
) (closedWindow, bool) {
	info := poll.Info()
	runtime := minerController.runtimeFor(state.MacAddr)
	sample := telemetrySample{
		scheduledAt: now, point: state.CurrentPoint(), phase: state.Phase,
		hashRate: info.HashRate, expectedHash: info.ExpectedHashRate,
		temp: info.Temp, vrTemp: info.VRTemp, power: info.Power,
		errorPercent: cloneFloat(info.ErrorPercentage), acceptedShare: info.SharesAccepted,
		rejectedShare: info.SharesRejected,
	}
	if len(runtime.samples) > 0 {
		previous := runtime.samples[len(runtime.samples)-1]
		if !now.After(previous.scheduledAt) || previous.point != sample.point || previous.phase != sample.phase {
			runtime.samples = nil
			runtime.maxGap = 0
			runtime.missed = 0
		}
	}
	if !runtime.lastSampleAt.IsZero() {
		gap := now.Sub(runtime.lastSampleAt)
		if gap > runtime.maxGap {
			runtime.maxGap = gap
		}
		if settings.MetricsTime > 0 && gap > settings.MetricsTime {
			runtime.missed += int(gap/settings.MetricsTime) - 1
		}
	}
	runtime.samples = append(runtime.samples, sample)
	runtime.lastSampleAt = now
	runtime.lastPoint = sample.point
	runtime.lastPhase = sample.phase
	span := now.Sub(runtime.samples[0].scheduledAt)
	if len(runtime.samples) < targetSampleCount(settings) && span < windowMaxSpan(settings) {
		return closedWindow{}, false
	}
	window := closedWindow{samples: runtime.samples, span: span, maxGap: runtime.maxGap, missed: runtime.missed}
	runtime.samples, runtime.maxGap, runtime.missed = nil, 0, 0
	return window, true
}

func (minerController *controller) runtimeFor(macAddr string) *minerRuntime {
	minerController.runtimeMu.Lock()
	defer minerController.runtimeMu.Unlock()
	runtime := minerController.runtimes[macAddr]
	if runtime == nil {
		runtime = &minerRuntime{}
		minerController.runtimes[macAddr] = runtime
	}
	return runtime
}

// resetRuntime clears only the in-memory sample buffer and jitter accounting. It has no authority
// over durable evidence progress: that lives in evidence_epochs, reachable only through
// lib.OptimizerStore.Apply. A caller that used to rely on resetRuntime to "erase" durable evidence
// must instead apply a CloseEpoch/AdvanceEpoch transition.
func (minerController *controller) resetRuntime(macAddr string) {
	minerController.runtimeMu.Lock()
	delete(minerController.runtimes, macAddr)
	minerController.runtimeMu.Unlock()
}

func (minerController *controller) formatWindow(state lib.MinerState, settings lib.Settings, now time.Time) string {
	if state.Phase == lib.PhaseEmergency {
		switch {
		case state.PendingKind == lib.MutationSafetyRollback:
			return fmt.Sprintf("minimum %s %s", formatOperatingPoint(state.PendingPoint()), formatStateAge(state.PendingSince, now))
		case state.PendingKind == lib.MutationFirmwareRecovery:
			return fmt.Sprintf("normalize %s %s", formatOperatingPoint(state.PendingPoint()), formatStateAge(state.PendingSince, now))
		case state.SafetyReason == lib.SafetyReasonTelemetryUnavailable:
			return fmt.Sprintf("verify %s", formatStateAge(state.PhaseStartedAt, now))
		case state.SafetyReason == lib.SafetyReasonFirmwareOverheat:
			return fmt.Sprintf("firmware cool %s", formatStateAge(state.PhaseStartedAt, now))
		default:
			return fmt.Sprintf("contain %s", formatStateAge(state.PhaseStartedAt, now))
		}
	}
	if state.PendingKind != "" {
		if state.PendingKind == lib.MutationSafetyRollback {
			return fmt.Sprintf("backoff %s %s", formatOperatingPoint(state.PendingPoint()), formatStateAge(state.PendingSince, now))
		}
		return fmt.Sprintf("%s %s", formatOperatingPoint(state.PendingPoint()), formatStateAge(state.PendingSince, now))
	}
	if state.MiningPending {
		return "mining"
	}
	prefix := ""
	if state.MonitorReason != "" && state.Phase == lib.PhaseMonitor {
		prefix = string(state.MonitorReason) + " "
	}
	epoch, open, err := minerController.states.OpenEvidenceEpochFor(state.MacAddr)
	if err != nil || !open {
		if prefix != "" {
			return strings.TrimSpace(prefix)
		}
		return fmt.Sprintf("0/%d", targetSampleCount(settings))
	}
	if epoch.Progress.SettledSamples() < rampSamples(settings) &&
		!(epoch.Purpose == lib.EpochMonitor && state.MonitorReferenceEpochID > 0) {
		return fmt.Sprintf("%sramp %d/%d", prefix, epoch.Progress.SettledSamples(), rampSamples(settings))
	}
	minerController.runtimeMu.Lock()
	count := 0
	if runtime := minerController.runtimes[state.MacAddr]; runtime != nil {
		count = len(runtime.samples)
	}
	minerController.runtimeMu.Unlock()
	if epoch.Progress.ClosedWindows() > 0 {
		return fmt.Sprintf("%s%d/%d %d/%d", prefix, epoch.Progress.ClosedWindows(), epoch.RequiredWindows, count, targetSampleCount(settings))
	}
	return fmt.Sprintf("%s%d/%d", prefix, count, targetSampleCount(settings))
}

func formatOperatingPoint(point lib.OperatingPoint) string {
	return fmt.Sprintf("%d/%d", point.Frequency, point.CoreVoltage)
}

func formatStateAge(since, now time.Time) string {
	if since.IsZero() || now.Before(since) {
		return "0s"
	}
	return now.Sub(since).Truncate(time.Second).String()
}

// summarizeWindow builds the closed window's aggregate through NewWindowAggregate, the one
// constructor that can produce a lib.WindowAggregate, so an invalid aggregate cannot exist.
func summarizeWindow(samples []telemetrySample) (lib.WindowAggregate, error) {
	hashes := make([]float64, 0, len(samples))
	expected := make([]float64, 0, len(samples))
	temps := make([]float64, 0, len(samples))
	vrTemps := make([]float64, 0, len(samples))
	powers := make([]float64, 0, len(samples))
	errors := make([]float64, 0, len(samples))
	for _, sample := range samples {
		hashes = append(hashes, sample.hashRate)
		expected = append(expected, sample.expectedHash)
		temps = append(temps, sample.temp)
		vrTemps = append(vrTemps, sample.vrTemp)
		powers = append(powers, sample.power)
		if sample.errorPercent != nil {
			errors = append(errors, *sample.errorPercent)
		}
	}
	medianHash := percentile(hashes, .5)
	expectedHash := percentile(expected, .5)
	meanTemp := mean(temps)
	p95Temp := percentile(temps, .95)
	p95VRTemp := percentile(vrTemps, .95)
	p95Power := percentile(powers, .95)
	attainment := 0.0
	if expectedHash > 0 {
		attainment = medianHash / expectedHash
	}
	var errorPercent *float64
	if len(errors) > 0 {
		value := percentile(errors, .5)
		errorPercent = &value
	}
	var acceptedDelta, rejectedDelta int
	if len(samples) > 1 {
		first, last := samples[0], samples[len(samples)-1]
		if last.acceptedShare >= first.acceptedShare {
			acceptedDelta = int(last.acceptedShare - first.acceptedShare)
		}
		if last.rejectedShare >= first.rejectedShare {
			rejectedDelta = int(last.rejectedShare - first.rejectedShare)
		}
	}
	span := time.Duration(0)
	if len(samples) > 0 {
		span = samples[len(samples)-1].scheduledAt.Sub(samples[0].scheduledAt)
	}
	return lib.NewWindowAggregate(
		len(samples), span, medianHash, expectedHash, attainment, meanTemp, p95Temp, p95VRTemp, p95Power,
		errorPercent, acceptedDelta, rejectedDelta,
	)
}

func percentile(values []float64, fraction float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if fraction == .5 && len(sorted)%2 == 0 {
		middle := len(sorted) / 2
		return (sorted[middle-1] + sorted[middle]) / 2
	}
	index := int(math.Ceil(fraction*float64(len(sorted)))) - 1
	return sorted[max(0, min(index, len(sorted)-1))]
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

func qualityHealthy(window lib.WindowAggregate, settings lib.Settings) bool {
	errorPercent := window.ErrorPercent()
	return finite(window.MedianHash()) && window.MedianHash() > 0 &&
		(errorPercent == nil ||
			(finite(*errorPercent) && *errorPercent >= 0 && *errorPercent <= settings.MaxErrorPercentage))
}

func hasExplorationHeadroom(window lib.WindowAggregate, settings lib.Settings) bool {
	return window.P95Temp() < settings.TargetTemp && window.P95Power() > 0 &&
		window.P95Power() <= settings.MaxPower-powerHeadroom &&
		(window.P95VRTemp() <= 0 || window.P95VRTemp() <= settings.VRTempHigh*vrExplorationFactor)
}

func assessInstantaneousSafety(info lib.Info, settings lib.Settings, livePoint, minimum lib.OperatingPoint) safetyAssessment {
	switch {
	case info.OverHeatMode != 0 || livePoint.Frequency == 50:
		return safetyAssessment{action: safetyFirmwareRecovery, failure: safetyFailure{status: string(lib.PointThermal), reason: "AxeOS firmware overheat state is active"}}
	case knownFirmwareTripExceeded(info):
		return safetyAssessment{action: safetyEmergencyHold, failure: safetyFailure{status: string(lib.PointThermal), reason: "telemetry exceeded a known AxeOS firmware trip boundary"}}
	case info.Temp >= settings.TempCutoff:
		if livePoint == minimum {
			return safetyAssessment{action: safetyEmergencyHold, failure: safetyFailure{status: string(lib.PointThermal), reason: "host cutoff reached at the advertised minimum"}}
		}
		return safetyAssessment{action: safetyHostContainment, failure: safetyFailure{status: string(lib.PointThermal), reason: "host cutoff reached"}}
	case info.Temp > settings.TempLimit:
		return rollbackAssessment(livePoint, minimum, string(lib.PointThermal), "ASIC temperature exceeded the hard limit")
	case info.Power >= settings.MaxPower:
		return rollbackAssessment(livePoint, minimum, string(lib.PointPower), "board power reached the hard limit")
	case info.VRTemp >= settings.VRTempHigh:
		return rollbackAssessment(livePoint, minimum, string(lib.PointVRHot), "VR temperature reached the hard limit")
	case !completeSafetyTelemetry(info) || hasPowerFault(info) || !supportedSafetyIdentity(info):
		return safetyAssessment{action: safetyUnavailable}
	default:
		return safetyAssessment{action: safetyNormal}
	}
}

func rollbackAssessment(livePoint, minimum lib.OperatingPoint, status, reason string) safetyAssessment {
	if livePoint == minimum {
		return safetyAssessment{action: safetyEmergencyHold, failure: safetyFailure{status: status, reason: reason}}
	}
	return safetyAssessment{action: safetyRollback, failure: safetyFailure{status: status, reason: reason}}
}

func windowSafetyFailure(window lib.WindowAggregate, settings lib.Settings) (safetyFailure, bool) {
	switch {
	case window.P95Temp() > settings.TempLimit:
		return safetyFailure{status: string(lib.PointThermal), reason: "window ASIC temperature exceeded the hard limit"}, true
	case window.P95Power() >= settings.MaxPower:
		return safetyFailure{status: string(lib.PointPower), reason: "window power reached the hard limit"}, true
	case window.P95VRTemp() >= settings.VRTempHigh:
		return safetyFailure{status: string(lib.PointVRHot), reason: "window VR temperature reached the hard limit"}, true
	default:
		return safetyFailure{}, false
	}
}

func operatingPointFromInfo(info lib.Info) lib.OperatingPoint {
	return lib.OperatingPoint{Frequency: info.Frequency, CoreVoltage: info.CoreVoltage}
}

func validLivePoint(point lib.OperatingPoint) bool {
	return point.Frequency > 0 && point.Frequency <= 10_000 && point.CoreVoltage >= 500 && point.CoreVoltage <= 2000
}

func operatingPointAdvertised(asic lib.ASICSettings, point lib.OperatingPoint) bool {
	return optionAdvertised(asic.FrequencyOptions, point.Frequency) && optionAdvertised(asic.VoltageOptions, point.CoreVoltage)
}

func optionAdvertised(options []int, target int) bool {
	index := sort.SearchInts(options, target)
	return index < len(options) && options[index] == target
}

func canonicalASICGrid(asic lib.ASICSettings) error {
	return lib.ValidateCanonicalASICGrid(asic)
}

func strictlyDeescalates(target, live lib.OperatingPoint) bool {
	return target != live && target.Frequency <= live.Frequency && target.CoreVoltage <= live.CoreVoltage
}

func rollbackRecordEligible(record lib.OperatingPointRecord, failedPoint lib.OperatingPoint, asic lib.ASICSettings, settings lib.Settings) bool {
	return record.Status == lib.PointValidated && record.EntryAttemptID >= 0 && record.Point() != failedPoint &&
		lib.IsCanonicalOperatingPoint(record.Point()) && operatingPointAdvertised(asic, record.Point()) &&
		strictlyDeescalates(record.Point(), failedPoint) &&
		finite(record.MedianHash) && record.MedianHash > 0 && finite(record.MeanTemp) && record.MeanTemp > 0 &&
		finite(record.P95Temp) && record.P95Temp > 0 && record.P95Temp < settings.TargetTemp &&
		finite(record.P95Power) && record.P95Power > 0 &&
		record.P95Power <= settings.MaxPower-powerHeadroom && record.P95VRTemp > 0 &&
		finite(record.P95VRTemp) && record.P95VRTemp <= settings.VRTempHigh*vrExplorationFactor &&
		(record.ErrorPercent == nil ||
			(finite(*record.ErrorPercent) && *record.ErrorPercent >= 0 && *record.ErrorPercent <= 100))
}

func minimumAdvertisedPoint(asic lib.ASICSettings) (lib.OperatingPoint, error) {
	if len(asic.FrequencyOptions) == 0 || len(asic.VoltageOptions) == 0 {
		return lib.OperatingPoint{}, fmt.Errorf("ASIC advertised no operating-point options")
	}
	return lib.OperatingPoint{Frequency: asic.FrequencyOptions[0], CoreVoltage: asic.VoltageOptions[0]}, nil
}

func nextLowerOption(options []int, current int) (int, bool) {
	index := sort.SearchInts(options, current)
	if index <= 0 || index > len(options) {
		return 0, false
	}
	return options[index-1], true
}

func nextHigherOption(options []int, current int) (int, bool) {
	index := sort.Search(len(options), func(index int) bool { return options[index] > current })
	if index >= len(options) {
		return 0, false
	}
	return options[index], true
}

func findRecord(records []lib.OperatingPointRecord, point lib.OperatingPoint) (lib.OperatingPointRecord, bool) {
	for _, record := range records {
		if record.Point() == point {
			return record, true
		}
	}
	return lib.OperatingPointRecord{}, false
}

func nextSweepCandidate(records []lib.OperatingPointRecord, frequency int, voltages []int) (lib.OperatingPoint, lib.OptimizerPhase, bool, bool) {
	var previous, beforePrevious lib.OperatingPointRecord
	hasPrevious := false
	for index, voltage := range voltages {
		point := lib.OperatingPoint{Frequency: frequency, CoreVoltage: voltage}
		record, found := findRecord(records, point)
		if !found {
			if !hasPrevious {
				phase := lib.PhaseFrequencyTest
				if index > 0 {
					phase = lib.PhaseVoltageTest
				}
				return point, phase, false, true
			}
			if !canAdmitAfterVoltage(previous, beforePrevious, index) {
				return lib.OperatingPoint{}, "", false, false
			}
			phase := lib.PhaseVoltageTest
			if index == 0 {
				phase = lib.PhaseFrequencyTest
			}
			return point, phase, false, true
		}
		if safetyPointStatus(record.Status) || record.Status == lib.PointUnobservable {
			return lib.OperatingPoint{}, "", true, false
		}
		beforePrevious = previous
		previous = record
		hasPrevious = true
	}
	return lib.OperatingPoint{}, "", false, false
}

func canAdmitAfterVoltage(
	previous lib.OperatingPointRecord,
	beforePrevious lib.OperatingPointRecord,
	index int,
) bool {
	if previous.Status == lib.PointUnobservable || safetyPointStatus(previous.Status) {
		return false
	}
	if index == 1 {
		return previous.Status == lib.PointNoGain || previous.Status == lib.PointUnstable
	}
	if previous.Status != lib.PointNoGain && previous.Status != lib.PointUnstable {
		return true
	}
	if beforePrevious.Status == "" {
		return false
	}
	return recordVoltageResponseUseful(previous, beforePrevious)
}

func performanceWinner(window lib.WindowAggregate, reference float64) bool {
	return reference <= 0 || window.MedianHash() >= reference*(1+minimumHashGain)
}

func undervoltUseful(window lib.WindowAggregate, prior lib.OperatingPointRecord, reference float64) bool {
	if reference > 0 && window.MedianHash() <= reference*(1-undervoltHashTolerance) {
		return false
	}
	if prior.P95Temp > 0 && window.P95Temp() >= prior.P95Temp && prior.P95Power > 0 && window.P95Power() >= prior.P95Power && prior.P95VRTemp > 0 && window.P95VRTemp() >= prior.P95VRTemp {
		return false
	}
	return true
}

func recordVoltageResponseUseful(current, previous lib.OperatingPointRecord) bool {
	return previous.MedianHash <= 0 || current.MedianHash >= previous.MedianHash*(1+minimumHashGain) ||
		current.ErrorPercent != nil && previous.ErrorPercent != nil && *previous.ErrorPercent-*current.ErrorPercent >= minimumErrorImprovement
}

func safetyPointStatus(status lib.PointStatus) bool {
	return status == lib.PointThermal || status == lib.PointPower || status == lib.PointVRHot
}

func targetSampleCount(settings lib.Settings) int {
	if settings.MetricsTime <= 0 {
		return 1
	}
	return max(1, int(math.Ceil(float64(settings.EvaluationWindowTime)/float64(settings.MetricsTime))))
}

// logRecoveryInstrumentation logs safeToRecover transitions and temperature slope on every poll
// while a miner is in COOLDOWN or EMERGENCY, purely for operator visibility; it influences no
// decision and duplicates none of controlMinerAfterSafety's own recoveryHealthyPolls bookkeeping —
// that durable counter is the actual recovery predicate, this is only a human-readable log of the
// same safeToRecover signal it consults. It is credential-free: only a hostname, a phase name, a
// temperature, and a boolean ever appear.
func (minerController *controller) logRecoveryInstrumentation(
	state lib.MinerState,
	info lib.Info,
	settings lib.Settings,
	now time.Time,
) {
	if state.Phase != lib.PhaseCooldown && state.Phase != lib.PhaseEmergency {
		return
	}
	if !finitePositive(info.Temp) {
		return
	}
	safe := safeToRecover(info, settings)
	runtime := minerController.runtimeFor(state.MacAddr)
	previous := runtime.recovery
	if previous != nil && previous.safe != safe {
		minerController.logf(
			"recovery instrumentation %s: safeToRecover %t -> %t at %.1fC (phase %s)",
			state.Hostname, previous.safe, safe, info.Temp, state.Phase,
		)
	}
	if previous != nil && now.After(previous.at) {
		if elapsed := now.Sub(previous.at).Seconds(); elapsed > 0 {
			minerController.logf(
				"recovery instrumentation %s: temp %.1fC slope %.4fC/s over %.0fs (phase %s)",
				state.Hostname, info.Temp, (info.Temp-previous.temp)/elapsed, elapsed, state.Phase,
			)
		}
	}
	runtime.recovery = &recoverySample{at: now, temp: info.Temp, safe: safe}
}

func safeToRecover(info lib.Info, settings lib.Settings) bool {
	return supportedSafetyIdentity(info) && completeSafetyTelemetry(info) && info.Temp <= settings.RecoveryTemp &&
		info.Power <= settings.MaxPower-powerHeadroom && info.VRTemp <= settings.VRTempHigh*vrExplorationFactor && !hasPowerFault(info)
}

func supportedSafetyIdentity(info lib.Info) bool {
	return info.Version == supportedAxeOSVersion && info.ASICModel == supportedASICModel && info.BoardVersion == supportedBoardVersion && info.MacAddr != ""
}

func completeSafetyTelemetry(info lib.Info) bool {
	return finitePositive(info.Temp) && finitePositive(info.VRTemp) && finitePositive(info.Power)
}

func finitePositive(value float64) bool { return value > 0 && finite(value) }
func finite(value float64) bool         { return !math.IsNaN(value) && !math.IsInf(value, 0) }

func knownFirmwareTripExceeded(info lib.Info) bool {
	return info.Temp > axeOSASICTripTemp || info.VRTemp > axeOSVRTripTemp
}

func incrementEmergencyCount(count int) int {
	const maximum = 1_000_000
	if count < 0 {
		return 1
	}
	if count >= maximum {
		return maximum
	}
	return count + 1
}

func reasonForSafetyFailure(failure safetyFailure) lib.SafetyReason {
	if strings.Contains(failure.reason, "uncertain") {
		return lib.SafetyReasonTelemetryUnavailable
	}
	if strings.Contains(failure.reason, "firmware overheat") {
		return lib.SafetyReasonFirmwareOverheat
	}
	if strings.Contains(failure.reason, "firmware trip") || strings.Contains(failure.reason, "known AxeOS firmware trip") {
		return lib.SafetyReasonFirmwareTrip
	}
	switch failure.status {
	case string(lib.PointThermal):
		if failure.reason == "host cutoff reached" || failure.reason == "host cutoff reached at the advertised minimum" {
			return lib.SafetyReasonHostCutoff
		}
		return lib.SafetyReasonASICLimit
	case string(lib.PointPower):
		return lib.SafetyReasonPowerLimit
	case string(lib.PointVRHot):
		return lib.SafetyReasonVRLimit
	default:
		return lib.SafetyReasonTelemetryUnavailable
	}
}

func safetyReasonRank(reason lib.SafetyReason) int {
	switch reason {
	case lib.SafetyReasonTelemetryUnavailable:
		return 1
	case lib.SafetyReasonASICLimit, lib.SafetyReasonPowerLimit, lib.SafetyReasonVRLimit:
		return 2
	case lib.SafetyReasonHostCutoff:
		return 3
	case lib.SafetyReasonFirmwareTrip:
		return 4
	case lib.SafetyReasonFirmwareOverheat:
		return 5
	default:
		return 0
	}
}

func escalateSafetyReason(current, observed lib.SafetyReason) lib.SafetyReason {
	if current == "" || safetyReasonRank(observed) > safetyReasonRank(current) {
		return observed
	}
	return current
}

func selectFinalPoint(records []lib.OperatingPointRecord, asic lib.ASICSettings, settings lib.Settings) (lib.OperatingPointRecord, bool) {
	feasible := feasibleFinalPoints(records, asic, settings)
	if len(feasible) == 0 {
		return lib.OperatingPointRecord{}, false
	}
	maximum := feasible[0].MedianHash
	for _, record := range feasible[1:] {
		maximum = max(maximum, record.MedianHash)
	}
	var selected lib.OperatingPointRecord
	found := false
	for _, record := range feasible {
		if record.MedianHash <= maximum*0.98 || record.MedianHash > maximum {
			continue
		}
		if !found || betterTie(record, selected) {
			selected = record
			found = true
		}
	}
	return selected, found
}

func selectBestPoint(records []lib.OperatingPointRecord, asic lib.ASICSettings, settings lib.Settings) (lib.OperatingPointRecord, bool) {
	feasible := feasibleFinalPoints(records, asic, settings)
	if len(feasible) == 0 {
		return lib.OperatingPointRecord{}, false
	}
	maximum := feasible[0].MedianHash
	for _, record := range feasible[1:] {
		maximum = max(maximum, record.MedianHash)
	}
	var selected lib.OperatingPointRecord
	found := false
	for _, record := range feasible {
		if record.MedianHash != maximum {
			continue
		}
		if !found || betterTie(record, selected) {
			selected = record
			found = true
		}
	}
	return selected, found
}

func feasibleFinalPoints(records []lib.OperatingPointRecord, asic lib.ASICSettings, settings lib.Settings) []lib.OperatingPointRecord {
	feasible := make([]lib.OperatingPointRecord, 0, len(records))
	for _, record := range records {
		if record.Status != lib.PointValidated || !lib.IsCanonicalOperatingPoint(record.Point()) ||
			!operatingPointAdvertised(asic, record.Point()) ||
			!finite(record.MedianHash) || record.MedianHash <= 0 ||
			!finite(record.MeanTemp) || record.MeanTemp <= 0 ||
			!finite(record.P95Temp) || record.P95Temp <= 0 || record.P95Temp > settings.TempLimit ||
			!finite(record.P95Power) || record.P95Power <= 0 || record.P95Power >= settings.MaxPower ||
			!finite(record.P95VRTemp) || record.P95VRTemp <= 0 || record.P95VRTemp >= settings.VRTempHigh {
			continue
		}
		if record.ErrorPercent != nil &&
			(!finite(*record.ErrorPercent) || *record.ErrorPercent < 0 || *record.ErrorPercent > settings.MaxErrorPercentage) {
			continue
		}
		feasible = append(feasible, record)
	}
	return feasible
}

func betterTie(left, right lib.OperatingPointRecord) bool {
	for _, comparison := range []func(lib.OperatingPointRecord) float64{
		func(record lib.OperatingPointRecord) float64 { return record.P95Temp },
		func(record lib.OperatingPointRecord) float64 { return record.P95Power },
		func(record lib.OperatingPointRecord) float64 { return record.P95VRTemp },
		func(record lib.OperatingPointRecord) float64 { return float64(record.CoreVoltage) },
		func(record lib.OperatingPointRecord) float64 { return float64(record.Frequency) },
	} {
		lv, rv := comparison(left), comparison(right)
		if lv != rv {
			return lv < rv
		}
	}
	return false
}

func maxTime(left, right time.Time) time.Time {
	if left.After(right) {
		return left
	}
	return right
}

func cloneFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
