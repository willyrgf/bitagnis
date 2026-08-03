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
)

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

type minerRuntime struct {
	samples         []telemetrySample
	firstWindow     *windowSummary
	deferredWindows []windowSummary
	lastSampleAt    time.Time
	lastPoint       lib.OperatingPoint
	lastPhase       lib.OptimizerPhase
	accounting      *accountingSample
}

type accountingSample struct {
	at            time.Time
	point         lib.OperatingPoint
	phase         lib.OptimizerPhase
	referenceHash float64
	hashRate      float64
	validHash     bool
	settled       bool
	state         lib.MinerState
}

type windowSummary struct {
	MedianHash    float64
	ExpectedHash  float64
	Attainment    float64
	MeanTemp      float64
	P95Temp       float64
	P95VRTemp     float64
	P95Power      float64
	ErrorPercent  *float64
	AcceptedDelta uint64
	RejectedDelta uint64
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
	return minerController.controlMinerAfterSafety(ctx, state, info, asic, settings, now, true)
}

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
	if err := canonicalASICGrid(asic); err != nil {
		expected := *state
		firmwareOverheat := info.OverHeatMode != 0 || info.Frequency == 50
		firmwareTrip := knownFirmwareTripExceeded(info)
		safetyOwned := state.SafetyReason != "" || state.Phase == lib.PhaseOverheat ||
			state.HoldReason == lib.HoldSafety ||
			state.PendingKind == lib.MutationSafetyRollback ||
			state.PendingKind == lib.MutationOverheatRecovery
		state.ClearPendingMutation()
		state.SetFallbackPoint(lib.OperatingPoint{})
		state.SettledAt = time.Time{}
		state.RampUntil = time.Time{}
		state.EvidenceDeadlineAt = time.Time{}
		assessment := assessInstantaneousSafety(info, settings, operatingPointFromInfo(info), lib.OperatingPoint{})
		if firmwareOverheat || firmwareTrip {
			if state.Phase != lib.PhaseOverheat {
				state.OverheatCount = incrementOverheatCount(state.OverheatCount)
				state.CooldownUntil = now.Add(overheatCooldown(settings, state.OverheatCount))
				state.PhaseStartedAt = now
			}
			state.Phase = lib.PhaseOverheat
			state.HoldReason = ""
			if firmwareOverheat {
				state.SafetyReason = lib.SafetyReasonFirmwareOverheat
			} else {
				state.SafetyReason = escalateSafetyReason(state.SafetyReason, lib.SafetyReasonFirmwareTrip)
			}
		} else if safetyOwned {
			if state.Phase != lib.PhaseOverheat {
				state.OverheatCount = incrementOverheatCount(state.OverheatCount)
				state.CooldownUntil = now.Add(overheatCooldown(settings, state.OverheatCount))
				state.PhaseStartedAt = now
			}
			state.Phase = lib.PhaseOverheat
			state.HoldReason = ""
			if state.SafetyReason == "" {
				state.SafetyReason = lib.SafetyReasonMutationUncertain
			}
		} else if assessment.action == safetyNormal || assessment.action == safetyUnavailable {
			state.Phase = lib.PhaseHold
			state.HoldReason = lib.HoldBlocked
		} else {
			state.Phase = lib.PhaseOverheat
			state.HoldReason = ""
			state.SafetyReason = lib.SafetyReasonMutationUncertain
			state.OverheatCount = incrementOverheatCount(state.OverheatCount)
			state.CooldownUntil = now.Add(overheatCooldown(settings, state.OverheatCount))
		}
		minerController.resetRuntime(state.MacAddr)
		if attempt, unfinished, attemptErr := minerController.states.UnfinishedMutationAttempt(state.MacAddr); attemptErr != nil {
			return true, fmt.Errorf("unsupported ASIC grid: %w; load mutation authority: %v", err, attemptErr)
		} else if unfinished {
			if supersedeErr := minerController.states.SupersedeMutation(&expected, state, attempt.ID, now); supersedeErr != nil {
				return true, fmt.Errorf("unsupported ASIC grid: %w; supersede mutation: %v", err, supersedeErr)
			}
			return true, nil
		}
		if saveErr := minerController.states.SaveMiner(state); saveErr != nil {
			return true, fmt.Errorf("unsupported ASIC grid: %w; persist block: %v", err, saveErr)
		}
		return true, nil
	}
	minimum, err := minimumAdvertisedPoint(asic)
	if err != nil {
		return true, err
	}
	live := operatingPointFromInfo(info)
	assessment := assessInstantaneousSafety(info, settings, live, minimum)
	if state.Phase == lib.PhaseOverheat ||
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
	if assessment.action == safetyUnavailable {
		minerController.resetRuntime(state.MacAddr)
		return true, nil
	}
	if state.PendingKind != "" || state.MiningPending {
		return true, nil
	}
	return false, nil
}

func (minerController *controller) controlMinerAfterSafety(
	ctx context.Context,
	state *lib.MinerState,
	info lib.Info,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
	allowOptimization bool,
) error {
	if state == nil {
		return fmt.Errorf("control miner after safety: state is nil")
	}
	livePoint := operatingPointFromInfo(info)
	if !validLivePoint(state.CurrentPoint()) {
		if !validLivePoint(livePoint) || livePoint.Frequency == 50 {
			return fmt.Errorf("%s reported invalid normal operating point %d MHz/%d mV", state.Hostname, livePoint.Frequency, livePoint.CoreVoltage)
		}
		state.SetCurrentPoint(livePoint)
		if canonicalASICGrid(asic) != nil || !operatingPointAdvertised(asic, livePoint) {
			state.Phase = lib.PhaseHold
			state.HoldReason = lib.HoldBlocked
			state.SettledAt = time.Time{}
			state.EvidenceDeadlineAt = time.Time{}
			minerController.resetRuntime(state.MacAddr)
			return minerController.states.SaveMiner(state)
		}
		state.Phase = lib.PhaseHold
		state.HoldReason = lib.HoldBlocked
		state.PhaseStartedAt = now
		state.EvidenceDeadlineAt = time.Time{}
		minerController.resetRuntime(state.MacAddr)
		return minerController.states.SaveMiner(state)
	}
	if livePoint != state.CurrentPoint() {
		return minerController.observeExternalPoint(state, livePoint, asic, settings, now)
	}
	if state.ObservedCount != 0 {
		state.ObservedCount = 0
		state.ObservedFrequency = 0
		state.ObservedCoreVoltage = 0
		if err := minerController.states.SaveMiner(state); err != nil {
			return err
		}
	}
	if state.PendingKind != "" || state.MiningPending {
		return nil
	}
	if state.Phase == lib.PhaseOverheat {
		return nil
	}
	if now.Before(state.CooldownUntil) {
		if state.Phase != lib.PhaseCooldown {
			state.Phase = lib.PhaseCooldown
			state.PhaseStartedAt = now
			state.HoldReason = ""
			if err := minerController.states.SaveMiner(state); err != nil {
				return err
			}
		}
		minerController.resetRuntime(state.MacAddr)
		return nil
	}
	if state.Phase == lib.PhaseCooldown {
		if state.RampUntil.IsZero() {
			state.RampUntil = now.Add(settings.RampUpTime)
			deadlineBase := state.RampUntil
			if state.CooldownUntil.After(deadlineBase) {
				deadlineBase = state.CooldownUntil
			}
			state.EvidenceDeadlineAt = deadlineBase.Add(2 * settings.EvaluationWindowTime)
			minerController.resetRuntime(state.MacAddr)
			return minerController.states.SaveMiner(state)
		}
		if state.EvidenceDeadlineAt.IsZero() && !state.RampUntil.IsZero() &&
			!now.Before(state.RampUntil) && state.SettledAt.IsZero() && state.SafetyReason != "" {
			return nil
		}
	}
	if state.Phase == lib.PhaseHold {
		switch state.HoldReason {
		case lib.HoldBlocked, lib.HoldSafety:
			return nil
		case lib.HoldManual:
			if !state.SettledAt.IsZero() || state.EvidenceDeadlineAt.IsZero() {
				return nil
			}
		case lib.HoldOptimized:
			if !state.SettledAt.IsZero() {
				return nil
			}
		default:
			return nil
		}
	}
	if !state.EvidenceDeadlineAt.IsZero() && !now.Before(state.EvidenceDeadlineAt) {
		return minerController.handleEvidenceDeadline(state, settings, now)
	}
	if now.Before(state.RampUntil) {
		return nil
	}
	if info.UpTimeSeconds < 0 {
		return nil
	}
	summary, ready := minerController.addSample(state.MacAddr, info, *state, settings, now)
	if !ready {
		return nil
	}
	runtime := minerController.runtimeFor(state.MacAddr)
	if !allowOptimization {
		if len(runtime.deferredWindows) == 2 {
			// A gate that lasts beyond two complete windows cannot preserve
			// consecutive evidence. Retain the newest window as a new first
			// window and require its successor after the gate opens.
			runtime.deferredWindows = runtime.deferredWindows[1:]
		}
		runtime.deferredWindows = append(runtime.deferredWindows, summary)
		return nil
	}
	if len(runtime.deferredWindows) > 0 {
		deferred := append([]windowSummary(nil), runtime.deferredWindows...)
		runtime.deferredWindows = nil
		for _, window := range deferred {
			if err := minerController.evaluateWindow(ctx, state, window, asic, settings, now); err != nil {
				return err
			}
			if state.PendingKind != "" || state.Phase == lib.PhaseHold || state.Phase == lib.PhaseOverheat {
				return nil
			}
		}
		return minerController.evaluateWindow(ctx, state, summary, asic, settings, now)
	}
	return minerController.evaluateWindow(ctx, state, summary, asic, settings, now)
}

func (minerController *controller) observeExternalPoint(
	state *lib.MinerState,
	livePoint lib.OperatingPoint,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) error {
	if !validLivePoint(livePoint) || livePoint.Frequency == 50 {
		return fmt.Errorf("%s reported invalid external operating point %d MHz/%d mV", state.Hostname, livePoint.Frequency, livePoint.CoreVoltage)
	}
	if state.Phase == lib.PhaseOverheat || state.Phase == lib.PhaseCooldown ||
		state.SafetyReason != "" || state.HoldReason == lib.HoldSafety || state.PendingKind != "" {
		return nil
	}
	if canonicalASICGrid(asic) != nil || !operatingPointAdvertised(asic, livePoint) {
		state.SetCurrentPoint(livePoint)
		state.ObservedFrequency = 0
		state.ObservedCoreVoltage = 0
		state.ObservedCount = 0
		state.Phase = lib.PhaseHold
		state.HoldReason = lib.HoldBlocked
		state.SettledAt = time.Time{}
		state.EvidenceDeadlineAt = time.Time{}
		minerController.resetRuntime(state.MacAddr)
		return minerController.states.SaveMiner(state)
	}
	if state.ObservedFrequency == livePoint.Frequency && state.ObservedCoreVoltage == livePoint.CoreVoltage {
		state.ObservedCount++
	} else {
		state.ObservedFrequency = livePoint.Frequency
		state.ObservedCoreVoltage = livePoint.CoreVoltage
		state.ObservedCount = 1
	}
	if state.ObservedCount < manualConfirmationPolls {
		return minerController.states.SaveMiner(state)
	}
	oldPoint := state.CurrentPoint()
	rampUntil := now.Add(settings.RampUpTime)
	deadline := rampUntil.Add(2 * settings.EvaluationWindowTime)
	minerController.resetRuntime(state.MacAddr)
	if err := minerController.states.AdoptManualPoint(state, livePoint, now, rampUntil, deadline); err != nil {
		return fmt.Errorf("adopt external operating point for %s: %w", state.Hostname, err)
	}
	minerController.logf("Adopted external operating point for %s: %d/%d -> %d MHz/%d mV", state.Hostname, oldPoint.Frequency, oldPoint.CoreVoltage, livePoint.Frequency, livePoint.CoreVoltage)
	return nil
}

func (minerController *controller) evaluateWindow(
	ctx context.Context,
	state *lib.MinerState,
	summary windowSummary,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) error {
	point := state.CurrentPoint()
	if failure, failed := windowSafetyFailure(summary, settings); failed {
		return minerController.rollbackForSafety(ctx, state, point, lib.Info{
			MacAddr: state.MacAddr, Hostname: state.Hostname, Frequency: point.Frequency,
			CoreVoltage: point.CoreVoltage, HashRate: summary.MedianHash,
			ExpectedHashRate: summary.ExpectedHash, Temp: summary.P95Temp,
			VRTemp: summary.P95VRTemp, Power: summary.P95Power,
		}, asic, settings, now, failure, true)
	}
	switch state.Phase {
	case lib.PhaseUndervolt, lib.PhaseFrequencyTest, lib.PhaseVoltageTest:
		return minerController.evaluateTrial(ctx, state, summary, asic, settings, now)
	case lib.PhaseCooldown:
		return minerController.finishSafetyHold(state, summary, settings, now)
	case lib.PhaseHold:
		if state.HoldReason == lib.HoldManual {
			return minerController.finishManualHold(state, summary, settings, now)
		}
		if state.HoldReason == lib.HoldOptimized && state.SettledAt.IsZero() {
			return minerController.finishFinalPlacement(state, summary, settings, now)
		}
		return nil
	case lib.PhaseBaseline:
		return minerController.evaluateBaseline(ctx, state, summary, asic, settings, now)
	default:
		return nil
	}
}

func (minerController *controller) evaluateBaseline(
	ctx context.Context,
	state *lib.MinerState,
	summary windowSummary,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) error {
	runtime := minerController.runtimeFor(state.MacAddr)
	if runtime.firstWindow == nil {
		if !qualityHealthy(summary, settings) {
			point := state.CurrentPoint()
			return minerController.finalizeBaseline(state, baselineRecordFromSummary(
				state, point, summary, lib.PointUnstable, now,
			), true, settings, now)
		}
		copy := summary
		runtime.firstWindow = &copy
		return nil
	}
	combined, err := combineWindowSummaries(*runtime.firstWindow, summary)
	runtime.firstWindow = nil
	if err != nil {
		point := state.CurrentPoint()
		return minerController.finalizeBaseline(state, lib.OperatingPointRecord{
			MacAddr: state.MacAddr, Frequency: point.Frequency, CoreVoltage: point.CoreVoltage,
			Status: lib.PointUnobservable, MedianHash: summary.MedianHash,
			ExpectedHash: summary.ExpectedHash, Attainment: summary.Attainment,
			MeanTemp: summary.MeanTemp, P95Temp: summary.P95Temp,
			P95VRTemp: summary.P95VRTemp, P95Power: summary.P95Power,
			ErrorPercent:  cloneFloat(summary.ErrorPercent),
			AcceptedDelta: summary.AcceptedDelta, RejectedDelta: summary.RejectedDelta,
			MeasuredAt: now, EnteredAt: statePassEntryTime(state, point),
		}, true, settings, now)
	}
	point := state.CurrentPoint()
	if records, listErr := minerController.states.ListPoints(state.MacAddr); listErr != nil {
		return listErr
	} else if baseline, found := findRecord(records, point); found && baseline.Status != lib.PointEntered {
		if !qualityHealthy(combined, settings) {
			state.Phase = lib.PhaseHold
			state.HoldReason = lib.HoldBlocked
			state.SettledAt = time.Time{}
			state.EvidenceDeadlineAt = time.Time{}
			return minerController.states.SaveMiner(state)
		}
		if !hasExplorationHeadroom(combined, settings) {
			return minerController.settleIfExhausted(ctx, state, asic, settings, now)
		}
		return minerController.startNextCandidate(ctx, state, combined, asic, settings, now)
	}
	baselineRecord := lib.OperatingPointRecord{
		MacAddr: state.MacAddr, Frequency: point.Frequency, CoreVoltage: point.CoreVoltage,
		Status: lib.PointValidated, MedianHash: combined.MedianHash,
		ExpectedHash: combined.ExpectedHash, Attainment: combined.Attainment,
		MeanTemp: combined.MeanTemp, P95Temp: combined.P95Temp, P95VRTemp: combined.P95VRTemp,
		P95Power: combined.P95Power, ErrorPercent: cloneFloat(combined.ErrorPercent),
		AcceptedDelta: combined.AcceptedDelta, RejectedDelta: combined.RejectedDelta,
		MeasuredAt: now,
		EnteredAt:  statePassEntryTime(state, point),
	}
	if !qualityHealthy(combined, settings) {
		baselineRecord.Status = lib.PointUnstable
		return minerController.finalizeBaseline(state, baselineRecord, true, settings, now)
	}
	if err := minerController.finalizeBaseline(state, baselineRecord, false, settings, now); err != nil {
		return err
	}
	if !hasExplorationHeadroom(combined, settings) {
		return minerController.settleIfExhausted(ctx, state, asic, settings, now)
	}
	return minerController.startNextCandidate(ctx, state, combined, asic, settings, now)
}

func (minerController *controller) finalizeBaseline(
	state *lib.MinerState,
	record lib.OperatingPointRecord,
	block bool,
	settings lib.Settings,
	now time.Time,
) error {
	if record.EnteredAt.IsZero() {
		record.EnteredAt = statePassEntryTime(state, record.Point())
	}
	if err := minerController.states.FinalizeBaseline(state, record, block, now); err != nil {
		return err
	}
	minerController.resetRuntime(state.MacAddr)
	if block {
		state.HoldReason = lib.HoldBlocked
		state.EvidenceDeadlineAt = time.Time{}
	}
	return nil
}

func baselineRecordFromSummary(
	state *lib.MinerState,
	point lib.OperatingPoint,
	summary windowSummary,
	status string,
	now time.Time,
) lib.OperatingPointRecord {
	return lib.OperatingPointRecord{
		MacAddr: state.MacAddr, Frequency: point.Frequency, CoreVoltage: point.CoreVoltage,
		Status: status, MedianHash: summary.MedianHash, ExpectedHash: summary.ExpectedHash,
		Attainment: summary.Attainment, MeanTemp: summary.MeanTemp, P95Temp: summary.P95Temp,
		P95VRTemp: summary.P95VRTemp, P95Power: summary.P95Power,
		ErrorPercent: cloneFloat(summary.ErrorPercent), AcceptedDelta: summary.AcceptedDelta,
		RejectedDelta: summary.RejectedDelta, MeasuredAt: now,
		EnteredAt: statePassEntryTime(state, point),
	}
}

func trialWindowPredicate(
	phase lib.OptimizerPhase,
	summary windowSummary,
	prior lib.OperatingPointRecord,
	reference float64,
) bool {
	switch phase {
	case lib.PhaseUndervolt:
		return undervoltUseful(summary, prior, reference)
	case lib.PhaseFrequencyTest, lib.PhaseVoltageTest:
		return performanceWinner(summary, reference)
	default:
		return false
	}
}

func (minerController *controller) finishManualHold(
	state *lib.MinerState,
	summary windowSummary,
	settings lib.Settings,
	now time.Time,
) error {
	state.EvidenceDeadlineAt = time.Time{}
	state.SettledAt = time.Time{}
	state.HoldReason = lib.HoldManual
	if qualityHealthy(summary, settings) {
		state.SettledAt = now
	}
	minerController.resetRuntime(state.MacAddr)
	return minerController.states.SaveMiner(state)
}

func (minerController *controller) finishFinalPlacement(
	state *lib.MinerState,
	summary windowSummary,
	settings lib.Settings,
	now time.Time,
) error {
	state.EvidenceDeadlineAt = time.Time{}
	state.SettledAt = time.Time{}
	if qualityHealthy(summary, settings) {
		state.HoldReason = lib.HoldOptimized
		state.Phase = lib.PhaseHold
		state.SettledAt = now
	} else {
		state.HoldReason = lib.HoldBlocked
		state.Phase = lib.PhaseHold
	}
	state.PhaseStartedAt = now
	minerController.resetRuntime(state.MacAddr)
	return minerController.states.SaveMiner(state)
}

func (minerController *controller) evaluateTrial(
	ctx context.Context,
	state *lib.MinerState,
	summary windowSummary,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) error {
	runtime := minerController.runtimeFor(state.MacAddr)
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
	if !qualityHealthy(summary, settings) {
		return minerController.finalizeTrial(state, summary, lib.PointUnstable, lib.TrialReturn, settings, now)
	}
	if !trialWindowPredicate(state.Phase, summary, prior, entered.ReferenceHash) {
		return minerController.finalizeTrial(state, summary, lib.PointNoGain, lib.TrialReturn, settings, now)
	}
	if runtime.firstWindow == nil {
		copy := summary
		runtime.firstWindow = &copy
		return nil
	}
	combined, err := combineWindowSummaries(*runtime.firstWindow, summary)
	runtime.firstWindow = nil
	if err != nil {
		return minerController.finalizeTrial(state, combined, lib.PointUnobservable, lib.TrialReturn, settings, now)
	}
	if !qualityHealthy(combined, settings) {
		return minerController.finalizeTrial(state, combined, lib.PointUnstable, lib.TrialReturn, settings, now)
	}
	if !trialWindowPredicate(state.Phase, combined, prior, entered.ReferenceHash) {
		return minerController.finalizeTrial(state, combined, lib.PointNoGain, lib.TrialReturn, settings, now)
	}
	winner := false
	switch state.Phase {
	case lib.PhaseUndervolt:
		winner = undervoltUseful(combined, prior, entered.ReferenceHash)
	case lib.PhaseFrequencyTest, lib.PhaseVoltageTest:
		winner = performanceWinner(combined, entered.ReferenceHash) &&
			minerController.entryMarginPositive(state, entered, settings, now)
	}
	if winner {
		return minerController.finalizeTrial(state, combined, lib.PointValidated, lib.TrialPromote, settings, now)
	}
	return minerController.finalizeTrial(state, combined, lib.PointNoGain, lib.TrialReturn, settings, now)
}

func (minerController *controller) finalizeTrial(
	state *lib.MinerState,
	summaryRecord windowSummary,
	status string,
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
		Status: status, MedianHash: summaryRecord.MedianHash,
		ExpectedHash: summaryRecord.ExpectedHash, Attainment: summaryRecord.Attainment,
		MeanTemp: summaryRecord.MeanTemp, P95Temp: summaryRecord.P95Temp,
		P95VRTemp: summaryRecord.P95VRTemp, P95Power: summaryRecord.P95Power,
		ErrorPercent: cloneFloat(summaryRecord.ErrorPercent), AcceptedDelta: summaryRecord.AcceptedDelta,
		RejectedDelta: summaryRecord.RejectedDelta, MeasuredAt: now,
		EnteredAt: entered.EnteredAt, EntryAttemptID: entered.EntryAttemptID,
		ReferenceHash: entered.ReferenceHash,
	}
	if err := minerController.states.FinalizeTrial(state, record, decision, now, now.Add(settings.RampUpTime), now.Add(settings.RampUpTime+4*settings.EvaluationWindowTime)); err != nil {
		return err
	}
	minerController.resetRuntime(state.MacAddr)
	return nil
}

func (minerController *controller) startNextCandidate(
	ctx context.Context,
	state *lib.MinerState,
	summary windowSummary,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) error {
	records, err := minerController.states.ListPoints(state.MacAddr)
	if err != nil {
		return err
	}
	incumbent := state.CurrentPoint()
	if !operatingPointAdvertised(asic, incumbent) || canonicalASICGrid(asic) != nil {
		state.Phase = lib.PhaseHold
		state.HoldReason = lib.HoldBlocked
		state.EvidenceDeadlineAt = time.Time{}
		return minerController.states.SaveMiner(state)
	}
	if lower, ok := nextLowerOption(asic.VoltageOptions, incumbent.CoreVoltage); ok {
		candidate := lib.OperatingPoint{Frequency: incumbent.Frequency, CoreVoltage: lower}
		if _, found := findRecord(records, candidate); !found {
			return minerController.admitTrial(ctx, state, candidate, incumbent, lib.PhaseUndervolt, settings, now)
		}
	}
	if higher, ok := nextHigherOption(asic.VoltageOptions, incumbent.CoreVoltage); ok {
		candidate := lib.OperatingPoint{Frequency: incumbent.Frequency, CoreVoltage: higher}
		if _, found := findRecord(records, candidate); !found {
			if prior, found := findRecord(records, incumbent); found && prior.Status == lib.PointValidated {
				return minerController.admitTrial(ctx, state, candidate, incumbent, lib.PhaseVoltageTest, settings, now)
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
			return minerController.admitTrial(ctx, state, candidate, incumbent, phase, settings, now)
		}
	}
	return minerController.settleIfExhausted(ctx, state, asic, settings, now)
}

func (minerController *controller) admitTrial(
	ctx context.Context,
	state *lib.MinerState,
	candidate lib.OperatingPoint,
	incumbent lib.OperatingPoint,
	phase lib.OptimizerPhase,
	settings lib.Settings,
	now time.Time,
) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	// PassReferenceHash is the frozen arm denominator used by reporting. A
	// candidate must instead freeze the current pass maximum, which can rise
	// after an earlier candidate is promoted.
	reference := state.BestHashRate
	if reference <= 0 || !finite(reference) {
		return fmt.Errorf("invalid pass reference hash")
	}
	deadline := now.Add(settings.RampUpTime + 4*settings.EvaluationWindowTime)
	_, err := minerController.states.AdmitTrial(state, candidate, incumbent, phase, reference, now, deadline)
	if err != nil {
		return err
	}
	minerController.resetRuntime(state.MacAddr)
	return nil
}

func (minerController *controller) settleIfExhausted(
	ctx context.Context,
	state *lib.MinerState,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	records, err := minerController.states.ListPoints(state.MacAddr)
	if err != nil {
		return err
	}
	best, ok := selectBestPoint(records, asic, settings)
	if !ok {
		state.Phase = lib.PhaseHold
		state.HoldReason = lib.HoldBlocked
		state.SettledAt = time.Time{}
		state.EvidenceDeadlineAt = time.Time{}
		return minerController.states.SaveMiner(state)
	}
	selected, selectedOK := selectFinalPoint(records, asic, settings)
	if !selectedOK {
		return fmt.Errorf("finite pass has a best point but no final placement")
	}
	state.SetBestPoint(best.Point())
	state.BestHashRate = best.MedianHash
	if state.CurrentPoint() != selected.Point() {
		return minerController.requestOperatingPoint(ctx, state, selected.Point(), lib.PhaseHold, lib.MutationOperatingPoint, now)
	}
	state.Phase = lib.PhaseHold
	state.HoldReason = lib.HoldOptimized
	state.SettledAt = time.Time{}
	state.PhaseStartedAt = now
	state.RampUntil = now
	state.EvidenceDeadlineAt = now.Add(2 * settings.EvaluationWindowTime)
	minerController.resetRuntime(state.MacAddr)
	return minerController.states.SaveMiner(state)
}

func (minerController *controller) entryMarginPositive(
	state *lib.MinerState,
	entry lib.OperatingPointRecord,
	settings lib.Settings,
	now time.Time,
) bool {
	if entry.EntryAttemptID <= 0 || entry.ReferenceHash <= 0 {
		return true
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
	if attempt.ID == 0 || attempt.MiningResumedAt.IsZero() || attempt.PatchRequestedAt.IsZero() {
		return false
	}
	conservative := entry.MedianHash
	horizon := (168 * time.Hour).Seconds()
	delay := attempt.MiningResumedAt.Sub(attempt.PatchRequestedAt).Seconds() + settings.RampUpTime.Seconds()
	margin := (conservative-entry.ReferenceHash)*horizon - conservative*delay
	return conservative >= entry.ReferenceHash*(1+minimumHashGain) && delay >= 0 && delay < horizon && margin > 0 && !now.Before(attempt.MiningResumedAt)
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
	if kind != lib.MutationOperatingPoint && kind != lib.MutationSafetyRollback && kind != lib.MutationOverheatRecovery {
		return fmt.Errorf("request operating point for %s: invalid mutation kind %q", state.Hostname, kind)
	}
	state.SetPendingMutation(kind, target, now)
	state.Phase = phase
	state.PhaseStartedAt = now
	state.RampUntil = time.Time{}
	state.EvidenceDeadlineAt = time.Time{}
	state.SettledAt = time.Time{}
	state.ObservedFrequency = 0
	state.ObservedCoreVoltage = 0
	state.ObservedCount = 0
	if phase == lib.PhaseHold {
		state.HoldReason = lib.HoldOptimized
	} else {
		state.HoldReason = ""
	}
	minerController.resetRuntime(state.MacAddr)
	return minerController.states.SaveMiner(state)
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
			safetyAssessment{action: safetyEmergencyHold, failure: failure}, true); err != nil {
			return err
		}
		return minerController.states.PersistSafetyTransition(&expected, state, safetyRecord, now)
	}
	state.RampUntil = time.Time{}
	if state.PendingKind == lib.MutationSafetyRollback && state.PendingPoint() == target && safetyRecord == nil {
		return minerController.states.SaveMiner(state)
	}
	state.SafetyReason = reasonForSafetyFailure(failure)
	state.SetFallbackPoint(lib.OperatingPoint{})
	state.SetPendingMutation(lib.MutationSafetyRollback, target, now)
	state.Phase = lib.PhaseCooldown
	state.PhaseStartedAt = now
	state.RampUntil = time.Time{}
	state.HoldReason = ""
	state.SettledAt = time.Time{}
	state.EvidenceDeadlineAt = time.Time{}
	return minerController.states.PersistSafetyTransition(&expected, state, safetyRecord, now)
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
		status := failure.status
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
	best := lib.OperatingPoint{}
	bestHash := -1.0
	for _, record := range records {
		if !rollbackRecordEligible(record, failedPoint, asic, settings) {
			continue
		}
		if record.MedianHash > bestHash || (record.MedianHash == bestHash && record.P95Temp < pointTemperature(records, best)) {
			best = record.Point()
			bestHash = record.MedianHash
		}
	}
	if validLivePoint(best) {
		return best, nil
	}
	return minimumAdvertisedPoint(asic)
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
	event, err := transitionEmergencyState(state, info, asic, settings, now, assessment, true)
	if err != nil {
		return err
	}
	if err := minerController.states.PersistSafetyTransition(&expected, state, safetyRecord, now); err != nil {
		return fmt.Errorf("persist overheat episode for %s: %w", state.Hostname, err)
	}
	if event == "started" {
		minerController.logf("OVERHEAT episode started on %s; preserving optimizer history", state.Hostname)
	}
	return nil
}

func transitionEmergencyState(
	state *lib.MinerState,
	info lib.Info,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
	assessment safetyAssessment,
	preserveInFlight bool,
) (string, error) {
	minimum, err := minimumAdvertisedPoint(asic)
	if err != nil {
		return "", err
	}
	newEpisode := state.Phase != lib.PhaseOverheat
	state.RampUntil = time.Time{}
	if newEpisode {
		state.OverheatCount = incrementOverheatCount(state.OverheatCount)
		state.CooldownUntil = now.Add(overheatCooldown(settings, state.OverheatCount))
		state.Phase = lib.PhaseOverheat
		state.PhaseStartedAt = now
		state.RampUntil = time.Time{}
		state.HoldReason = ""
		state.SettledAt = time.Time{}
		state.EvidenceDeadlineAt = time.Time{}
		state.SetFallbackPoint(lib.OperatingPoint{})
	}
	if state.CooldownUntil.IsZero() {
		state.CooldownUntil = now.Add(overheatCooldown(settings, max(state.OverheatCount, 1)))
	}
	reason := reasonForSafetyFailure(assessment.failure)
	state.SafetyReason = escalateSafetyReason(state.SafetyReason, reason)
	live := operatingPointFromInfo(info)
	firmware := info.OverHeatMode != 0 || live.Frequency == 50 || assessment.action == safetyFirmwareRecovery
	trip := knownFirmwareTripExceeded(info) || assessment.action == safetyEmergencyHold && !firmware
	switch {
	case state.SafetyReason == lib.SafetyReasonFirmwareOverheat:
		if safeToRecover(info, settings) {
			state.SetPendingMutation(lib.MutationOverheatRecovery, minimum, now)
		} else {
			state.ClearPendingMutation()
		}
	case state.SafetyReason == lib.SafetyReasonFirmwareTrip:
		if safeToRecover(info, settings) {
			state.SetPendingMutation(lib.MutationSafetyRollback, minimum, now)
		} else {
			state.ClearPendingMutation()
		}
	case state.SafetyReason == lib.SafetyReasonMutationUncertain:
		if safeToRecover(info, settings) {
			state.SetPendingMutation(lib.MutationSafetyRollback, minimum, now)
		} else {
			state.ClearPendingMutation()
		}
	case firmware:
		if safeToRecover(info, settings) {
			state.SetPendingMutation(lib.MutationOverheatRecovery, minimum, now)
		} else {
			state.ClearPendingMutation()
		}
	case trip:
		state.ClearPendingMutation()
	case live == minimum && !safeToRecover(info, settings):
		state.ClearPendingMutation()
	case live == minimum && state.SafetyReason == lib.SafetyReasonFirmwareOverheat:
		state.SetPendingMutation(lib.MutationOverheatRecovery, minimum, now)
	case live == minimum:
		state.ClearPendingMutation()
		state.Phase = lib.PhaseCooldown
		state.PhaseStartedAt = now
		state.RampUntil = time.Time{}
	case live != minimum:
		state.SetPendingMutation(lib.MutationSafetyRollback, minimum, now)
	}
	if preserveInFlight && state.PendingKind == lib.MutationOperatingPoint {
		state.ClearPendingMutation()
	}
	if !newEpisode && state.Phase == lib.PhaseOverheat && state.PendingKind == "" && safeToRecover(info, settings) && state.SafetyReason == "" {
		state.Phase = lib.PhaseCooldown
	}
	return map[bool]string{true: "started", false: ""}[newEpisode], nil
}

func (minerController *controller) finishSafetyHold(
	state *lib.MinerState,
	summary windowSummary,
	settings lib.Settings,
	now time.Time,
) error {
	if !qualityHealthy(summary, settings) {
		state.HoldReason = ""
		state.SettledAt = time.Time{}
		state.EvidenceDeadlineAt = time.Time{}
		return minerController.states.SaveMiner(state)
	}
	state.Phase = lib.PhaseHold
	state.HoldReason = lib.HoldSafety
	state.SettledAt = now
	state.EvidenceDeadlineAt = time.Time{}
	state.PhaseStartedAt = now
	return minerController.states.SaveMiner(state)
}

func (minerController *controller) handleEvidenceDeadline(
	state *lib.MinerState,
	settings lib.Settings,
	now time.Time,
) error {
	records, err := minerController.states.ListPoints(state.MacAddr)
	if err != nil {
		return err
	}
	if state.Phase == lib.PhaseCooldown {
		// Safety validation is allowed to expire into a visibly blocked
		// COOLDOWN. It must not be reinterpreted as ordinary blocked HOLD or
		// reopen normal exploration after the deadline.
		state.SettledAt = time.Time{}
		state.EvidenceDeadlineAt = time.Time{}
		minerController.resetRuntime(state.MacAddr)
		return minerController.states.SaveMiner(state)
	}
	if state.Phase == lib.PhaseHold && state.HoldReason == lib.HoldManual {
		state.SettledAt = time.Time{}
		state.EvidenceDeadlineAt = time.Time{}
		minerController.resetRuntime(state.MacAddr)
		return minerController.states.SaveMiner(state)
	}
	point := state.CurrentPoint()
	if record, found := findRecord(records, point); found && record.Status == lib.PointEntered {
		if record.EntryAttemptID > 0 {
			return minerController.finalizeTrial(state, windowSummary{}, lib.PointUnobservable, lib.TrialReturn, settings, now)
		}
		record.Status = lib.PointUnobservable
		record.MeasuredAt = now
		if record.MeasuredAt.Before(record.EnteredAt) {
			record.MeasuredAt = record.EnteredAt
		}
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
		return minerController.finalizeBaseline(state, record, true, settings, now)
	}
	state.Phase = lib.PhaseHold
	state.HoldReason = lib.HoldBlocked
	state.SettledAt = time.Time{}
	state.EvidenceDeadlineAt = time.Time{}
	minerController.resetRuntime(state.MacAddr)
	return minerController.states.SaveMiner(state)
}

func (minerController *controller) addSample(
	macAddr string,
	info lib.Info,
	state lib.MinerState,
	settings lib.Settings,
	now time.Time,
) (windowSummary, bool) {
	if !finitePositive(info.Temp) || !finitePositive(info.VRTemp) || !finitePositive(info.Power) ||
		!finite(info.HashRate) || info.HashRate < 0 || !finite(info.ExpectedHashRate) || info.ExpectedHashRate < 0 {
		minerController.resetRuntime(macAddr)
		return windowSummary{}, false
	}
	sample := telemetrySample{
		scheduledAt: now, point: state.CurrentPoint(), phase: state.Phase,
		hashRate: info.HashRate, expectedHash: info.ExpectedHashRate,
		temp: info.Temp, vrTemp: info.VRTemp, power: info.Power,
		errorPercent: cloneFloat(info.ErrorPercentage), acceptedShare: info.SharesAccepted,
		rejectedShare: info.SharesRejected,
	}
	runtime := minerController.runtimeFor(macAddr)
	if len(runtime.samples) > 0 {
		previous := runtime.samples[len(runtime.samples)-1]
		if !now.After(previous.scheduledAt) || now.Sub(previous.scheduledAt) > settings.MetricsTime || previous.point != sample.point || previous.phase != sample.phase {
			runtime.samples = nil
			runtime.firstWindow = nil
			runtime.deferredWindows = nil
		}
	}
	if !runtime.lastSampleAt.IsZero() {
		gap := now.Sub(runtime.lastSampleAt)
		if !now.After(runtime.lastSampleAt) || (settings.MetricsTime > 0 && gap > settings.MetricsTime) ||
			runtime.lastPoint != sample.point || runtime.lastPhase != sample.phase {
			runtime.samples = nil
			runtime.firstWindow = nil
			runtime.deferredWindows = nil
		}
	}
	runtime.samples = append(runtime.samples, sample)
	runtime.lastSampleAt = now
	runtime.lastPoint = sample.point
	runtime.lastPhase = sample.phase
	count := targetSampleCount(settings)
	if len(runtime.samples) < count {
		return windowSummary{}, false
	}
	samples := append([]telemetrySample(nil), runtime.samples[:count]...)
	runtime.samples = runtime.samples[count:]
	return summarizeWindow(samples), true
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

func (minerController *controller) resetRuntime(macAddr string) {
	minerController.runtimeMu.Lock()
	delete(minerController.runtimes, macAddr)
	minerController.runtimeMu.Unlock()
}

func (minerController *controller) formatWindow(state lib.MinerState, settings lib.Settings, now time.Time) string {
	if state.Phase == lib.PhaseOverheat {
		switch state.PendingKind {
		case lib.MutationSafetyRollback:
			return fmt.Sprintf("contain %s %s", formatOperatingPoint(state.PendingPoint()), formatStateAge(state.PendingSince, now))
		case lib.MutationOverheatRecovery:
			return fmt.Sprintf("wait cool %s", formatStateAge(state.PhaseStartedAt, now))
		default:
			return fmt.Sprintf("wait cool %s", formatStateAge(state.PhaseStartedAt, now))
		}
	}
	if state.PendingKind != "" {
		return fmt.Sprintf("%s %s", formatOperatingPoint(state.PendingPoint()), formatStateAge(state.PendingSince, now))
	}
	if state.MiningPending {
		return "mining"
	}
	if state.HoldReason != "" && state.Phase == lib.PhaseHold {
		return string(state.HoldReason)
	}
	if now.Before(state.RampUntil) {
		return fmt.Sprintf("ramp %ds", int(math.Ceil(state.RampUntil.Sub(now).Seconds())))
	}
	minerController.runtimeMu.Lock()
	count := 0
	if runtime := minerController.runtimes[state.MacAddr]; runtime != nil {
		count = len(runtime.samples)
	}
	minerController.runtimeMu.Unlock()
	return fmt.Sprintf("%d/%d", count, targetSampleCount(settings))
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

func summarizeWindow(samples []telemetrySample) windowSummary {
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
	summary := windowSummary{
		MedianHash: percentile(hashes, .5), ExpectedHash: percentile(expected, .5),
		MeanTemp: mean(temps), P95Temp: percentile(temps, .95),
		P95VRTemp: percentile(vrTemps, .95), P95Power: percentile(powers, .95),
	}
	if summary.ExpectedHash > 0 {
		summary.Attainment = summary.MedianHash / summary.ExpectedHash
	}
	if len(errors) > 0 {
		value := percentile(errors, .5)
		summary.ErrorPercent = &value
	}
	if len(samples) > 1 {
		first, last := samples[0], samples[len(samples)-1]
		if last.acceptedShare >= first.acceptedShare {
			summary.AcceptedDelta = last.acceptedShare - first.acceptedShare
		}
		if last.rejectedShare >= first.rejectedShare {
			summary.RejectedDelta = last.rejectedShare - first.rejectedShare
		}
	}
	return summary
}

func combineWindowSummaries(first, second windowSummary) (windowSummary, error) {
	combined := windowSummary{
		MedianHash:   min(first.MedianHash, second.MedianHash),
		ExpectedHash: max(first.ExpectedHash, second.ExpectedHash),
		MeanTemp:     max(first.MeanTemp, second.MeanTemp), P95Temp: max(first.P95Temp, second.P95Temp),
		P95VRTemp: max(first.P95VRTemp, second.P95VRTemp), P95Power: max(first.P95Power, second.P95Power),
	}
	if combined.ExpectedHash > 0 {
		combined.Attainment = combined.MedianHash / combined.ExpectedHash
	}
	if first.ErrorPercent == nil && second.ErrorPercent == nil {
		combined.ErrorPercent = nil
	} else if first.ErrorPercent == nil {
		combined.ErrorPercent = cloneFloat(second.ErrorPercent)
	} else if second.ErrorPercent == nil {
		combined.ErrorPercent = cloneFloat(first.ErrorPercent)
	} else {
		value := max(*first.ErrorPercent, *second.ErrorPercent)
		combined.ErrorPercent = &value
	}
	if ^uint64(0)-first.AcceptedDelta < second.AcceptedDelta || ^uint64(0)-first.RejectedDelta < second.RejectedDelta {
		return windowSummary{}, fmt.Errorf("window share delta overflow")
	}
	combined.AcceptedDelta = first.AcceptedDelta + second.AcceptedDelta
	combined.RejectedDelta = first.RejectedDelta + second.RejectedDelta
	if !finite(combined.MedianHash) || !finite(combined.ExpectedHash) || !finite(combined.Attainment) {
		return windowSummary{}, fmt.Errorf("window aggregate is non-finite")
	}
	return combined, nil
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

func qualityHealthy(summary windowSummary, settings lib.Settings) bool {
	return summary.MedianHash > 0 && (summary.ErrorPercent == nil || *summary.ErrorPercent <= settings.MaxErrorPercentage)
}

func hasExplorationHeadroom(summary windowSummary, settings lib.Settings) bool {
	return summary.P95Temp < settings.TargetTemp && summary.P95Power > 0 &&
		summary.P95Power <= settings.MaxPower-powerHeadroom &&
		(summary.P95VRTemp <= 0 || summary.P95VRTemp <= settings.VRTempHigh*vrExplorationFactor)
}

func assessInstantaneousSafety(info lib.Info, settings lib.Settings, livePoint, minimum lib.OperatingPoint) safetyAssessment {
	switch {
	case info.OverHeatMode != 0 || livePoint.Frequency == 50:
		return safetyAssessment{action: safetyFirmwareRecovery, failure: safetyFailure{status: lib.PointThermal, reason: "AxeOS firmware overheat state is active"}}
	case knownFirmwareTripExceeded(info):
		return safetyAssessment{action: safetyEmergencyHold, failure: safetyFailure{status: lib.PointThermal, reason: "telemetry exceeded a known AxeOS firmware trip boundary"}}
	case info.Temp >= settings.TempCutoff:
		if livePoint == minimum {
			return safetyAssessment{action: safetyEmergencyHold, failure: safetyFailure{status: lib.PointThermal, reason: "host cutoff reached at the advertised minimum"}}
		}
		return safetyAssessment{action: safetyHostContainment, failure: safetyFailure{status: lib.PointThermal, reason: "host cutoff reached"}}
	case info.Temp > settings.TempLimit:
		return rollbackAssessment(livePoint, minimum, lib.PointThermal, "ASIC temperature exceeded the hard limit")
	case info.Power >= settings.MaxPower:
		return rollbackAssessment(livePoint, minimum, lib.PointPower, "board power reached the hard limit")
	case info.VRTemp >= settings.VRTempHigh:
		return rollbackAssessment(livePoint, minimum, lib.PointVRHot, "VR temperature reached the hard limit")
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

func windowSafetyFailure(summary windowSummary, settings lib.Settings) (safetyFailure, bool) {
	switch {
	case summary.P95Temp > settings.TempLimit:
		return safetyFailure{status: lib.PointThermal, reason: "window ASIC temperature exceeded the hard limit"}, true
	case summary.P95Power >= settings.MaxPower:
		return safetyFailure{status: lib.PointPower, reason: "window power reached the hard limit"}, true
	case summary.P95VRTemp >= settings.VRTempHigh:
		return safetyFailure{status: lib.PointVRHot, reason: "window VR temperature reached the hard limit"}, true
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
		operatingPointAdvertised(asic, record.Point()) && strictlyDeescalates(record.Point(), failedPoint) &&
		record.P95Temp > 0 && record.P95Temp < settings.TargetTemp && record.P95Power > 0 &&
		record.P95Power <= settings.MaxPower-powerHeadroom && record.P95VRTemp > 0 &&
		record.P95VRTemp <= settings.VRTempHigh*vrExplorationFactor
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

func performanceWinner(summary windowSummary, reference float64) bool {
	return reference <= 0 || summary.MedianHash >= reference*(1+minimumHashGain)
}

func undervoltUseful(summary windowSummary, prior lib.OperatingPointRecord, reference float64) bool {
	if reference > 0 && summary.MedianHash <= reference*(1-undervoltHashTolerance) {
		return false
	}
	if prior.P95Temp > 0 && summary.P95Temp >= prior.P95Temp && prior.P95Power > 0 && summary.P95Power >= prior.P95Power && prior.P95VRTemp > 0 && summary.P95VRTemp >= prior.P95VRTemp {
		return false
	}
	return true
}

func recordVoltageResponseUseful(current, previous lib.OperatingPointRecord) bool {
	return previous.MedianHash <= 0 || current.MedianHash >= previous.MedianHash*(1+minimumHashGain) ||
		current.ErrorPercent != nil && previous.ErrorPercent != nil && *previous.ErrorPercent-*current.ErrorPercent >= minimumErrorImprovement
}

func safetyPointStatus(status string) bool {
	return status == lib.PointThermal || status == lib.PointPower || status == lib.PointVRHot
}

func targetSampleCount(settings lib.Settings) int {
	if settings.MetricsTime <= 0 {
		return 1
	}
	return max(1, int(math.Ceil(float64(settings.EvaluationWindowTime)/float64(settings.MetricsTime))))
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

func incrementOverheatCount(count int) int {
	const maximum = 1_000_000
	if count < 0 {
		return 1
	}
	if count >= maximum {
		return maximum
	}
	return count + 1
}

func overheatCooldown(settings lib.Settings, count int) time.Duration {
	const maximum = 24 * time.Hour
	if count <= 0 || settings.OverheatCooldownMins <= 0 {
		return maximum
	}
	base := time.Duration(settings.OverheatCooldownMins) * time.Minute
	if time.Duration(count) > maximum/base {
		return maximum
	}
	return min(base*time.Duration(count), maximum)
}

func reasonForSafetyFailure(failure safetyFailure) lib.SafetyReason {
	if strings.Contains(failure.reason, "uncertain") {
		return lib.SafetyReasonMutationUncertain
	}
	if strings.Contains(failure.reason, "firmware overheat") {
		return lib.SafetyReasonFirmwareOverheat
	}
	if strings.Contains(failure.reason, "firmware trip") || strings.Contains(failure.reason, "known AxeOS firmware trip") {
		return lib.SafetyReasonFirmwareTrip
	}
	switch failure.status {
	case lib.PointThermal:
		if failure.reason == "host cutoff reached" || failure.reason == "host cutoff reached at the advertised minimum" {
			return lib.SafetyReasonHostCutoff
		}
		return lib.SafetyReasonASICLimit
	case lib.PointPower:
		return lib.SafetyReasonPowerLimit
	case lib.PointVRHot:
		return lib.SafetyReasonVRLimit
	default:
		return lib.SafetyReasonMutationUncertain
	}
}

func safetyReasonRank(reason lib.SafetyReason) int {
	switch reason {
	case lib.SafetyReasonMutationUncertain:
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

func statePassEntryTime(state *lib.MinerState, point lib.OperatingPoint) time.Time {
	return state.PassStartedAt
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
		if record.Status == lib.PointValidated && operatingPointAdvertised(asic, record.Point()) &&
			record.MedianHash > 0 && record.P95Temp <= settings.TempLimit &&
			record.P95Power < settings.MaxPower && record.P95VRTemp < settings.VRTempHigh &&
			qualityHealthy(windowSummary{MedianHash: record.MedianHash, ErrorPercent: record.ErrorPercent}, settings) {
			feasible = append(feasible, record)
		}
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

func pointTemperature(records []lib.OperatingPointRecord, point lib.OperatingPoint) float64 {
	if record, ok := findRecord(records, point); ok {
		return record.P95Temp
	}
	return math.MaxFloat64
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
