package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/willyrgf/bitagnis/lib"
)

const (
	manualConfirmationPolls = 2
	blockedPointRetry       = 2 * time.Hour
	minimumHashGain         = 0.02
	undervoltHashTolerance  = 0.02
	minimumErrorImprovement = 1.0
	powerHeadroom           = 2.0
	vrExplorationFactor     = 0.90
)

type telemetrySample struct {
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
	samples []telemetrySample
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

func (minerController *controller) controlMiner(
	ctx context.Context,
	state *lib.MinerState,
	info lib.Info,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) error {
	handled, err := minerController.enforceMinerSafety(
		ctx,
		state,
		info,
		asic,
		settings,
		now,
	)
	if err != nil || handled {
		return err
	}
	return minerController.controlMinerAfterSafety(
		ctx,
		state,
		info,
		asic,
		settings,
		now,
		true,
	)
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
	livePoint := operatingPointFromInfo(info)

	if info.OverHeatMode != 0 {
		return true, minerController.handleOverheat(ctx, state, info, asic, settings, now)
	}
	if state.OverheatPending {
		return true, minerController.handleOverheat(ctx, state, info, asic, settings, now)
	}

	if failure, failed := instantaneousSafetyFailure(info, settings); failed {
		if state.PendingKind != "" &&
			state.PendingPoint() != livePoint &&
			noMoreAggressive(state.PendingPoint(), livePoint) {
			state.Phase = lib.PhaseCooldown
			state.PhaseStartedAt = now
			return true, minerController.states.SaveMiner(state)
		}
		return true, minerController.rollbackForSafety(
			ctx,
			state,
			livePoint,
			info,
			asic,
			settings,
			now,
			failure,
			true,
		)
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
	livePoint := operatingPointFromInfo(info)
	if !validLivePoint(state.CurrentPoint()) {
		if !validLivePoint(livePoint) {
			return fmt.Errorf(
				"%s reported invalid normal operating point %d MHz/%d mV",
				state.Hostname,
				livePoint.Frequency,
				livePoint.CoreVoltage,
			)
		}
		state.SetCurrentPoint(livePoint)
		state.Phase = lib.PhaseBaseline
		state.PhaseStartedAt = now
		state.RampUntil = now.Add(settings.RampUpTime)
		minerController.resetRuntime(state.MacAddr)
		return minerController.states.SaveMiner(state)
	}

	if livePoint != state.CurrentPoint() {
		return minerController.observeExternalPoint(state, livePoint, settings, now)
	}
	if state.ObservedCount != 0 {
		state.ObservedCount = 0
		state.ObservedFrequency = 0
		state.ObservedCoreVoltage = 0
		if err := minerController.states.SaveMiner(state); err != nil {
			return err
		}
	}

	if now.Before(state.CooldownUntil) {
		if state.Phase != lib.PhaseCooldown {
			state.Phase = lib.PhaseCooldown
			state.PhaseStartedAt = now
			if err := minerController.states.SaveMiner(state); err != nil {
				return err
			}
		}
		return nil
	}
	if state.Phase == lib.PhaseCooldown {
		state.Phase = lib.PhaseBaseline
		state.PhaseStartedAt = now
		state.RampUntil = now.Add(settings.RampUpTime)
		minerController.resetRuntime(state.MacAddr)
		if err := minerController.states.SaveMiner(state); err != nil {
			return err
		}
		return nil
	}

	if !allowOptimization {
		return nil
	}
	if info.UpTimeSeconds < 60 || now.Before(state.RampUntil) {
		return nil
	}
	summary, ready := minerController.addSample(state.MacAddr, info, settings)
	if !ready {
		return nil
	}
	return minerController.evaluateWindow(ctx, state, summary, asic, settings, now)
}

func (minerController *controller) observeExternalPoint(
	state *lib.MinerState,
	livePoint lib.OperatingPoint,
	settings lib.Settings,
	now time.Time,
) error {
	if !validLivePoint(livePoint) {
		return fmt.Errorf(
			"%s reported invalid external operating point %d MHz/%d mV",
			state.Hostname,
			livePoint.Frequency,
			livePoint.CoreVoltage,
		)
	}
	if state.ObservedFrequency == livePoint.Frequency &&
		state.ObservedCoreVoltage == livePoint.CoreVoltage {
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
	state.SetCurrentPoint(livePoint)
	state.SetFallbackPoint(lib.OperatingPoint{})
	state.SetBestPoint(lib.OperatingPoint{})
	state.BestHashRate = 0
	state.ObservedCount = 0
	state.ObservedFrequency = 0
	state.ObservedCoreVoltage = 0
	state.ConsecutiveBadWindows = 0
	state.Phase = lib.PhaseBaseline
	state.PhaseStartedAt = now
	state.RampUntil = now.Add(settings.RampUpTime)
	minerController.resetRuntime(state.MacAddr)
	if err := minerController.states.SaveMiner(state); err != nil {
		return fmt.Errorf("adopt external operating point for %s: %w", state.Hostname, err)
	}
	minerController.logf(
		"Adopted external operating point for %s: %d/%d -> %d MHz/%d mV",
		state.Hostname,
		oldPoint.Frequency,
		oldPoint.CoreVoltage,
		livePoint.Frequency,
		livePoint.CoreVoltage,
	)
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
	if summary.MedianHash < 0 ||
		summary.P95Temp <= 0 ||
		summary.P95Power <= 0 {
		minerController.logWindow(state, point, summary, "telemetry incomplete; holding")
		if state.Phase == lib.PhaseUndervolt ||
			state.Phase == lib.PhaseFrequencyTest ||
			state.Phase == lib.PhaseVoltageTest {
			previous := state.FallbackPoint()
			if validLivePoint(previous) {
				return minerController.requestRollback(
					ctx,
					state,
					previous,
					now,
					"candidate could not be validated because telemetry was incomplete",
				)
			}
		}
		state.Phase = lib.PhaseHold
		state.ConsecutiveBadWindows = 0
		return minerController.states.SaveMiner(state)
	}
	if summaryFailure, failed := windowSafetyFailure(summary, settings); failed {
		if err := minerController.saveWindowRecord(
			state,
			point,
			summary,
			summaryFailure.status,
			now.Add(blockedPointRetry),
			now,
		); err != nil {
			return err
		}
		return minerController.rollbackForSafety(
			ctx,
			state,
			point,
			lib.Info{
				Frequency:   point.Frequency,
				CoreVoltage: point.CoreVoltage,
				HashRate:    summary.MedianHash,
				Power:       summary.P95Power,
				Temp:        summary.P95Temp,
				VRTemp:      summary.P95VRTemp,
			},
			asic,
			settings,
			now,
			summaryFailure,
			false,
		)
	}

	switch state.Phase {
	case lib.PhaseUndervolt, lib.PhaseFrequencyTest, lib.PhaseVoltageTest:
		return minerController.evaluateTrial(ctx, state, summary, asic, settings, now)
	default:
		return minerController.evaluateEstablished(ctx, state, summary, asic, settings, now)
	}
}

func (minerController *controller) evaluateEstablished(
	ctx context.Context,
	state *lib.MinerState,
	summary windowSummary,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) error {
	point := state.CurrentPoint()
	if !operatingPointAdvertised(asic, point) {
		state.Phase = lib.PhaseHold
		state.ConsecutiveBadWindows = 0
		minerController.logf(
			"Holding %s at externally selected off-grid point %d MHz/%d mV",
			state.Hostname,
			point.Frequency,
			point.CoreVoltage,
		)
		return minerController.states.SaveMiner(state)
	}
	stable := qualityHealthy(summary, settings) &&
		(state.BestHashRate <= 0 ||
			state.BestPoint() != point ||
			summary.MedianHash >= state.BestHashRate*(1-undervoltHashTolerance))
	if stable {
		if err := minerController.saveWindowRecord(
			state,
			point,
			summary,
			lib.PointValidated,
			time.Time{},
			now,
		); err != nil {
			return err
		}
		if state.BestPoint() != point || state.BestHashRate <= 0 {
			state.SetBestPoint(point)
			state.BestHashRate = summary.MedianHash
		} else {
			state.BestHashRate = max(state.BestHashRate, summary.MedianHash)
		}
		state.ConsecutiveBadWindows = 0
		state.SetFallbackPoint(lib.OperatingPoint{})
		state.Phase = lib.PhaseHold
		state.PhaseStartedAt = now
		if err := minerController.states.SaveMiner(state); err != nil {
			return err
		}
		minerController.logWindow(state, point, summary, "validated")
		if !hasExplorationHeadroom(summary, settings) {
			return nil
		}
		return minerController.startNextCandidate(ctx, state, summary, asic, now)
	}

	state.ConsecutiveBadWindows++
	if state.ConsecutiveBadWindows < 2 {
		if err := minerController.states.SaveMiner(state); err != nil {
			return err
		}
		minerController.logWindow(state, point, summary, "suspect; waiting for confirmation")
		return nil
	}
	if err := minerController.saveWindowRecord(
		state,
		point,
		summary,
		lib.PointUnstable,
		now.Add(blockedPointRetry),
		now,
	); err != nil {
		return err
	}
	if !hasExplorationHeadroom(summary, settings) {
		state.Phase = lib.PhaseHold
		return minerController.states.SaveMiner(state)
	}
	nextVoltage, ok := nextHigherOption(asic.VoltageOptions, point.CoreVoltage)
	if !ok || !validLivePoint(state.BestPoint()) {
		state.Phase = lib.PhaseHold
		return minerController.states.SaveMiner(state)
	}
	return minerController.requestTrial(
		ctx,
		state,
		lib.OperatingPoint{Frequency: point.Frequency, CoreVoltage: nextVoltage},
		state.BestPoint(),
		lib.PhaseVoltageTest,
		now,
	)
}

func (minerController *controller) evaluateTrial(
	ctx context.Context,
	state *lib.MinerState,
	summary windowSummary,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) error {
	point := state.CurrentPoint()
	previous := state.FallbackPoint()
	healthy := qualityHealthy(summary, settings)

	switch state.Phase {
	case lib.PhaseUndervolt:
		productive := state.BestHashRate <= 0 ||
			summary.MedianHash >= state.BestHashRate*(1-undervoltHashTolerance)
		if healthy && productive {
			return minerController.acceptTrial(ctx, state, summary, asic, settings, now)
		}
		status := lib.PointUnstable
		if healthy {
			status = lib.PointNoGain
		}
		if err := minerController.saveWindowRecord(
			state,
			point,
			summary,
			status,
			now.Add(blockedPointRetry),
			now,
		); err != nil {
			return err
		}
		return minerController.requestRollback(
			ctx,
			state,
			previous,
			now,
			"undervolt did not preserve stable hash rate",
		)

	case lib.PhaseFrequencyTest, lib.PhaseVoltageTest:
		improvesBest := state.BestHashRate <= 0 ||
			summary.MedianHash >= state.BestHashRate*(1+minimumHashGain)
		if healthy && improvesBest {
			return minerController.acceptTrial(ctx, state, summary, asic, settings, now)
		}

		records, err := minerController.states.ListPoints(state.MacAddr)
		if err != nil {
			return err
		}
		status := lib.PointUnstable
		if healthy {
			status = lib.PointNoGain
		}
		if err := minerController.saveWindowRecord(
			state,
			point,
			summary,
			status,
			now.Add(blockedPointRetry),
			now,
		); err != nil {
			return err
		}
		if !hasExplorationHeadroom(summary, settings) {
			return minerController.requestRollback(
				ctx,
				state,
				previous,
				now,
				"higher frequency produced no useful safe hash gain",
			)
		}

		nextVoltage, ok := nextHigherOption(asic.VoltageOptions, point.CoreVoltage)
		continueSweep := state.Phase == lib.PhaseFrequencyTest
		if state.Phase == lib.PhaseVoltageTest {
			if prior, found := priorVoltageRecord(records, point); found {
				continueSweep = voltageResponseUseful(summary, prior)
			}
		}
		if !ok || !continueSweep {
			return minerController.requestRollback(
				ctx,
				state,
				previous,
				now,
				"voltage sweep produced no useful actual-hash response",
			)
		}
		return minerController.requestTrial(
			ctx,
			state,
			lib.OperatingPoint{Frequency: point.Frequency, CoreVoltage: nextVoltage},
			previous,
			lib.PhaseVoltageTest,
			now,
		)
	default:
		return fmt.Errorf("evaluate trial: unsupported phase %q", state.Phase)
	}
}

func (minerController *controller) acceptTrial(
	ctx context.Context,
	state *lib.MinerState,
	summary windowSummary,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) error {
	point := state.CurrentPoint()
	if err := minerController.saveWindowRecord(
		state,
		point,
		summary,
		lib.PointValidated,
		time.Time{},
		now,
	); err != nil {
		return err
	}
	state.SetBestPoint(point)
	state.BestHashRate = summary.MedianHash
	state.SetFallbackPoint(lib.OperatingPoint{})
	state.ConsecutiveBadWindows = 0
	state.Phase = lib.PhaseHold
	state.PhaseStartedAt = now
	if err := minerController.states.SaveMiner(state); err != nil {
		return err
	}
	minerController.logWindow(state, point, summary, "trial accepted")
	if !hasExplorationHeadroom(summary, settings) {
		return nil
	}
	return minerController.startNextCandidate(ctx, state, summary, asic, now)
}

func (minerController *controller) startNextCandidate(
	ctx context.Context,
	state *lib.MinerState,
	summary windowSummary,
	asic lib.ASICSettings,
	now time.Time,
) error {
	records, err := minerController.states.ListPoints(state.MacAddr)
	if err != nil {
		return err
	}
	current := state.BestPoint()
	if !operatingPointAdvertised(asic, current) {
		state.Phase = lib.PhaseHold
		state.PhaseStartedAt = now
		return minerController.states.SaveMiner(state)
	}

	if lowerVoltage, ok := nextLowerOption(asic.VoltageOptions, current.CoreVoltage); ok {
		candidate := lib.OperatingPoint{
			Frequency:   current.Frequency,
			CoreVoltage: lowerVoltage,
		}
		record, found := findRecord(records, candidate)
		switch {
		case !found || (!record.RetryAfter.IsZero() && !now.Before(record.RetryAfter)):
			return minerController.requestTrial(
				ctx,
				state,
				candidate,
				current,
				lib.PhaseUndervolt,
				now,
			)
		}
	}

	if higherVoltage, ok := nextHigherOption(asic.VoltageOptions, current.CoreVoltage); ok {
		candidate := lib.OperatingPoint{
			Frequency:   current.Frequency,
			CoreVoltage: higherVoltage,
		}
		if record, found := findRecord(records, candidate); candidateDue(record, found, now) {
			return minerController.requestTrial(
				ctx,
				state,
				candidate,
				current,
				lib.PhaseVoltageTest,
				now,
			)
		}
	}

	for _, frequency := range asic.FrequencyOptions {
		if frequency <= current.Frequency {
			continue
		}
		candidate, phase, stop, ok := nextSweepCandidate(
			records,
			frequency,
			asic.VoltageOptions,
			now,
		)
		if stop {
			break
		}
		if !ok {
			continue
		}
		return minerController.requestTrial(
			ctx,
			state,
			candidate,
			current,
			phase,
			now,
		)
	}

	state.Phase = lib.PhaseHold
	state.PhaseStartedAt = now
	minerController.logWindow(state, current, summary, "highest available safe frontier reached")
	return minerController.states.SaveMiner(state)
}

func (minerController *controller) requestTrial(
	ctx context.Context,
	state *lib.MinerState,
	candidate lib.OperatingPoint,
	previous lib.OperatingPoint,
	phase lib.OptimizerPhase,
	now time.Time,
) error {
	state.SetFallbackPoint(previous)
	state.ConsecutiveBadWindows = 0
	return minerController.requestOperatingPoint(ctx, state, candidate, phase, false, now)
}

func (minerController *controller) requestRollback(
	ctx context.Context,
	state *lib.MinerState,
	target lib.OperatingPoint,
	now time.Time,
	reason string,
) error {
	if !validLivePoint(target) {
		return fmt.Errorf("%s has no valid point to roll back to", state.Hostname)
	}
	minerController.logf(
		"Rolling back %s to %d MHz/%d mV: %s",
		state.Hostname,
		target.Frequency,
		target.CoreVoltage,
		reason,
	)
	state.SetFallbackPoint(lib.OperatingPoint{})
	return minerController.requestOperatingPoint(
		ctx,
		state,
		target,
		lib.PhaseBaseline,
		false,
		now,
	)
}

func (minerController *controller) requestOperatingPoint(
	_ context.Context,
	state *lib.MinerState,
	target lib.OperatingPoint,
	phase lib.OptimizerPhase,
	recovery bool,
	now time.Time,
) error {
	if !validLivePoint(target) {
		return fmt.Errorf(
			"request operating point for %s: invalid target %d MHz/%d mV",
			state.Hostname,
			target.Frequency,
			target.CoreVoltage,
		)
	}
	minerController.asicMu.Lock()
	asic, hasASIC := minerController.asicCache[state.MacAddr]
	minerController.asicMu.Unlock()
	if hasASIC && !operatingPointAdvertised(asic, target) {
		return fmt.Errorf(
			"request operating point for %s: target %d MHz/%d mV is not advertised by AxeOS",
			state.Hostname,
			target.Frequency,
			target.CoreVoltage,
		)
	}
	kind := lib.MutationOperatingPoint
	if recovery {
		kind = lib.MutationOverheatRecovery
	}
	state.SetPendingMutation(kind, target, now)
	state.Phase = phase
	state.PhaseStartedAt = now
	state.RampUntil = time.Time{}
	state.ObservedCount = 0
	state.ObservedFrequency = 0
	state.ObservedCoreVoltage = 0
	if err := minerController.states.SaveMiner(state); err != nil {
		return fmt.Errorf("persist %s operating-point request: %w", state.Hostname, err)
	}

	minerController.resetRuntime(state.MacAddr)
	minerController.logf(
		"Operating point queued for %s: %d MHz/%d mV (%s)",
		state.Hostname,
		target.Frequency,
		target.CoreVoltage,
		phase,
	)
	return nil
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
	if recordFailure && validLivePoint(failedPoint) {
		record := lib.OperatingPointRecord{
			MacAddr:      state.MacAddr,
			Frequency:    failedPoint.Frequency,
			CoreVoltage:  failedPoint.CoreVoltage,
			Status:       failure.status,
			MedianHash:   max(info.HashRate, 0),
			ExpectedHash: max(info.ExpectedHashRate, 0),
			MeanTemp:     max(info.Temp, 0),
			P95Temp:      max(info.Temp, 0),
			P95VRTemp:    max(info.VRTemp, 0),
			P95Power:     max(info.Power, 0),
			MeasuredAt:   now,
			RetryAfter:   now.Add(blockedPointRetry),
		}
		if record.ExpectedHash > 0 {
			record.Attainment = record.MedianHash / record.ExpectedHash
		}
		if err := minerController.states.SavePoint(&record); err != nil {
			return err
		}
	}
	target, err := minerController.bestRollbackPoint(state, failedPoint, asic, settings)
	if err != nil {
		return err
	}
	if target == failedPoint {
		state.Phase = lib.PhaseHold
		return minerController.states.SaveMiner(state)
	}
	minerController.logf(
		"Safety rollback for %s: %s; %d MHz/%d mV -> %d MHz/%d mV",
		state.Hostname,
		failure.reason,
		failedPoint.Frequency,
		failedPoint.CoreVoltage,
		target.Frequency,
		target.CoreVoltage,
	)
	state.SetFallbackPoint(lib.OperatingPoint{})
	return minerController.requestOperatingPoint(
		ctx,
		state,
		target,
		lib.PhaseCooldown,
		false,
		now,
	)
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
		if record.Status != lib.PointValidated {
			continue
		}
		point := record.Point()
		if point == failedPoint ||
			!operatingPointAdvertised(asic, point) ||
			record.P95Temp >= settings.TargetTemp ||
			record.P95Power > settings.MaxPower-powerHeadroom ||
			(record.P95VRTemp > 0 &&
				record.P95VRTemp > settings.VRTempHigh*vrExplorationFactor) {
			continue
		}
		if record.MedianHash > bestHash {
			best = point
			bestHash = record.MedianHash
		}
	}
	if validLivePoint(best) {
		return best, nil
	}
	return minimumAdvertisedPoint(asic)
}

func (minerController *controller) handleOverheat(
	_ context.Context,
	state *lib.MinerState,
	info lib.Info,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) error {
	minerController.resetRuntime(state.MacAddr)
	if !state.OverheatPending {
		state.OverheatPending = true
		state.OverheatCount = incrementOverheatCount(state.OverheatCount)
		state.CooldownUntil = now.Add(overheatCooldown(settings, state.OverheatCount))
		state.Phase = lib.PhaseOverheat
		state.PhaseStartedAt = now
		target, err := minimumAdvertisedPoint(asic)
		if err != nil {
			return err
		}
		state.SetPendingMutation(lib.MutationOverheatRecovery, target, now)
		state.SetFallbackPoint(lib.OperatingPoint{})
		if err := minerController.states.SaveMiner(state); err != nil {
			return fmt.Errorf("record overheat for %s: %w", state.Hostname, err)
		}
		minerController.logf(
			"OVERHEAT detected on %s; preserving optimizer history and waiting to recover",
			state.Hostname,
		)
	} else if state.CooldownUntil.IsZero() {
		state.CooldownUntil = now.Add(overheatCooldown(settings, max(state.OverheatCount, 1)))
		if err := minerController.states.SaveMiner(state); err != nil {
			return err
		}
	}
	if state.PendingKind != lib.MutationOverheatRecovery {
		target, err := minimumAdvertisedPoint(asic)
		if err != nil {
			return err
		}
		state.SetPendingMutation(lib.MutationOverheatRecovery, target, now)
		state.SetFallbackPoint(lib.OperatingPoint{})
		if err := minerController.states.SaveMiner(state); err != nil {
			return fmt.Errorf("replace %s mutation with overheat recovery: %w", state.Hostname, err)
		}
	}
	if !safeToRecover(info, settings) {
		return nil
	}
	return nil
}

func (minerController *controller) addSample(
	macAddr string,
	info lib.Info,
	settings lib.Settings,
) (windowSummary, bool) {
	sample := telemetrySample{
		hashRate:      info.HashRate,
		expectedHash:  info.ExpectedHashRate,
		temp:          info.Temp,
		vrTemp:        info.VRTemp,
		power:         info.Power,
		errorPercent:  cloneFloat(info.ErrorPercentage),
		acceptedShare: info.SharesAccepted,
		rejectedShare: info.SharesRejected,
	}
	target := targetSampleCount(settings)

	minerController.runtimeMu.Lock()
	runtime := minerController.runtimes[macAddr]
	if runtime == nil {
		runtime = &minerRuntime{}
		minerController.runtimes[macAddr] = runtime
	}
	runtime.samples = append(runtime.samples, sample)
	if len(runtime.samples) < target {
		minerController.runtimeMu.Unlock()
		return windowSummary{}, false
	}
	samples := append([]telemetrySample(nil), runtime.samples[:target]...)
	runtime.samples = runtime.samples[:0]
	minerController.runtimeMu.Unlock()
	return summarizeWindow(samples), true
}

func (minerController *controller) resetRuntime(macAddr string) {
	minerController.runtimeMu.Lock()
	delete(minerController.runtimes, macAddr)
	minerController.runtimeMu.Unlock()
}

func (minerController *controller) formatWindow(
	state lib.MinerState,
	settings lib.Settings,
	now time.Time,
) string {
	if state.PendingKind != "" || state.MiningPending {
		return "apply"
	}
	if now.Before(state.RampUntil) {
		remaining := state.RampUntil.Sub(now)
		if remaining < 0 {
			remaining = 0
		}
		return fmt.Sprintf("ramp %ds", int(math.Ceil(remaining.Seconds())))
	}
	minerController.runtimeMu.Lock()
	count := 0
	if runtime := minerController.runtimes[state.MacAddr]; runtime != nil {
		count = len(runtime.samples)
	}
	minerController.runtimeMu.Unlock()
	return fmt.Sprintf("%d/%d", count, targetSampleCount(settings))
}

func (minerController *controller) saveWindowRecord(
	state *lib.MinerState,
	point lib.OperatingPoint,
	summary windowSummary,
	status string,
	retryAfter time.Time,
	now time.Time,
) error {
	record := lib.OperatingPointRecord{
		MacAddr:       state.MacAddr,
		Frequency:     point.Frequency,
		CoreVoltage:   point.CoreVoltage,
		Status:        status,
		MedianHash:    summary.MedianHash,
		ExpectedHash:  summary.ExpectedHash,
		Attainment:    summary.Attainment,
		MeanTemp:      summary.MeanTemp,
		P95Temp:       summary.P95Temp,
		P95VRTemp:     summary.P95VRTemp,
		P95Power:      summary.P95Power,
		ErrorPercent:  cloneFloat(summary.ErrorPercent),
		AcceptedDelta: summary.AcceptedDelta,
		RejectedDelta: summary.RejectedDelta,
		MeasuredAt:    now,
		RetryAfter:    retryAfter,
	}
	return minerController.states.SavePoint(&record)
}

func (minerController *controller) logWindow(
	state *lib.MinerState,
	point lib.OperatingPoint,
	summary windowSummary,
	result string,
) {
	minerController.logf(
		"%s %s at %d MHz/%d mV: %.0f/%.0f GH/s (%.1f%%), avg/p95 %.1f/%.1f°C, VR %.1f°C, %.1fW",
		state.Hostname,
		result,
		point.Frequency,
		point.CoreVoltage,
		summary.MedianHash,
		summary.ExpectedHash,
		summary.Attainment*100,
		summary.MeanTemp,
		summary.P95Temp,
		summary.P95VRTemp,
		summary.P95Power,
	)
}

func summarizeWindow(samples []telemetrySample) windowSummary {
	hashRates := make([]float64, 0, len(samples))
	expectedRates := make([]float64, 0, len(samples))
	temps := make([]float64, 0, len(samples))
	vrTemps := make([]float64, 0, len(samples))
	powers := make([]float64, 0, len(samples))
	errors := make([]float64, 0, len(samples))
	for _, sample := range samples {
		hashRates = append(hashRates, sample.hashRate)
		expectedRates = append(expectedRates, sample.expectedHash)
		temps = append(temps, sample.temp)
		vrTemps = append(vrTemps, sample.vrTemp)
		powers = append(powers, sample.power)
		if sample.errorPercent != nil {
			errors = append(errors, *sample.errorPercent)
		}
	}
	summary := windowSummary{
		MedianHash:   percentile(hashRates, 0.50),
		ExpectedHash: percentile(expectedRates, 0.50),
		MeanTemp:     mean(temps),
		P95Temp:      percentile(temps, 0.95),
		P95VRTemp:    percentile(vrTemps, 0.95),
		P95Power:     percentile(powers, 0.95),
	}
	if summary.ExpectedHash > 0 {
		summary.Attainment = summary.MedianHash / summary.ExpectedHash
	}
	if len(errors) > 0 {
		errorMedian := percentile(errors, 0.50)
		summary.ErrorPercent = &errorMedian
	}
	if len(samples) > 1 {
		first := samples[0]
		last := samples[len(samples)-1]
		if last.acceptedShare >= first.acceptedShare {
			summary.AcceptedDelta = last.acceptedShare - first.acceptedShare
		}
		if last.rejectedShare >= first.rejectedShare {
			summary.RejectedDelta = last.rejectedShare - first.rejectedShare
		}
	}
	return summary
}

func percentile(values []float64, fraction float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if fraction == 0.50 && len(sorted)%2 == 0 {
		middle := len(sorted) / 2
		return (sorted[middle-1] + sorted[middle]) / 2
	}
	index := int(math.Ceil(fraction*float64(len(sorted)))) - 1
	index = max(0, min(index, len(sorted)-1))
	return sorted[index]
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
	if summary.MedianHash <= 0 {
		return false
	}
	return summary.ErrorPercent == nil ||
		*summary.ErrorPercent <= settings.MaxErrorPercentage
}

func hasExplorationHeadroom(summary windowSummary, settings lib.Settings) bool {
	return summary.P95Temp < settings.TargetTemp &&
		summary.P95Power > 0 &&
		summary.P95Power <= settings.MaxPower-powerHeadroom &&
		(summary.P95VRTemp <= 0 ||
			summary.P95VRTemp <= settings.VRTempHigh*vrExplorationFactor)
}

func instantaneousSafetyFailure(
	info lib.Info,
	settings lib.Settings,
) (safetyFailure, bool) {
	switch {
	case info.Temp >= settings.TempCutoff:
		return safetyFailure{
			status: lib.PointThermal,
			reason: fmt.Sprintf(
				"ASIC temperature %.1f°C reached the %.1f°C cutoff",
				info.Temp,
				settings.TempCutoff,
			),
		}, true
	case info.Temp > settings.TempLimit:
		return safetyFailure{
			status: lib.PointThermal,
			reason: fmt.Sprintf(
				"ASIC temperature %.1f°C exceeded %.1f°C",
				info.Temp,
				settings.TempLimit,
			),
		}, true
	case info.Power >= settings.MaxPower:
		return safetyFailure{
			status: lib.PointPower,
			reason: fmt.Sprintf(
				"power %.1fW reached the %.1fW limit",
				info.Power,
				settings.MaxPower,
			),
		}, true
	case info.VRTemp >= settings.VRTempHigh:
		return safetyFailure{
			status: lib.PointVRHot,
			reason: fmt.Sprintf(
				"VR temperature %.1f°C reached the %.1f°C limit",
				info.VRTemp,
				settings.VRTempHigh,
			),
		}, true
	default:
		return safetyFailure{}, false
	}
}

func windowSafetyFailure(
	summary windowSummary,
	settings lib.Settings,
) (safetyFailure, bool) {
	switch {
	case summary.P95Temp > settings.TempLimit:
		return safetyFailure{status: lib.PointThermal, reason: "window ASIC temperature was high"}, true
	case summary.P95Power >= settings.MaxPower:
		return safetyFailure{status: lib.PointPower, reason: "window power reached its limit"}, true
	case summary.P95VRTemp >= settings.VRTempHigh:
		return safetyFailure{status: lib.PointVRHot, reason: "window VR temperature reached its limit"}, true
	default:
		return safetyFailure{}, false
	}
}

func operatingPointFromInfo(info lib.Info) lib.OperatingPoint {
	return lib.OperatingPoint{
		Frequency:   info.Frequency,
		CoreVoltage: info.CoreVoltage,
	}
}

func validLivePoint(point lib.OperatingPoint) bool {
	return point.Frequency > 0 && point.Frequency <= 10_000 &&
		point.CoreVoltage >= 500 && point.CoreVoltage <= 2000
}

func operatingPointAdvertised(
	asic lib.ASICSettings,
	point lib.OperatingPoint,
) bool {
	return optionAdvertised(asic.FrequencyOptions, point.Frequency) &&
		optionAdvertised(asic.VoltageOptions, point.CoreVoltage)
}

func optionAdvertised(options []int, target int) bool {
	index := sort.SearchInts(options, target)
	return index < len(options) && options[index] == target
}

func noMoreAggressive(target lib.OperatingPoint, live lib.OperatingPoint) bool {
	return target.Frequency <= live.Frequency &&
		target.CoreVoltage <= live.CoreVoltage
}

func minimumAdvertisedPoint(asic lib.ASICSettings) (lib.OperatingPoint, error) {
	if len(asic.FrequencyOptions) == 0 || len(asic.VoltageOptions) == 0 {
		return lib.OperatingPoint{}, fmt.Errorf("ASIC advertised no operating-point options")
	}
	return lib.OperatingPoint{
		Frequency:   asic.FrequencyOptions[0],
		CoreVoltage: asic.VoltageOptions[0],
	}, nil
}

func nextLowerOption(options []int, current int) (int, bool) {
	index := sort.SearchInts(options, current)
	if index == 0 {
		return 0, false
	}
	if index == len(options) || options[index] >= current {
		return options[index-1], true
	}
	return 0, false
}

func nextHigherOption(options []int, current int) (int, bool) {
	index := sort.Search(len(options), func(index int) bool {
		return options[index] > current
	})
	if index >= len(options) {
		return 0, false
	}
	return options[index], true
}

func findRecord(
	records []lib.OperatingPointRecord,
	point lib.OperatingPoint,
) (lib.OperatingPointRecord, bool) {
	for _, record := range records {
		if record.Frequency == point.Frequency &&
			record.CoreVoltage == point.CoreVoltage {
			return record, true
		}
	}
	return lib.OperatingPointRecord{}, false
}

func candidateDue(
	record lib.OperatingPointRecord,
	found bool,
	now time.Time,
) bool {
	return !found ||
		(!record.RetryAfter.IsZero() && !now.Before(record.RetryAfter))
}

func nextSweepCandidate(
	records []lib.OperatingPointRecord,
	frequency int,
	voltages []int,
	now time.Time,
) (lib.OperatingPoint, lib.OptimizerPhase, bool, bool) {
	var previous lib.OperatingPointRecord
	hasPrevious := false
	for index, voltage := range voltages {
		point := lib.OperatingPoint{
			Frequency:   frequency,
			CoreVoltage: voltage,
		}
		record, found := findRecord(records, point)
		if candidateDue(record, found, now) {
			phase := lib.PhaseVoltageTest
			if index == 0 {
				phase = lib.PhaseFrequencyTest
			}
			return point, phase, false, true
		}
		if safetyPointStatus(record.Status) {
			return lib.OperatingPoint{}, "", index == 0, false
		}
		if hasPrevious &&
			(record.Status == lib.PointNoGain || record.Status == lib.PointUnstable) &&
			!recordVoltageResponseUseful(record, previous) {
			return lib.OperatingPoint{}, "", false, false
		}
		previous = record
		hasPrevious = true
	}
	return lib.OperatingPoint{}, "", false, false
}

func priorVoltageRecord(
	records []lib.OperatingPointRecord,
	point lib.OperatingPoint,
) (lib.OperatingPointRecord, bool) {
	var prior lib.OperatingPointRecord
	found := false
	for _, record := range records {
		if record.Frequency != point.Frequency ||
			record.CoreVoltage >= point.CoreVoltage {
			continue
		}
		if !found || record.CoreVoltage > prior.CoreVoltage {
			prior = record
			found = true
		}
	}
	return prior, found
}

func voltageResponseUseful(
	summary windowSummary,
	prior lib.OperatingPointRecord,
) bool {
	hashImproved := prior.MedianHash <= 0 ||
		summary.MedianHash >= prior.MedianHash*(1+minimumHashGain)
	errorImproved := summary.ErrorPercent != nil &&
		prior.ErrorPercent != nil &&
		*prior.ErrorPercent-*summary.ErrorPercent >= minimumErrorImprovement
	return hashImproved || errorImproved
}

func recordVoltageResponseUseful(
	current lib.OperatingPointRecord,
	previous lib.OperatingPointRecord,
) bool {
	hashImproved := previous.MedianHash <= 0 ||
		current.MedianHash >= previous.MedianHash*(1+minimumHashGain)
	errorImproved := current.ErrorPercent != nil &&
		previous.ErrorPercent != nil &&
		*previous.ErrorPercent-*current.ErrorPercent >= minimumErrorImprovement
	return hashImproved || errorImproved
}

func safetyPointStatus(status string) bool {
	return status == lib.PointThermal ||
		status == lib.PointPower ||
		status == lib.PointVRHot
}

func targetSampleCount(settings lib.Settings) int {
	if settings.MetricsTime <= 0 {
		return 1
	}
	count := int(math.Ceil(
		float64(settings.EvaluationWindowTime) / float64(settings.MetricsTime),
	))
	return max(count, 1)
}

func safeToRecover(info lib.Info, settings lib.Settings) bool {
	if info.Temp <= 0 || info.Temp > settings.RecoveryTemp {
		return false
	}
	return info.VRTemp <= 0 || info.VRTemp < settings.VRTempHigh*vrExplorationFactor
}

func incrementOverheatCount(count int) int {
	const maximumTrackedOverheats = 1_000_000
	if count < 0 {
		return 1
	}
	if count >= maximumTrackedOverheats {
		return maximumTrackedOverheats
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

func cloneFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
