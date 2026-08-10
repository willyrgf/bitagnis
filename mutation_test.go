package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/willyrgf/bitagnis/lib"
)

type rediscoveryASICDevice struct {
	*scriptedMutationDevice
	asicIPs []string
}

func (device *rediscoveryASICDevice) GetASICSettings(_ context.Context, ip string) (lib.ASICSettings, error) {
	device.asicIPs = append(device.asicIPs, ip)
	return device.asic, nil
}

func newRootMutationStore(t *testing.T) (*lib.OptimizerStore, lib.Settings, lib.MinerState, time.Time) {
	t.Helper()
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	settings := rootTestSettings(t)
	now := time.Now().UTC().Round(time.Second)
	result, err := store.Apply(lib.Bootstrap{
		Info:           rootTestInfo(lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}, 100),
		IP:             "192.0.2.12",
		PairAdvertised: true,
	}, now)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, settings, result.State, now
}

func TestCooldownRecoveryReleasesFleetNormalMutationBlock(t *testing.T) {
	store, settings, _, now := newRootMutationStore(t)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	safetyState, err := store.LoadMiner(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	baseline := mustOpenEpoch(t, store, safetyState.MacAddr)
	safetyState.Phase = lib.PhaseCooldown
	safetyState.PhaseStartedAt = now
	safetyState.SafetyReason = lib.SafetyReasonFirmwareOverheat
	result, err := store.Apply(lib.CloseEpoch{
		State: safetyState, Epoch: baseline, Outcome: lib.EpochContradicted,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	safetyState = result.State

	otherInfo := rootTestInfo(point, 100)
	otherInfo.MacAddr = "aa:bb:cc:dd:ee:03"
	otherInfo.Hostname = "root-test-other"
	otherIP := "192.0.2.13"
	otherResult, err := store.Apply(lib.Bootstrap{
		Info: otherInfo, IP: otherIP, PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	otherState := closeInitialBaselineEpoch(t, store, otherResult.State, now)
	target := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	otherState.SetPendingMutation(lib.MutationOperatingPoint, target, now)
	if result, err := store.Apply(lib.SaveState{State: otherState}, now); err != nil {
		t.Fatal(err)
	} else {
		otherState = result.State
	}

	asic := rootTestASIC()
	safetyInfo := rootTestInfo(point, 100)
	observations := map[string]*minerObservation{
		safetyState.MacAddr: {
			miner: lib.DiscoveredMiner{IP: safetyState.IP, Info: safetyInfo},
			info:  safetyInfo, asic: asic, settings: settings, state: safetyState,
		},
		otherState.MacAddr: {
			miner: lib.DiscoveredMiner{IP: otherIP, Info: otherInfo},
			info:  otherInfo, asic: asic, settings: settings, state: otherState,
		},
	}
	coordinator := newMutationCoordinator(
		nil, store, lib.SettingsFile{},
		[]lib.DiscoveredMiner{observations[safetyState.MacAddr].miner, observations[otherState.MacAddr].miner},
		nil, "", nil, nil, log.New(io.Discard, "", 0), nil,
	)
	coordinator.gateOpen = true
	coordinator.now = func() time.Time { return now.Add(time.Hour) }
	if allowed, err := coordinator.Advance(context.Background(), observations, now); err != nil || allowed {
		t.Fatalf("advance while cooldown blocks fleet = allowed:%t err:%v", allowed, err)
	}
	if coordinator.normalActive != "" {
		t.Fatalf("normal mutation started during fleet safety block: %s", coordinator.normalActive)
	}
	if _, unfinished, err := store.UnfinishedMutationAttempt(otherState.MacAddr); err != nil || unfinished {
		t.Fatalf("other miner attempt started during cooldown: unfinished:%t err:%v", unfinished, err)
	}

	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	for poll := 1; poll <= recoveryHealthyPolls(settings); poll++ {
		at := now.Add(time.Duration(poll) * settings.MetricsTime)
		if err := minerController.controlMinerAfterSafety(
			context.Background(), &safetyState, mustReadablePoll(t, safetyInfo, asic), settings, at, true,
		); err != nil {
			t.Fatalf("recovery poll %d: %v", poll, err)
		}
	}
	epoch := mustOpenEpoch(t, store, safetyState.MacAddr)
	window, err := lib.NewWindowAggregate(
		30, settings.EvaluationWindowTime, 100, 100, 1, 55, 55, 70, 18, nil, 0, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	finishedAt := now.Add(time.Hour)
	if err := minerController.finishSafetyValidation(&safetyState, epoch, window, settings, finishedAt); err != nil {
		t.Fatal(err)
	}
	if safetyState.Phase != lib.PhaseBaseline || safetyState.HoldReason != "" ||
		safetyState.SafetyReason != "" || !safetyState.SettledAt.IsZero() {
		t.Fatalf("recovery did not resume the finite pass: %+v", safetyState)
	}
	recoveryBaseline := mustOpenEpoch(t, store, safetyState.MacAddr)
	if recoveryBaseline.Purpose != lib.EpochBaseline || recoveryBaseline.RequiredWindows != 2 {
		t.Fatalf("recovery baseline = %+v", recoveryBaseline)
	}

	observations[safetyState.MacAddr].state = safetyState
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if allowed, err := coordinator.Advance(ctx, observations, finishedAt); err != nil || allowed {
		t.Fatalf("advance after recovery = allowed:%t err:%v", allowed, err)
	}
	if coordinator.normalActive != otherState.MacAddr {
		t.Fatalf("normal mutation active = %q, want %q", coordinator.normalActive, otherState.MacAddr)
	}
	attempt, unfinished, err := store.UnfinishedMutationAttempt(otherState.MacAddr)
	if err != nil || !unfinished || attempt.Kind != lib.MutationOperatingPoint || attempt.TargetPoint() != target {
		t.Fatalf("other miner attempt = %+v unfinished:%t err:%v", attempt, unfinished, err)
	}
}

func TestWrongMACAfterPatchIsQuarantinedWithoutActuation(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	target := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	state.SetPendingMutation(lib.MutationOperatingPoint, target, now)
	if _, err := store.Apply(lib.SaveState{State: state}, now); err != nil {
		t.Fatal(err)
	}
	wrong := rootTestInfo(target, 100)
	wrong.MacAddr = "00:11:22:33:44:55"
	device := &scriptedMutationDevice{
		asic:   rootTestASIC(),
		source: rootTestInfo(state.CurrentPoint(), 100),
		target: wrong,
	}
	coordinator := newMutationCoordinator(
		device, store, lib.SettingsFile{}, []lib.DiscoveredMiner{{IP: state.IP, Info: device.source}}, nil, "",
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		}, nil, log.New(io.Discard, "", 0), nil,
	)
	coordinator.rediscoveryDelay = time.Millisecond
	coordinator.now = func() time.Time { return now }
	observation := &minerObservation{
		miner: lib.DiscoveredMiner{IP: state.IP, Info: device.source},
		info:  device.source, asic: device.asic, settings: settings, state: state,
	}
	if err := coordinator.startLocked(context.Background(), observation, "", ""); err != nil {
		t.Fatal(err)
	}
	result := <-coordinator.results
	if result.err == nil || result.failureStage == "" || !result.readbackUnavailable {
		t.Fatalf("wrong-MAC result = %+v", result)
	}
	coordinator.results <- result
	if _, err := coordinator.applyResultsLocked(); err != nil {
		t.Fatal(err)
	}
	if device.restartCount != 0 {
		t.Fatalf("wrong-MAC response issued restart: %d", device.restartCount)
	}
	attempts, err := store.ListMutationAttempts(state.MacAddr)
	if err != nil || len(attempts) != 1 || attempts[0].FailureStage != lib.MutationFailureConfiguredVerification {
		t.Fatalf("wrong-MAC attempt = %+v, %v", attempts, err)
	}
	loaded, err := store.LoadMiner(state.MacAddr)
	if err != nil || loaded.Phase != lib.PhaseOverheat || loaded.SafetyReason != lib.SafetyReasonMutationUncertain {
		t.Fatalf("wrong-MAC quarantine state = %+v, %v", loaded, err)
	}
}

func TestUnavailableSafetyReadbackRetainsTypedObligation(t *testing.T) {
	store, _, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	state.Phase = lib.PhaseCooldown
	state.HoldReason = ""
	state.SettledAt = time.Time{}
	state.SafetyReason = lib.SafetyReasonASICLimit
	state.SetPendingMutation(lib.MutationSafetyRollback, minimum, now)
	if _, err := store.Apply(lib.SaveState{State: state}, now); err != nil {
		t.Fatal(err)
	}
	attempt := lib.MutationAttempt{
		MacAddr: state.MacAddr, Kind: lib.MutationSafetyRollback, Reason: state.SafetyReason,
		FromFrequency: state.CurrentFrequency, FromCoreVoltage: state.CurrentCoreVoltage,
		TargetFrequency: minimum.Frequency, TargetCoreVoltage: minimum.CoreVoltage,
		IntentCreatedAt: now, StartedAt: now, ConfiguredVerifiedUptimeSeconds: -1,
	}
	attemptID, err := store.StartMutationAttempt(&attempt)
	if err != nil {
		t.Fatal(err)
	}
	patchAt := now.Add(time.Second)
	if err := store.AdvanceMutationAttempt(attemptID, lib.MutationMilestonePatchRequested, patchAt); err != nil {
		t.Fatal(err)
	}
	attempts, err := store.ListMutationAttempts(state.MacAddr)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("safety attempt = %+v, %v", attempts, err)
	}
	coordinator := newMutationCoordinator(
		nil, store, lib.SettingsFile{}, nil, nil, "", nil, nil,
		log.New(io.Discard, "", 0), nil,
	)
	coordinator.now = func() time.Time { return now.Add(time.Minute) }
	result := mutationResult{
		attemptID: attemptID, macAddr: state.MacAddr, hostname: state.Hostname,
		kind: lib.MutationSafetyRollback, point: minimum,
		failureStage:        lib.MutationFailureConfiguredVerification,
		readbackUnavailable: true,
	}
	if err := coordinator.handleTerminalMutationFailureLocked(result, attempts[0]); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadMiner(state.MacAddr)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.PendingKind != lib.MutationSafetyRollback || loaded.PendingPoint() != minimum ||
		loaded.Phase != lib.PhaseCooldown || loaded.SafetyReason != lib.SafetyReasonASICLimit {
		t.Fatalf("safety obligation was cleared: %+v", loaded)
	}
	closed, err := store.ListMutationAttempts(state.MacAddr)
	if err != nil || len(closed) != 1 || closed[0].FailureStage != lib.MutationFailureConfiguredVerification {
		t.Fatalf("closed safety attempt = %+v, %v", closed, err)
	}
}

func TestProductionOverheatLoopStateEntersCooldownWithoutMutation(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	state.SetCurrentPoint(minimum)
	state.Phase = lib.PhaseOverheat
	state.HoldReason = ""
	state.SettledAt = time.Time{}
	state.PhaseStartedAt = now.Add(-time.Hour)
	state.SafetyReason = lib.SafetyReasonMutationUncertain
	state.SetPendingMutation(lib.MutationSafetyRollback, minimum, now.Add(-time.Minute))
	if result, err := store.Apply(lib.SaveState{State: state}, now); err != nil {
		t.Fatal(err)
	} else {
		state = result.State
	}
	info := rootTestInfo(minimum, 100)
	controller := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	handled, err := controller.enforceMinerSafety(
		context.Background(), &state, info, rootTestASIC(), settings, now,
	)
	if err != nil || !handled {
		t.Fatalf("production recovery poll = handled:%t err:%v", handled, err)
	}
	if state.Phase != lib.PhaseCooldown || state.PendingKind != "" || state.CurrentPoint() != minimum {
		t.Fatalf("production loop state did not enter mutation-free cooldown: %+v", state)
	}
	attempts, err := store.ListMutationAttempts(state.MacAddr)
	if err != nil || len(attempts) != 0 {
		t.Fatalf("mutation-free recovery created attempts: %+v, %v", attempts, err)
	}
}

func TestRedundantMinimumSafetyMutationIsSupersededBeforePatch(t *testing.T) {
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	tests := []struct {
		name   string
		kind   lib.MutationKind
		reason lib.SafetyReason
	}{
		{name: "uncertain rollback", kind: lib.MutationSafetyRollback, reason: lib.SafetyReasonMutationUncertain},
		{name: "completed firmware recovery", kind: lib.MutationOverheatRecovery, reason: lib.SafetyReasonFirmwareOverheat},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			store, settings, state, now := newRootMutationStore(t)
			state = closeInitialBaselineEpoch(t, store, state, now)
			state.SetCurrentPoint(minimum)
			state.Phase = lib.PhaseOverheat
			state.HoldReason = ""
			state.SettledAt = time.Time{}
			state.PhaseStartedAt = now
			state.SafetyReason = testCase.reason
			state.SetPendingMutation(testCase.kind, minimum, now)
			if result, err := store.Apply(lib.SaveState{State: state}, now); err != nil {
				t.Fatal(err)
			} else {
				state = result.State
			}

			info := rootTestInfo(minimum, 100)
			device := &scriptedMutationDevice{asic: rootTestASIC(), source: info, target: info}
			var logs bytes.Buffer
			coordinator := newMutationCoordinator(
				device, store, lib.SettingsFile{}, []lib.DiscoveredMiner{{IP: state.IP, Info: info}}, nil, "",
				nil, nil, log.New(&logs, "", 0), nil,
			)
			coordinator.now = func() time.Time { return now }
			observation := &minerObservation{
				miner: lib.DiscoveredMiner{IP: state.IP, Info: info}, info: info,
				asic: device.asic, settings: settings, state: state,
			}
			if !coordinator.canStartLocked(observation) {
				t.Fatal("coordinator refused to run safety supersession preflight")
			}
			if err := coordinator.startLocked(context.Background(), observation, "", ""); err != nil {
				t.Fatal(err)
			}
			result := <-coordinator.results
			if result.err == nil {
				t.Fatal("redundant safety mutation unexpectedly succeeded")
			}
			coordinator.results <- result
			if _, err := coordinator.applyResultsLocked(); err != nil {
				t.Fatal(err)
			}
			if device.patchCount != 0 || device.restartCount != 0 {
				t.Fatalf("redundant mutation touched hardware: patches=%d restarts=%d", device.patchCount, device.restartCount)
			}
			if strings.Contains(logs.String(), "Mutation ") {
				t.Fatalf("safety-superseded worker was logged as a mutation failure: %q", logs.String())
			}
			loaded, err := store.LoadMiner(state.MacAddr)
			if err != nil || loaded.Phase != lib.PhaseCooldown || loaded.PendingKind != "" || loaded.CurrentPoint() != minimum {
				t.Fatalf("reconciled state = %+v, %v", loaded, err)
			}
			attempts, err := store.ListMutationAttempts(state.MacAddr)
			if err != nil || len(attempts) != 1 || attempts[0].FailureStage != lib.MutationFailureSafetySuperseded {
				t.Fatalf("superseded attempts = %+v, %v", attempts, err)
			}
		})
	}
}

func TestRebootPollsPreserveSafetyAuthority(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	state.SetCurrentPoint(minimum)
	state.Phase = lib.PhaseOverheat
	state.HoldReason = ""
	state.SettledAt = time.Time{}
	state.PhaseStartedAt = now
	state.SafetyReason = lib.SafetyReasonMutationUncertain
	state.SetPendingMutation(lib.MutationSafetyRollback, minimum, now)
	if result, err := store.Apply(lib.SaveState{State: state}, now); err != nil {
		t.Fatal(err)
	} else {
		state = result.State
	}
	attempt := lib.MutationAttempt{
		MacAddr: state.MacAddr, Kind: lib.MutationSafetyRollback, Reason: state.SafetyReason,
		FromFrequency: minimum.Frequency, FromCoreVoltage: minimum.CoreVoltage,
		TargetFrequency: minimum.Frequency, TargetCoreVoltage: minimum.CoreVoltage,
		IntentCreatedAt: state.PendingSince, StartedAt: now, ConfiguredVerifiedUptimeSeconds: -1,
	}
	id, err := store.StartMutationAttempt(&attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, lib.MutationMilestonePatchRequested, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordConfiguredVerification(id, now.Add(2*time.Second), 100); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, lib.MutationMilestoneRestartRequested, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}

	info := rootTestInfo(minimum, 101)
	info.Temp = 0
	controller := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	handled, err := controller.enforceMinerSafety(
		context.Background(), &state, info, rootTestASIC(), settings, now.Add(4*time.Second),
	)
	if err != nil || !handled {
		t.Fatalf("incomplete reboot poll = handled:%t err:%v", handled, err)
	}
	loaded, err := store.LoadMiner(state.MacAddr)
	if err != nil || loaded != state {
		t.Fatalf("incomplete poll changed pending safety authority: loaded=%+v state=%+v err=%v", loaded, state, err)
	}
	stored, unfinished, err := store.UnfinishedMutationAttempt(state.MacAddr)
	if err != nil || !unfinished || stored.ID != id || stored.FailureStage != "" {
		t.Fatalf("incomplete poll superseded rebooting attempt: attempt=%+v unfinished=%t err=%v", stored, unfinished, err)
	}

	// A fully readable neutral poll is still not reboot proof. The worker owns completion until it
	// records that proof, so polling must preserve the same authority here too.
	info.Temp = 55
	handled, err = controller.enforceMinerSafety(
		context.Background(), &state, info, rootTestASIC(), settings, now.Add(5*time.Second),
	)
	if err != nil || !handled {
		t.Fatalf("neutral reboot poll = handled:%t err:%v", handled, err)
	}
	loaded, err = store.LoadMiner(state.MacAddr)
	if err != nil || loaded != state {
		t.Fatalf("neutral poll changed pending safety authority: loaded=%+v state=%+v err=%v", loaded, state, err)
	}
	stored, unfinished, err = store.UnfinishedMutationAttempt(state.MacAddr)
	if err != nil || !unfinished || stored.ID != id || stored.FailureStage != "" {
		t.Fatalf("neutral poll superseded rebooting attempt: attempt=%+v unfinished=%t err=%v", stored, unfinished, err)
	}
}

// TestTerminalOperatingPointMutationFailureIsRejectedNotStarved covers
// handleTerminalMutationFailureLocked's fallback branch for a non-trial MutationOperatingPoint
// attempt that fails its pipeline before ever completing (preflight/PATCH/restart/rediscovery) —
// the "never got its patch/restart to complete" shape that reads like starvation but is
// deliberately classified HoldRejected, because this site has no durable epoch to recover a
// starved-baseline resume target from (see the code's own comment at this call site).
//
// This branch calls state.ClearPendingMutation() before calling Apply(lib.FailMutation{...}). That
// used to always fail applyFailMutation's staleness check, which required the caller's State to
// match durable storage on PendingKind/PendingPoint/FallbackPoint/MiningPending exactly — impossible
// here, since ClearPendingMutation had just changed those fields locally while durable storage still
// held the original pending authority (the attempt never reached CompleteMutation, so nothing else
// cleared it durably first). That check has been narrowed to CurrentPoint and the accounting cursor
// only, matching QuarantineMutation's pattern; the real "is this genuinely the attempt's own pending
// authority" invariant is enforced separately, against durable and attempt.Kind/TargetPoint. See
// applyFailMutation's comment in lib/state.go for the full reasoning.
func TestTerminalOperatingPointMutationFailureIsRejectedNotStarved(t *testing.T) {
	store, _, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	target := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	state.SetPendingMutation(lib.MutationOperatingPoint, target, now)
	if _, err := store.Apply(lib.SaveState{State: state}, now); err != nil {
		t.Fatal(err)
	}
	attempt := lib.MutationAttempt{
		MacAddr: state.MacAddr, Kind: lib.MutationOperatingPoint,
		FromFrequency: state.CurrentFrequency, FromCoreVoltage: state.CurrentCoreVoltage,
		TargetFrequency: target.Frequency, TargetCoreVoltage: target.CoreVoltage,
		IntentCreatedAt: now, StartedAt: now, ConfiguredVerifiedUptimeSeconds: -1,
	}
	attemptID, err := store.StartMutationAttempt(&attempt)
	if err != nil {
		t.Fatal(err)
	}
	attempts, err := store.ListMutationAttempts(state.MacAddr)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("operating-point attempt = %+v, %v", attempts, err)
	}
	coordinator := newMutationCoordinator(
		nil, store, lib.SettingsFile{}, nil, nil, "", nil, nil,
		log.New(io.Discard, "", 0), nil,
	)
	coordinator.now = func() time.Time { return now.Add(time.Minute) }
	result := mutationResult{
		attemptID: attemptID, macAddr: state.MacAddr, hostname: state.Hostname,
		kind: lib.MutationOperatingPoint, point: target,
		failureStage:        lib.MutationFailurePreflight,
		readbackUnavailable: false,
	}
	if err := coordinator.handleTerminalMutationFailureLocked(result, attempts[0]); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadMiner(state.MacAddr)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Phase != lib.PhaseHold || loaded.HoldReason != lib.HoldRejected || loaded.PendingKind != "" {
		t.Fatalf("uncompleted operating-point mutation failure was not classified rejected: %+v", loaded)
	}
	closed, err := store.ListMutationAttempts(state.MacAddr)
	if err != nil || len(closed) != 1 || closed[0].FailureStage != lib.MutationFailurePreflight {
		t.Fatalf("closed operating-point attempt = %+v, %v", closed, err)
	}
}

// TestMiningResumeFailureAfterCompletedOperatingPointMutationIsRejected covers
// handleMutationResumeFailureLocked's fallback branch: a non-trial MutationOperatingPoint attempt
// that fully completed (PATCH + restart + readback verified — the device is confirmed running the
// target configuration) but never produced a positive hash within the health deadline. That is a
// real, measured conclusion about the configuration, mirroring the trial-phase branch's identical
// no-positive-hash outcome, so it is classified HoldRejected, not HoldStarved.
func TestMiningResumeFailureAfterCompletedOperatingPointMutationIsRejected(t *testing.T) {
	store, _, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	target := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	state.SetPendingMutation(lib.MutationOperatingPoint, target, now)
	saveResult, err := store.Apply(lib.SaveState{State: state}, now)
	if err != nil {
		t.Fatal(err)
	}
	state = saveResult.State
	attempt := lib.MutationAttempt{
		MacAddr: state.MacAddr, Kind: lib.MutationOperatingPoint,
		FromFrequency: state.CurrentFrequency, FromCoreVoltage: state.CurrentCoreVoltage,
		TargetFrequency: target.Frequency, TargetCoreVoltage: target.CoreVoltage,
		IntentCreatedAt: now, StartedAt: now, ConfiguredVerifiedUptimeSeconds: -1,
	}
	attemptID, err := store.StartMutationAttempt(&attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(attemptID, lib.MutationMilestonePatchRequested, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordConfiguredVerification(attemptID, now.Add(2*time.Second), 101); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(attemptID, lib.MutationMilestoneRestartRequested, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(attemptID, lib.MutationMilestoneRebootVerified, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	// handleMutationResumeFailureLocked's own comment states the precondition precisely: "This
	// attempt already completed (PATCH + restart + readback verified)". CompleteMutation is what
	// durably moves CurrentPoint to the target and clears PendingKind — required here to actually
	// represent that documented precondition (applyFailMutation's staleness check itself no longer
	// requires this, having been narrowed to CurrentPoint and the accounting cursor only, but
	// skipping completion would test a scenario this function was never meant to handle).
	completeResult, err := store.Apply(lib.CompleteMutation{
		MacAddr: state.MacAddr, IP: state.IP, AttemptID: attemptID,
	}, now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	state = completeResult.State
	attempts, err := store.ListMutationAttempts(state.MacAddr)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("operating-point attempt = %+v, %v", attempts, err)
	}
	observation := &minerObservation{state: state}
	coordinator := &mutationCoordinator{states: store}
	if err := coordinator.handleMutationResumeFailureLocked(observation, attempts[0], now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadMiner(state.MacAddr)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Phase != lib.PhaseHold || loaded.HoldReason != lib.HoldRejected || loaded.PendingKind != "" {
		t.Fatalf("completed operating-point mutation resume failure was not classified rejected: %+v", loaded)
	}
}

func TestRetuneRequiresVerifiedFinalSelectionAndElapsedRamp(t *testing.T) {
	store, _, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	settings := rootTestSettings(t)
	info := rootTestInfo(state.CurrentPoint(), 100)
	asic := rootTestASIC()
	if !qualifiesSettledObservation(store, state, info, asic, settings, now, true) {
		t.Fatal("verified optimized hold was rejected")
	}
	// An unsettled optimized HOLD with its required validation epoch must not qualify for retune.
	state.SettledAt = time.Time{}
	if _, err := store.Apply(lib.OpenEpoch{
		State: state, Purpose: lib.EpochHoldValidation, Point: state.CurrentPoint(), RequiredWindows: 1,
	}, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if qualifiesSettledObservation(store, state, info, asic, settings, now, true) {
		t.Fatal("retune qualification ignored an open evidence epoch")
	}
	state.SettledAt = now.Add(-time.Second)
	closed, err := store.Apply(lib.CloseEpoch{
		State: state, Epoch: mustOpenEpoch(t, store, rootTestMAC), Outcome: lib.EpochValidated,
	}, now.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	state = closed.State
	state.SetBestPoint(lib.OperatingPoint{Frequency: 550, CoreVoltage: 1000})
	state.BestHashRate = 200
	if qualifiesSettledObservation(store, state, info, asic, settings, now, true) {
		t.Fatal("retune qualification ignored a stronger non-selected validated point")
	}

	coordinator := newMutationCoordinator(
		nil, store, lib.SettingsFile{}, []lib.DiscoveredMiner{{IP: state.IP, Info: info}}, nil,
		state.Hostname, nil, nil, log.New(io.Discard, "", 0), nil,
	)
	coordinator.now = func() time.Time { return now }
	coordinator.retuneHost = state.Hostname
	accepted, err := coordinator.advanceRetuneLocked(map[string]*minerObservation{
		state.MacAddr: {miner: lib.DiscoveredMiner{IP: state.IP, Info: info}, info: info, asic: asic, settings: settings, state: state},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if accepted || coordinator.retuneHealthyCount != 0 {
		t.Fatal("retune advanced from an unverified final selection")
	}
}

func TestRetuneDeadlineStartsOnDiscoveryAndExpiresWhileAbsent(t *testing.T) {
	coordinator := &mutationCoordinator{
		retuneHost: "root-test",
		logger:     log.New(io.Discard, "", 0),
	}
	first := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	coordinator.trackRetuneDeadlineLocked(nil, first)
	if !coordinator.retuneFirstSeen.IsZero() {
		t.Fatal("retune timer started without a successful discovery")
	}
	observation := &minerObservation{info: lib.Info{Hostname: "root-test"}}
	coordinator.trackRetuneDeadlineLocked(map[string]*minerObservation{"mac": observation}, first.Add(time.Hour))
	if !coordinator.retuneFirstSeen.Equal(first.Add(time.Hour)) {
		t.Fatalf("retune first discovery = %v", coordinator.retuneFirstSeen)
	}
	coordinator.trackRetuneDeadlineLocked(nil, first.Add(time.Hour+3*time.Minute))
	if coordinator.retuneHost != "" || !coordinator.retuneFirstSeen.IsZero() || !coordinator.retuneRefused {
		t.Fatalf("expired retune request = host:%q first:%v refused:%t", coordinator.retuneHost, coordinator.retuneFirstSeen, coordinator.retuneRefused)
	}
}

func TestRetuneDiscoveryStartsDeadlineBeforeMetrics(t *testing.T) {
	first := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	coordinator := &mutationCoordinator{
		retuneHost: "root-test",
		routes: map[string]lib.DiscoveredMiner{
			"aa:bb:cc:dd:ee:01": {Info: lib.Info{Hostname: "root-test"}},
		},
		logger: log.New(io.Discard, "", 0),
	}
	coordinator.RecordRetuneDiscovery(first)
	if !coordinator.retuneFirstSeen.Equal(first) {
		t.Fatalf("retune first discovery = %v", coordinator.retuneFirstSeen)
	}
	coordinator.trackRetuneDeadlineLocked(nil, first.Add(3*time.Minute))
	if coordinator.retuneHost != "" || !coordinator.retuneRefused {
		t.Fatalf("retune deadline did not expire before metrics: host=%q refused=%t", coordinator.retuneHost, coordinator.retuneRefused)
	}
}

func TestRetuneHealthyPollsMustBeConsecutive(t *testing.T) {
	coordinator := &mutationCoordinator{
		retuneHost:         "root-test",
		retuneHealthyCount: 1,
		logger:             log.New(io.Discard, "", 0),
	}
	if _, err := coordinator.advanceRetuneLocked(nil, time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if coordinator.retuneHealthyCount != 0 {
		t.Fatalf("retune healthy count after absent poll = %d", coordinator.retuneHealthyCount)
	}
}

func TestRetuneSafetyBlockBreaksConsecutivePair(t *testing.T) {
	store, _, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	info := rootTestInfo(state.CurrentPoint(), 100)
	coordinator := newMutationCoordinator(
		nil, store, lib.SettingsFile{}, []lib.DiscoveredMiner{{IP: state.IP, Info: info}}, nil,
		state.Hostname, nil, nil, log.New(io.Discard, "", 0), nil,
	)
	coordinator.retuneHost = state.Hostname
	observations := map[string]*minerObservation{
		state.MacAddr: {info: info, asic: rootTestASIC(), settings: rootTestSettings(t), state: state},
	}
	accepted, err := coordinator.advanceRetuneLocked(observations, now)
	if err != nil || accepted || coordinator.retuneHealthyCount != 1 {
		t.Fatalf("first retune poll = accepted:%t count:%d err:%v", accepted, coordinator.retuneHealthyCount, err)
	}
	state.Phase = lib.PhaseCooldown
	state.HoldReason = ""
	state.SettledAt = time.Time{}
	if _, err := store.Apply(lib.SaveState{State: state}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Advance(context.Background(), nil, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if coordinator.retuneHealthyCount != 0 {
		t.Fatalf("retune healthy count after safety block = %d", coordinator.retuneHealthyCount)
	}
}

func TestOffGridManualObservationRequiresTwoPolls(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	controller := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	offGrid := lib.OperatingPoint{Frequency: 500, CoreVoltage: 1000}
	asic := rootTestASIC()
	firstPoll := mustReadablePoll(t, rootTestInfo(offGrid, 100), asic)
	if err := controller.observeExternalPoint(&state, firstPoll, settings, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if state.CurrentPoint() == offGrid || state.ObservedCount != 1 {
		t.Fatalf("first off-grid observation = %+v", state)
	}
	secondPoll := mustReadablePoll(t, rootTestInfo(offGrid, 100), asic)
	if err := controller.observeExternalPoint(&state, secondPoll, settings, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if state.CurrentPoint() != offGrid || state.Phase != lib.PhaseHold || state.HoldReason != lib.HoldRejected {
		t.Fatalf("second off-grid observation = %+v", state)
	}
}

// TestPhaseHandlingRunsBeforeLiveDifferingPointReconciliation is commit 5's ordering-fix regression
// test. Live-point reconciliation is an input to every phase, not a phase itself — it must not
// precede the PendingKind/Overheat/Cooldown recovery handling (the exact ordering defect that left
// mineira deadlocked in COOLDOWN, since COOLDOWN's own exit check was unreachable once a differing
// live point returned early into observeExternalPoint first). The fix is deliberately scoped no
// wider than the RFC's own cited range: it must NOT also move reconciliation past the Hold-reason
// switch, or a settled HoldOptimized/HoldManual miner's live-point drift would silently stop being
// reconciled at all — a regression the RFC never discusses and this fix must not introduce. COOLDOWN's
// own exit now has direct live coverage elsewhere (see
// TestCooldownExitsAfterConsecutiveHealthyPollsAndClearsSafetyReason); this test instead proves the
// negative for the other phase the reordering affects: a settled, optimized miner's live-point
// drift is still tracked and, after two confirmations, still adopted — exactly as it was before
// this reordering.
func TestPhaseHandlingRunsBeforeLiveDifferingPointReconciliation(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	controller := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	drifted := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	asic := rootTestASIC()
	for poll := 0; poll < 2; poll++ {
		at := now.Add(time.Duration(poll+1) * time.Second)
		if err := controller.controlMinerAfterSafety(
			context.Background(), &state, mustReadablePoll(t, rootTestInfo(drifted, 100), asic), settings, at, true,
		); err != nil {
			t.Fatalf("poll %d: %v", poll, err)
		}
	}
	loaded, err := store.LoadMiner(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.CurrentPoint() != drifted || loaded.Phase != lib.PhaseHold || loaded.HoldReason != lib.HoldManual {
		t.Fatalf("settled HoldOptimized drift was not reconciled after two confirmations, as it was before this reordering: %+v", loaded)
	}
}

// TestLedgerReconciledPointIsAdoptedNotRefusedAsForeign covers the case ledgerReconciledAttempt
// actually reaches, NOT mineira's own scenario (see that function's doc comment for why mineira's
// safety-recovery case is still blocked by SafetyReason, unresolved pending architect review): a
// trial candidate's PATCH is verified against the device, but the attempt then fails for a reason
// unrelated to safety, returning the miner to a clean PhaseBaseline with no SafetyReason. This is
// reachable in production via mutation.go's mining-resume-health-check failure path
// (handleMutationResumeFailureLocked -> FailMutationFinalizeTrial{Decision: TrialReturn}); this
// test constructs the same clean, reconcilable end state via SupersedeMutation instead, since that
// lets the test set the resulting state directly without threading the full resume-health machinery
// through a fake device. If the device is actually still running the failed attempt's verified
// target, the live observation legitimately differs from durable current. The ledger lookup must
// attribute the eventual adoption to the controller's own failed attempt, not log it as a foreign
// manual retune — while still requiring the same two consecutive confirmations as any other manual
// observation.
func TestLedgerReconciledPointIsAdoptedNotRefusedAsForeign(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	incumbent := state.CurrentPoint()
	candidate := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	admitResult := admitTestTrial(t, store, state, candidate, lib.PhaseUndervolt, 100, now)
	state = admitResult.State
	attemptID := admitResult.AttemptID
	if err := store.AdvanceMutationAttempt(attemptID, lib.MutationMilestonePatchRequested, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	verifiedAt := now.Add(2 * time.Second)
	if err := store.RecordConfiguredVerification(attemptID, verifiedAt, 101); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(attemptID, lib.MutationMilestoneRestartRequested, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	// The ledger shape (patch_requested_at and configured_verified_at set, restart_requested_at set,
	// reboot_verified_at never reached, then superseded) matches mineira's exactly. What does NOT
	// match mineira is the resulting MinerState below: real code never clears SafetyReason on this
	// path (see ledgerReconciledAttempt's doc comment), so a miner recovering the way mineira did
	// would still have SafetyReason set here and this scenario would not be reachable. This directly
	// constructs a state with SafetyReason already cleared — via SupersedeMutation's caller-chosen
	// replacement state, not via any real code path — specifically to test ledgerReconciledAttempt
	// in isolation from that still-open, unrelated gap. Durable CurrentPoint stays at the stale
	// incumbent while the device itself is still running the candidate the verified PATCH configured.
	expected := state
	state.SetFallbackPoint(lib.OperatingPoint{})
	state.ClearPendingMutation()
	state.Phase = lib.PhaseBaseline
	state.PhaseStartedAt = now.Add(4 * time.Second)
	supersedeResult, err := store.Apply(lib.SupersedeMutation{
		Expected: expected, State: state, AttemptID: attemptID,
	}, now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	state = supersedeResult.State
	if state.Phase != lib.PhaseBaseline || state.SafetyReason != "" || state.PendingKind != "" || state.CurrentPoint() != incumbent {
		t.Fatalf("precondition: miner is not in a clean, reconcilable state: %+v", state)
	}

	var buffer bytes.Buffer
	controller := &controller{states: store, logger: log.New(&buffer, "", 0), runtimes: make(map[string]*minerRuntime)}
	asic := rootTestASIC()
	// The device is actually still running the superseded attempt's verified target — the PATCH
	// held, only the higher-level attempt was superseded. Two consecutive polls confirm it.
	for poll := 0; poll < 2; poll++ {
		at := now.Add(time.Duration(5+poll) * time.Second)
		if err := controller.controlMinerAfterSafety(
			context.Background(), &state, mustReadablePoll(t, rootTestInfo(candidate, 100), asic), settings, at, true,
		); err != nil {
			t.Fatalf("poll %d: %v", poll, err)
		}
	}
	if state.CurrentPoint() != candidate {
		t.Fatalf("ledger-confirmed point was not adopted after two confirmations: %+v", state)
	}
	log := buffer.String()
	if !strings.Contains(log, "Reconciled operating point") || !strings.Contains(log, "the controller's own ledger") {
		t.Fatalf("adoption was not attributed to the controller's own ledger: %q", log)
	}
	if strings.Contains(log, "Adopted external operating point") {
		t.Fatalf("a controller-caused change was logged as a foreign manual retune: %q", log)
	}
}

// TestUnreconciledForeignPointIsStillAdoptedAsManual confirms the ledger lookup changes only
// attribution, never the mechanism: a live point with no matching ledger entry goes through the
// same two-confirmation manual-observation path as before, ending in the same outcome and the same
// "Adopted external operating point" log line.
func TestUnreconciledForeignPointIsStillAdoptedAsManual(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	var buffer bytes.Buffer
	controller := &controller{states: store, logger: log.New(&buffer, "", 0), runtimes: make(map[string]*minerRuntime)}
	asic := rootTestASIC()
	manual := lib.OperatingPoint{Frequency: 490, CoreVoltage: 1060}
	for poll := 0; poll < 2; poll++ {
		at := now.Add(time.Duration(poll+1) * time.Second)
		if err := controller.controlMinerAfterSafety(
			context.Background(), &state, mustReadablePoll(t, rootTestInfo(manual, 100), asic), settings, at, true,
		); err != nil {
			t.Fatalf("poll %d: %v", poll, err)
		}
	}
	if state.CurrentPoint() != manual {
		t.Fatalf("unreconciled manual point was not adopted after two confirmations: %+v", state)
	}
	log := buffer.String()
	if !strings.Contains(log, "Adopted external operating point") {
		t.Fatalf("manual adoption was not logged as external: %q", log)
	}
	if strings.Contains(log, "controller's own ledger") {
		t.Fatalf("a genuinely foreign change was misattributed to the ledger: %q", log)
	}
}

func TestPostRediscoveryASICReadUsesTheRediscoveredIP(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	target := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	state.SetPendingMutation(lib.MutationOperatingPoint, target, now)
	if _, err := store.Apply(lib.SaveState{State: state}, now); err != nil {
		t.Fatal(err)
	}
	device := &rediscoveryASICDevice{scriptedMutationDevice: &scriptedMutationDevice{
		asic: rootTestASIC(), source: rootTestInfo(state.CurrentPoint(), 100), target: rootTestInfo(target, 100),
	}}
	newIP := "192.0.2.44"
	coordinator := newMutationCoordinator(
		device, store, lib.SettingsFile{}, []lib.DiscoveredMiner{{IP: state.IP, Info: device.source}}, nil, "",
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			postBoot := device.target
			postBoot.UpTimeSeconds = 1
			return lib.DiscoveredMiner{IP: newIP, Info: postBoot}, nil
		}, nil, log.New(io.Discard, "", 0), nil,
	)
	coordinator.rediscoveryDelay = time.Millisecond
	coordinator.rebootDeadline = time.Second
	coordinator.now = func() time.Time { return now }
	observation := &minerObservation{
		miner: lib.DiscoveredMiner{IP: state.IP, Info: device.source},
		info:  device.source, asic: device.asic, settings: settings, state: state,
	}
	mutationContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := coordinator.startLocked(mutationContext, observation, "", ""); err != nil {
		t.Fatal(err)
	}
	var result mutationResult
	select {
	case result = <-coordinator.results:
	case <-mutationContext.Done():
		t.Fatal(mutationContext.Err())
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	if err := coordinator.completeMutationLocked(result); err != nil {
		t.Fatal(err)
	}
	coordinator.now = func() time.Time { return now.Add(10 * time.Second) }
	if err := coordinator.completeMutationLocked(result); err != nil {
		t.Fatalf("idempotent completion retry: %v", err)
	}
	if len(device.asicIPs) == 0 || device.asicIPs[len(device.asicIPs)-1] != newIP {
		t.Fatalf("ASIC read IPs = %v, want final read at %s", device.asicIPs, newIP)
	}
}

func TestFirmwareEvidenceWinsOverUnsupportedGrid(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	controller := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	info := rootTestInfo(state.CurrentPoint(), 100)
	info.Frequency = 50
	info.CoreVoltage = 1000
	info.OverHeatMode = 1
	grid := rootTestASIC()
	grid.VoltageOptions = []int{1000, 1050, 1100, 1150, 1200, 1250}
	handled, err := controller.enforceMinerSafety(context.Background(), &state, info, grid, settings, now)
	if err != nil || !handled {
		t.Fatalf("unsupported-grid firmware handling = handled:%t err:%v", handled, err)
	}
	loaded, err := store.LoadMiner(state.MacAddr)
	if err != nil || loaded.Phase != lib.PhaseOverheat || loaded.SafetyReason != lib.SafetyReasonFirmwareOverheat {
		t.Fatalf("unsupported-grid firmware state = %+v, %v", loaded, err)
	}
}

func TestUnreadableSafetySupersessionPreservesFirmwareCause(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	state.Phase = lib.PhaseOverheat
	state.HoldReason = ""
	state.SettledAt = time.Time{}
	state.SafetyReason = lib.SafetyReasonFirmwareOverheat
	state.SetPendingMutation(lib.MutationOverheatRecovery, minimum, now)
	if result, err := store.Apply(lib.SaveState{State: state}, now); err != nil {
		t.Fatal(err)
	} else {
		state = result.State
	}
	attempt := lib.MutationAttempt{
		MacAddr: state.MacAddr, Kind: lib.MutationOverheatRecovery, Reason: state.SafetyReason,
		FromFrequency: state.CurrentFrequency, FromCoreVoltage: state.CurrentCoreVoltage,
		TargetFrequency: minimum.Frequency, TargetCoreVoltage: minimum.CoreVoltage,
		IntentCreatedAt: state.PendingSince, StartedAt: now, ConfiguredVerifiedUptimeSeconds: -1,
	}
	id, err := store.StartMutationAttempt(&attempt)
	if err != nil {
		t.Fatal(err)
	}
	attempt.ID = id
	coordinator := newMutationCoordinator(
		nil, store, lib.SettingsFile{}, nil, nil, "", nil, nil,
		log.New(io.Discard, "", 0), nil,
	)
	coordinator.now = func() time.Time { return now.Add(time.Minute) }
	request := mutationRequest{
		attemptID: id, macAddr: state.MacAddr, kind: attempt.Kind,
		point: minimum, settings: settings, attempt: attempt,
	}
	if err := coordinator.supersedeReadback(request, rootTestInfo(minimum, 100), offGridASIC()); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadMiner(state.MacAddr)
	if err != nil || loaded.SafetyReason != lib.SafetyReasonFirmwareOverheat ||
		loaded.Phase != lib.PhaseOverheat || loaded.PendingKind != "" {
		t.Fatalf("unreadable supersession downgraded firmware cause: %+v, %v", loaded, err)
	}
}

func offGridASIC() lib.ASICSettings {
	grid := rootTestASIC()
	grid.VoltageOptions = []int{1000, 1050, 1100, 1150, 1200, 1250}
	return grid
}

// TestUnsupportedGridWithUnsafeTelemetryEscalatesOnThatSamePoll is the boundary the unreadable-poll
// rule must not cross: telemetry that failed grid validation but is nonetheless unsafe (here, a hard
// ASIC temperature limit) must escalate immediately, at every value of unreadable_poll_count — never
// suppressed and never deferred to the count-based limit.
func TestUnsupportedGridWithUnsafeTelemetryEscalatesOnThatSamePoll(t *testing.T) {
	for _, priorCount := range []int{0, 5, 11} {
		store, settings, state, now := newRootMutationStore(t)
		controller := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
		state.UnreadablePollCount = priorCount
		if _, err := store.Apply(lib.SaveState{State: state}, now); err != nil {
			t.Fatal(err)
		}
		info := rootTestInfo(state.CurrentPoint(), 100)
		info.Temp = settings.TempLimit + 1
		handled, err := controller.enforceMinerSafety(context.Background(), &state, info, offGridASIC(), settings, now)
		if err != nil || !handled {
			t.Fatalf("priorCount=%d: unsafe-telemetry handling = handled:%t err:%v", priorCount, handled, err)
		}
		loaded, err := store.LoadMiner(state.MacAddr)
		if err != nil || loaded.Phase != lib.PhaseOverheat || loaded.SafetyReason != lib.SafetyReasonMutationUncertain {
			t.Fatalf("priorCount=%d: unsafe-telemetry state = %+v, %v", priorCount, loaded, err)
		}
		if loaded.UnreadablePollCount != 0 {
			t.Fatalf("priorCount=%d: unreadable poll count was not reset by a real escalation: %d", priorCount, loaded.UnreadablePollCount)
		}
	}
}

// TestUnsupportedGridWithSafeTelemetryOnlyAdvancesCounter is the non-event case: the grid failed
// validation, there is no firmware emergency, and the assessment over whatever telemetry did
// validate is not unsafe. This must change nothing but the durable unreadable_poll_count — no phase
// transition, no ClearPendingMutation, no SetFallbackPoint.
func TestUnsupportedGridWithSafeTelemetryOnlyAdvancesCounter(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	controller := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	originalPhase := state.Phase
	info := rootTestInfo(state.CurrentPoint(), 100)
	handled, err := controller.enforceMinerSafety(context.Background(), &state, info, offGridASIC(), settings, now)
	if err != nil || !handled {
		t.Fatalf("safe-telemetry handling = handled:%t err:%v", handled, err)
	}
	loaded, err := store.LoadMiner(state.MacAddr)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.UnreadablePollCount != 1 {
		t.Fatalf("unreadable poll count = %d, want 1", loaded.UnreadablePollCount)
	}
	if loaded.Phase != originalPhase || loaded.SafetyReason != "" || loaded.PendingKind != "" {
		t.Fatalf("a non-event unreadable poll changed durable state: %+v", loaded)
	}
}

// TestUnreadablePollLimitEscalatesAfterTwelveConsecutivePolls exercises the count-based escalation
// boundary directly: eleven consecutive uninformative polls must not escalate, and the twelfth must.
func TestUnreadablePollLimitEscalatesAfterTwelveConsecutivePolls(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	controller := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	limit := unreadablePollLimit(settings)
	if limit != 12 {
		t.Fatalf("unreadablePollLimit at default settings = %d, want 12", limit)
	}
	info := rootTestInfo(state.CurrentPoint(), 100)
	for poll := 1; poll < limit; poll++ {
		at := now.Add(time.Duration(poll) * settings.MetricsTime)
		handled, err := controller.enforceMinerSafety(context.Background(), &state, info, offGridASIC(), settings, at)
		if err != nil || !handled {
			t.Fatalf("poll %d: handling = handled:%t err:%v", poll, handled, err)
		}
	}
	loaded, err := store.LoadMiner(state.MacAddr)
	if err != nil || loaded.UnreadablePollCount != limit-1 || loaded.Phase == lib.PhaseOverheat {
		t.Fatalf("after %d unreadable polls: state = %+v, %v", limit-1, loaded, err)
	}
	handled, err := controller.enforceMinerSafety(context.Background(), &state, info, offGridASIC(), settings, now.Add(time.Duration(limit)*settings.MetricsTime))
	if err != nil || !handled {
		t.Fatalf("escalating poll: handling = handled:%t err:%v", handled, err)
	}
	loaded, err = store.LoadMiner(state.MacAddr)
	if err != nil || loaded.Phase != lib.PhaseOverheat || loaded.SafetyReason != lib.SafetyReasonMutationUncertain || loaded.UnreadablePollCount != 0 {
		t.Fatalf("twelfth unreadable poll did not escalate: %+v, %v", loaded, err)
	}
}

// TestRebootInFlightSuppressesUnreadableEscalationAndStillCompletes reproduces the exact shape of
// the mineira incident this RFC diagnoses: a rollback attempt reaches restart_requested_at, the
// booting device then answers with a non-canonical ASIC grid (ordinary mid-boot behavior), and the
// unreadable-poll count must not touch the pending authority — the ledger already states a reboot is
// expected, and this counter must not contradict it. The attempt must still complete once the device
// proves its new boot.
func TestRebootInFlightSuppressesUnreadableEscalationAndStillCompletes(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	controller := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	target := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	incumbent := state.CurrentPoint()
	state.Phase = lib.PhaseCooldown
	state.HoldReason = ""
	state.SettledAt = time.Time{}
	state.SafetyReason = lib.SafetyReasonASICLimit
	state.SetPendingMutation(lib.MutationSafetyRollback, target, now)
	if _, err := store.Apply(lib.SaveState{State: state}, now); err != nil {
		t.Fatal(err)
	}
	attempt := lib.MutationAttempt{
		MacAddr: state.MacAddr, Kind: lib.MutationSafetyRollback, Reason: lib.SafetyReasonASICLimit,
		FromFrequency: incumbent.Frequency, FromCoreVoltage: incumbent.CoreVoltage,
		TargetFrequency: target.Frequency, TargetCoreVoltage: target.CoreVoltage,
		IntentCreatedAt: now, StartedAt: now, ConfiguredVerifiedUptimeSeconds: -1,
	}
	id, err := store.StartMutationAttempt(&attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, lib.MutationMilestonePatchRequested, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordConfiguredVerification(id, now.Add(2*time.Second), 101); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, lib.MutationMilestoneRestartRequested, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	// The device is now mid-reboot. Poll it several times with a non-canonical grid and safe
	// telemetry, exactly as a booting AxeOS device answers before it has finished coming up.
	bootingInfo := rootTestInfo(target, 5)
	pollAt := now.Add(3 * time.Second)
	for poll := 0; poll < 20; poll++ {
		pollAt = pollAt.Add(settings.MetricsTime)
		handled, err := controller.enforceMinerSafety(context.Background(), &state, bootingInfo, offGridASIC(), settings, pollAt)
		if err != nil || !handled {
			t.Fatalf("booting poll %d: handling = handled:%t err:%v", poll, handled, err)
		}
	}
	loaded, err := store.LoadMiner(state.MacAddr)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.UnreadablePollCount != 0 {
		t.Fatalf("reboot-in-flight suppression did not hold: unreadable poll count = %d", loaded.UnreadablePollCount)
	}
	if loaded.PendingKind != lib.MutationSafetyRollback || loaded.PendingPoint() != target {
		t.Fatalf("pending authority was disturbed by unreadable polls during reboot: %+v", loaded)
	}
	if _, unfinished, err := store.UnfinishedMutationAttempt(state.MacAddr); err != nil || !unfinished {
		t.Fatalf("mutation attempt was superseded or closed during reboot: unfinished=%t, err=%v", unfinished, err)
	}
	// The device proves its new boot and the attempt completes normally.
	if err := store.AdvanceMutationAttempt(id, lib.MutationMilestoneRebootVerified, pollAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply(lib.CompleteMutation{
		MacAddr: state.MacAddr, IP: state.IP, AttemptID: id,
	}, pollAt.Add(2*time.Second)); err != nil {
		t.Fatalf("attempt did not complete after reboot proof: %v", err)
	}
}

// TestTelemetryGapIsToleratedNotDropped is the direct inverse of the pre-cutover
// TestTelemetryGapDropsDeferredEvidence: sampling jitter is now a data-quality attribute of a
// window (tracked via maxGap, checked at admission), not a fatal event that discards the buffer.
// Only a genuine contradiction (a different point, phase, or non-advancing clock) clears it.
func TestTelemetryGapIsToleratedNotDropped(t *testing.T) {
	settings := rootTestSettings(t)
	settings.MetricsTime = time.Second
	settings.EvaluationWindowTime = 3 * time.Second
	state := lib.MinerState{
		MacAddr: rootTestMAC, CurrentFrequency: 525, CurrentCoreVoltage: 1150,
		Phase: lib.PhaseBaseline,
	}
	controller := &controller{runtimes: make(map[string]*minerRuntime)}
	asic := rootTestASIC()
	info := rootTestInfo(state.CurrentPoint(), 100)
	poll, ok := newReadablePoll(info, asic)
	if !ok {
		t.Fatal("test telemetry failed to construct a readable poll")
	}
	start := time.Now().UTC()
	if _, closed := controller.addSample(poll, state, settings, start); closed {
		t.Fatal("first sample unexpectedly completed a window")
	}
	if _, closed := controller.addSample(poll, state, settings, start.Add(5*time.Second)); closed {
		t.Fatal("gapped sample unexpectedly completed a window early")
	}
	runtime := controller.runtimeFor(state.MacAddr)
	if len(runtime.samples) != 2 {
		t.Fatalf("gap discarded evidence: samples=%d", len(runtime.samples))
	}
	if runtime.maxGap < 5*time.Second {
		t.Fatalf("gap was not recorded as a data-quality attribute: maxGap=%s", runtime.maxGap)
	}
}

// TestInvalidErrorPercentageFailsPollConstruction: the implausible-telemetry guard moved into
// newReadablePoll's fallible construction, so addSample no longer re-checks it.
func TestInvalidErrorPercentageFailsPollConstruction(t *testing.T) {
	asic := rootTestASIC()
	info := rootTestInfo(lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}, 100)
	errorPercent := 101.0
	info.ErrorPercentage = &errorPercent
	if _, ok := newReadablePoll(info, asic); ok {
		t.Fatal("invalid error percentage constructed a readable poll")
	}
}

func TestCompletionRetryRequiresTheOriginalTimestamp(t *testing.T) {
	store, _, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	target := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	state.SetPendingMutation(lib.MutationOperatingPoint, target, now)
	if _, err := store.Apply(lib.SaveState{State: state}, now); err != nil {
		t.Fatal(err)
	}
	attempt := lib.MutationAttempt{
		MacAddr: state.MacAddr, Kind: lib.MutationOperatingPoint,
		FromFrequency: 525, FromCoreVoltage: 1150,
		TargetFrequency: 525, TargetCoreVoltage: 1100,
		IntentCreatedAt: now, StartedAt: now, ConfiguredVerifiedUptimeSeconds: -1,
	}
	id, err := store.StartMutationAttempt(&attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, lib.MutationMilestonePatchRequested, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordConfiguredVerification(id, now.Add(2*time.Second), 101); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, lib.MutationMilestoneRestartRequested, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, lib.MutationMilestoneRebootVerified, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	completeResult, err := store.Apply(lib.CompleteMutation{
		MacAddr: state.MacAddr, IP: state.IP, AttemptID: id,
	}, now.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	state = completeResult.State
	state, err = store.LoadMiner(state.MacAddr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply(lib.CompleteMutation{
		MacAddr: state.MacAddr, IP: state.IP, AttemptID: id,
	}, now.Add(6*time.Second)); err == nil {
		t.Fatal("completion retry with a conflicting timestamp was accepted")
	}
}
