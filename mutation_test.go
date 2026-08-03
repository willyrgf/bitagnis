package main

import (
	"context"
	"io"
	"log"
	"path/filepath"
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
	state, _, err := store.BootstrapMiner(
		rootTestInfo(lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}, 100),
		"192.0.2.12", now, settings.RampUpTime, settings.EvaluationWindowTime, true,
	)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, settings, state, now
}

func TestWrongMACAfterPatchIsQuarantinedWithoutActuation(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	target := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	state.SetPendingMutation(lib.MutationOperatingPoint, target, now)
	if err := store.SaveMiner(&state); err != nil {
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
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	state.Phase = lib.PhaseCooldown
	state.SafetyReason = lib.SafetyReasonASICLimit
	state.SetPendingMutation(lib.MutationSafetyRollback, minimum, now)
	state.CooldownUntil = now
	if err := store.SaveMiner(&state); err != nil {
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

func TestRetuneRequiresVerifiedFinalSelectionAndElapsedRamp(t *testing.T) {
	store, _, state, now := newRootMutationStore(t)
	record := rootRecord(rootTestMAC, state.CurrentPoint(), 100, 55, 18, 70)
	record.EnteredAt = now
	record.MeasuredAt = now.Add(time.Minute)
	record.ReferenceHash = 0
	if err := store.FinalizeBaseline(&state, record, false, record.MeasuredAt); err != nil {
		t.Fatal(err)
	}
	state.Phase = lib.PhaseHold
	state.HoldReason = lib.HoldOptimized
	state.SettledAt = now.Add(-time.Second)
	state.RampUntil = now.Add(-time.Minute)
	state.EvidenceDeadlineAt = time.Time{}
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	settings := rootTestSettings(t)
	info := rootTestInfo(state.CurrentPoint(), 100)
	asic := rootTestASIC()
	if !qualifiesSettledObservation(store, state, info, asic, settings, now, true) {
		t.Fatal("verified optimized hold was rejected")
	}
	state.RampUntil = now.Add(time.Minute)
	if qualifiesSettledObservation(store, state, info, asic, settings, now, true) {
		t.Fatal("retune qualification ignored an active ramp")
	}
	state.RampUntil = now.Add(-time.Minute)
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

func TestRetuneAcceptsSettledSafetyHoldAfterTwoHealthyPolls(t *testing.T) {
	store, _, state, now := newRootMutationStore(t)
	state.Phase = lib.PhaseHold
	state.HoldReason = lib.HoldSafety
	state.SafetyReason = lib.SafetyReasonASICLimit
	state.SettledAt = now.Add(-time.Second)
	state.RampUntil = now.Add(-time.Minute)
	state.EvidenceDeadlineAt = time.Time{}
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	settings := rootTestSettings(t)
	info := rootTestInfo(state.CurrentPoint(), 100)
	asic := rootTestASIC()
	coordinator := newMutationCoordinator(
		nil, store, lib.SettingsFile{}, []lib.DiscoveredMiner{{IP: state.IP, Info: info}}, nil,
		state.Hostname, nil, nil, log.New(io.Discard, "", 0), nil,
	)
	coordinator.retuneHost = state.Hostname
	observations := map[string]*minerObservation{
		state.MacAddr: {miner: lib.DiscoveredMiner{IP: state.IP, Info: info}, info: info, asic: asic, settings: settings, state: state},
	}
	accepted, err := coordinator.advanceRetuneLocked(observations, now)
	if err != nil || accepted {
		t.Fatalf("first safety retune poll = accepted:%t err:%v", accepted, err)
	}
	accepted, err = coordinator.advanceRetuneLocked(observations, now.Add(time.Second))
	if err != nil || !accepted {
		t.Fatalf("second safety retune poll = accepted:%t err:%v", accepted, err)
	}
	loaded, err := store.LoadMiner(state.MacAddr)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Phase != lib.PhaseBaseline || loaded.SafetyReason != "" || loaded.PassTrigger != lib.PassOperator {
		t.Fatalf("accepted safety retune state = %+v", loaded)
	}
}

func TestOffGridManualObservationRequiresTwoPolls(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	record := rootRecord(rootTestMAC, state.CurrentPoint(), 100, 55, 18, 70)
	record.EnteredAt = now
	record.MeasuredAt = now.Add(time.Minute)
	record.ReferenceHash = 0
	if err := store.FinalizeBaseline(&state, record, false, record.MeasuredAt); err != nil {
		t.Fatal(err)
	}
	state.Phase = lib.PhaseHold
	state.HoldReason = lib.HoldOptimized
	state.SettledAt = now
	state.RampUntil = now.Add(-time.Minute)
	state.EvidenceDeadlineAt = time.Time{}
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	controller := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	offGrid := lib.OperatingPoint{Frequency: 500, CoreVoltage: 1000}
	if err := controller.observeExternalPoint(&state, offGrid, rootTestASIC(), settings, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if state.CurrentPoint() == offGrid || state.ObservedCount != 1 {
		t.Fatalf("first off-grid observation = %+v", state)
	}
	if err := controller.observeExternalPoint(&state, offGrid, rootTestASIC(), settings, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if state.CurrentPoint() != offGrid || state.Phase != lib.PhaseHold || state.HoldReason != lib.HoldBlocked {
		t.Fatalf("second off-grid observation = %+v", state)
	}
}

func TestPostRediscoveryASICReadUsesTheRediscoveredIP(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	target := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	state.SetPendingMutation(lib.MutationOperatingPoint, target, now)
	if err := store.SaveMiner(&state); err != nil {
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

func TestTelemetryGapDropsDeferredEvidence(t *testing.T) {
	settings := rootTestSettings(t)
	settings.MetricsTime = time.Second
	settings.EvaluationWindowTime = 2 * time.Second
	state := lib.MinerState{
		MacAddr: rootTestMAC, CurrentFrequency: 525, CurrentCoreVoltage: 1150,
		Phase: lib.PhaseBaseline,
	}
	controller := &controller{runtimes: make(map[string]*minerRuntime)}
	info := rootTestInfo(state.CurrentPoint(), 100)
	start := time.Now().UTC()
	if _, ready := controller.addSample(state.MacAddr, info, state, settings, start); ready {
		t.Fatal("first sample unexpectedly completed a window")
	}
	if _, ready := controller.addSample(state.MacAddr, info, state, settings, start.Add(time.Second)); !ready {
		t.Fatal("second sample did not complete a window")
	}
	runtime := controller.runtimeFor(state.MacAddr)
	runtime.deferredWindows = []windowSummary{{MedianHash: 100}}
	if _, ready := controller.addSample(state.MacAddr, info, state, settings, start.Add(3*time.Second)); ready {
		t.Fatal("gapped sample completed a continuous window")
	}
	if len(runtime.deferredWindows) != 0 || len(runtime.samples) != 1 {
		t.Fatalf("gap retained evidence: deferred=%d samples=%d", len(runtime.deferredWindows), len(runtime.samples))
	}
}

func TestCompletionRetryRequiresTheOriginalTimestamp(t *testing.T) {
	store, _, state, now := newRootMutationStore(t)
	target := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	state.SetPendingMutation(lib.MutationOperatingPoint, target, now)
	if err := store.SaveMiner(&state); err != nil {
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
	if err := store.CompleteMutationAttempt(&state, id, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	state, err = store.LoadMiner(state.MacAddr)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteMutationAttempt(&state, id, now.Add(6*time.Second)); err == nil {
		t.Fatal("completion retry with a conflicting timestamp was accepted")
	}
}
