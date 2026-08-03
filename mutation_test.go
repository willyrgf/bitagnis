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
