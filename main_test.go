package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/willyrgf/bitagnis/lib"
)

const rootTestMAC = "aa:bb:cc:dd:ee:02"

func rootTestSettings(t *testing.T) lib.Settings {
	t.Helper()
	settings, err := (lib.SettingsFile{}).ForHost("")
	if err != nil {
		t.Fatal(err)
	}
	return settings
}

func rootTestInfo(point lib.OperatingPoint, uptime int) lib.Info {
	return lib.Info{
		Version: "v2.8.1", ASICModel: "BM1370", BoardVersion: "601",
		Hostname: "root-test", MacAddr: rootTestMAC,
		Frequency: point.Frequency, CoreVoltage: point.CoreVoltage,
		CoreVoltageActual: float64(point.CoreVoltage), HashRate: 100,
		ExpectedHashRate: 100, Temp: 55, VRTemp: 70, Power: 18,
		UpTimeSeconds: uptime,
	}
}

func rootTestASIC() lib.ASICSettings {
	return lib.ASICSettings{
		ASICModel: "BM1370", DefaultFrequency: 525, DefaultVoltage: 1150,
		FrequencyOptions: []int{400, 490, 525, 550, 600, 625},
		VoltageOptions:   []int{1000, 1060, 1100, 1150, 1200, 1250},
	}
}

func TestParseArgumentsRetuneContract(t *testing.T) {
	options, err := parseArguments([]string{"--retune", "bitaxe-example"})
	if err != nil || options.retune != "bitaxe-example" || len(options.hostnames) != 1 || !options.hostnames["bitaxe-example"] {
		t.Fatalf("retune options = %+v, %v", options, err)
	}
	if _, err := parseArguments([]string{"--retune", "one", "two"}); err == nil {
		t.Fatal("retune accepted multiple hostnames")
	}
	if _, err := parseArguments([]string{"--retune", "one", "--reapply-mining"}); err == nil {
		t.Fatal("retune accepted reapply-mining")
	}
	if _, err := parseArguments([]string{"--retune"}); err == nil {
		t.Fatal("retune without hostname was accepted")
	}
	options, err = parseArguments(nil)
	if err != nil || !options.hostnames["all"] {
		t.Fatalf("default options = %+v, %v", options, err)
	}
	options, err = parseArguments([]string{"--report", "one-arm", "treatment", "control", "2026-08-01T00:00:00Z"})
	if err != nil || options.reportMode != "one-arm" || len(options.reportHosts) != 2 || len(options.reportStarts) != 1 || len(options.hostnames) != 0 {
		t.Fatalf("report options = %+v, %v", options, err)
	}
	for _, arguments := range [][]string{
		{"--report"},
		{"--report", "unknown", "one", "two", "2026-08-01T00:00:00Z"},
		{"--report", "one", "two"},
		{"--report", "one-arm", "one", "two", "2026-08-01T00:30:00Z"},
		{"--report", "one-arm", "one", "two", "2026-08-01T00:00:00Z", "extra"},
		{"--report", "one-arm", "one", "two", "2026-08-01T00:00:00Z", "--retune", "two"},
		{"--report", "one-arm", "one", "two", "2026-08-01T00:00:00Z", "--reapply-mining", "two"},
	} {
		if _, err := parseArguments(arguments); err == nil {
			t.Fatalf("invalid report arguments accepted: %v", arguments)
		}
	}
}

func TestCanonicalGridIsTheOnlyAutomationAuthority(t *testing.T) {
	if err := canonicalASICGrid(rootTestASIC()); err != nil {
		t.Fatal(err)
	}
	grid := rootTestASIC()
	grid.VoltageOptions = []int{1000, 1050, 1100, 1150, 1200, 1250}
	if err := canonicalASICGrid(grid); err == nil {
		t.Fatal("off-grid advertised values were accepted")
	}
}

func TestCanonicalFrontierHasExactStructuralRestartBound(t *testing.T) {
	asic := rootTestASIC()
	pairs := len(asic.FrequencyOptions) * len(asic.VoltageOptions)
	if pairs != 36 {
		t.Fatalf("canonical pair count = %d", pairs)
	}
	if normalRestarts := 2 * (pairs - 1); normalRestarts != 70 {
		t.Fatalf("normal restart bound = %d", normalRestarts)
	}
}

func TestSafetyThresholdsRemainDistinct(t *testing.T) {
	settings := rootTestSettings(t)
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	live := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	info := rootTestInfo(live, 100)
	info.Temp = settings.TempLimit
	if assessment := assessInstantaneousSafety(info, settings, live, minimum); assessment.action != safetyNormal {
		t.Fatalf("temperature at hard limit was not normal: %+v", assessment)
	}
	info.Temp = settings.TempLimit + .01
	if assessment := assessInstantaneousSafety(info, settings, live, minimum); assessment.action != safetyRollback {
		t.Fatalf("temperature above hard limit did not roll back: %+v", assessment)
	}
	info.Temp = settings.TempCutoff
	if assessment := assessInstantaneousSafety(info, settings, live, minimum); assessment.action != safetyHostContainment {
		t.Fatalf("temperature at host cutoff did not contain: %+v", assessment)
	}
	info.Temp = axeOSASICTripTemp + .01
	if assessment := assessInstantaneousSafety(info, settings, live, minimum); assessment.action != safetyEmergencyHold {
		t.Fatalf("temperature above firmware trip did not hold emergency: %+v", assessment)
	}
	info = rootTestInfo(live, 100)
	info.Power = settings.MaxPower
	if assessment := assessInstantaneousSafety(info, settings, live, minimum); assessment.action != safetyRollback {
		t.Fatalf("power at maximum did not roll back: %+v", assessment)
	}
	info = rootTestInfo(live, 100)
	info.VRTemp = settings.VRTempHigh
	if assessment := assessInstantaneousSafety(info, settings, live, minimum); assessment.action != safetyRollback {
		t.Fatalf("VR temperature at maximum did not roll back: %+v", assessment)
	}
}

func TestConservativeWindowSummaryAndFixedFinalSelection(t *testing.T) {
	settings := rootTestSettings(t)
	firstError := 1.0
	secondError := 4.0
	first := windowSummary{MedianHash: 110, ExpectedHash: 110, MeanTemp: 55, P95Temp: 55, P95VRTemp: 70, P95Power: 18, ErrorPercent: &firstError}
	second := windowSummary{MedianHash: 100, ExpectedHash: 120, MeanTemp: 60, P95Temp: 64, P95VRTemp: 80, P95Power: 22, ErrorPercent: &secondError}
	combined, err := combineWindowSummaries(first, second)
	if err != nil || combined.MedianHash != 100 || combined.ExpectedHash != 120 || combined.P95Temp != 64 || combined.P95Power != 22 || combined.ErrorPercent == nil || *combined.ErrorPercent != 4 {
		t.Fatalf("conservative summary = %+v, %v", combined, err)
	}
	asic := rootTestASIC()
	points := []lib.OperatingPointRecord{
		rootRecord(rootTestMAC, lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}, 100, 60, 18, 70),
		rootRecord(rootTestMAC, lib.OperatingPoint{Frequency: 550, CoreVoltage: 1100}, 99, 55, 17, 65),
		rootRecord(rootTestMAC, lib.OperatingPoint{Frequency: 490, CoreVoltage: 1000}, 97, 50, 16, 60),
	}
	selected, ok := selectFinalPoint(points, asic, settings)
	if !ok || selected.Frequency != 550 || selected.CoreVoltage != 1100 {
		t.Fatalf("fixed-anchor selection = %+v, %t", selected, ok)
	}
}

func TestFinalPlacementKeepsExactMaximumSeparateFromSelectedPoint(t *testing.T) {
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	records := []lib.OperatingPointRecord{
		rootRecord(rootTestMAC, lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}, 100, 60, 18, 70),
		rootRecord(rootTestMAC, lib.OperatingPoint{Frequency: 550, CoreVoltage: 1100}, 99, 55, 17, 65),
	}
	best, bestOK := selectBestPoint(records, asic, settings)
	selected, selectedOK := selectFinalPoint(records, asic, settings)
	if !bestOK || !selectedOK || best.MedianHash != 100 || best.Point() != (lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}) || selected.Point() != (lib.OperatingPoint{Frequency: 550, CoreVoltage: 1100}) {
		t.Fatalf("best/final placement = %+v/%+v (%t/%t)", best, selected, bestOK, selectedOK)
	}
}

func TestTrialPredicatesApplyToEachWindow(t *testing.T) {
	prior := rootRecord(rootTestMAC, lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}, 100, 60, 18, 70)
	good := windowSummary{MedianHash: 103, ExpectedHash: 100}
	badPerformance := windowSummary{MedianHash: 101, ExpectedHash: 100}
	if !trialWindowPredicate(lib.PhaseFrequencyTest, good, prior, 100) || trialWindowPredicate(lib.PhaseFrequencyTest, badPerformance, prior, 100) {
		t.Fatal("performance predicate did not distinguish individual windows")
	}
	prior.P95Temp = 60
	prior.P95Power = 18
	prior.P95VRTemp = 70
	goodUndervolt := good
	goodUndervolt.P95Temp = 55
	goodUndervolt.P95Power = 17
	goodUndervolt.P95VRTemp = 65
	badUndervolt := goodUndervolt
	badUndervolt.MedianHash = 97
	if !trialWindowPredicate(lib.PhaseUndervolt, goodUndervolt, prior, 100) || trialWindowPredicate(lib.PhaseUndervolt, badUndervolt, prior, 100) {
		t.Fatal("undervolt predicate did not distinguish individual windows")
	}
}

func TestLongTermReportFormattingIsDeterministicAndCredentialFree(t *testing.T) {
	from := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	report := lib.ArmReport{
		Window:        lib.ReportWindow{Start: from, End: from.Add(lib.ReportArmDuration)},
		TreatmentHost: "treatment-host",
		ControlHost:   "control-host",
		Treatment: lib.ReportMinerMetrics{
			Coverage: .99, ObservedSeconds: 600, UnknownGapSeconds: 6,
			ActualHashSeconds: 123456, NormalizedWork: 1.03, SettledSeconds: 540,
			TrialSeconds: 60, PreArmSettledHashRate: 100,
			Restart: lib.RestartExposure{NormalRequests: 2, NormalExposureSeconds: 31, SafetyRequests: 1, SafetyExposureSeconds: 12, UnresolvedAttempts: 0},
		},
		Control: lib.ReportMinerMetrics{
			Coverage: .98, ObservedSeconds: 590, UnknownGapSeconds: 16,
			ActualHashSeconds: 120000, NormalizedWork: 1,
			PreArmSettledHashRate: 100,
			Restart:               lib.RestartExposure{NormalRequests: 1, NormalExposureSeconds: 14},
		},
		Uplift: .03, Valid: true,
	}
	var first, second bytes.Buffer
	formatArmReport(&first, report)
	formatArmReport(&second, report)
	if first.String() != second.String() || !strings.Contains(first.String(), "result: VALID") || !strings.Contains(first.String(), "2026-08-01T00:00:00Z") {
		t.Fatalf("report formatting is not deterministic:\n%s", first.String())
	}
	if strings.Contains(first.String(), "192.0.2.") || strings.Contains(first.String(), "aa:bb:") || strings.Contains(first.String(), "password") {
		t.Fatalf("report formatting exposed sensitive device data:\n%s", first.String())
	}
}

func TestMutationOverlapsReportWindowUsesHealthyOrFailureBoundary(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	window := lib.ReportWindow{Start: start, End: start.Add(24 * time.Hour)}
	finishedBefore := lib.MutationAttempt{
		StartedAt: start.Add(-2 * time.Hour),
		FailedAt:  start.Add(-time.Minute),
	}
	if mutationOverlapsReportWindow(finishedBefore, window) {
		t.Fatal("mutation failed before the arm was classified as overlapping")
	}
	resumedDuring := lib.MutationAttempt{
		StartedAt:       start.Add(-2 * time.Hour),
		CompletedAt:     start.Add(-time.Hour),
		MiningResumedAt: start.Add(time.Hour),
	}
	if !mutationOverlapsReportWindow(resumedDuring, window) {
		t.Fatal("mutation resuming during the arm was not classified as overlapping")
	}
	unfinished := lib.MutationAttempt{StartedAt: start.Add(-time.Hour)}
	if !mutationOverlapsReportWindow(unfinished, window) {
		t.Fatal("unfinished mutation was not classified as overlapping")
	}
}

func TestHourlyFragmentsSplitUTCAndClassifyTrials(t *testing.T) {
	state := lib.MinerState{
		MacAddr: rootTestMAC, Hostname: "root-test", IP: "192.0.2.12",
		Phase: lib.PhaseFrequencyTest, PhaseStartedAt: time.Now().UTC(),
		CurrentFrequency: 550, CurrentCoreVoltage: 1100,
		FallbackFrequency: 525, FallbackCoreVoltage: 1150,
		PassStartedAt: time.Now().UTC(), PassTrigger: lib.PassInitial,
		AccountedThroughAt: time.Now().UTC(), PassReferenceHash: 100,
	}
	sample := accountingSample{point: state.CurrentPoint(), phase: state.Phase, referenceHash: 100, hashRate: 105, validHash: true, state: state}
	start := time.Date(2026, 8, 1, 11, 30, 0, 0, time.UTC)
	end := time.Date(2026, 8, 1, 13, 15, 0, 0, time.UTC)
	fragments := hourlyFragments(rootTestMAC, start, end, sample, true)
	if len(fragments) != 3 || fragments[0].ObservedSeconds != 1800 || fragments[1].ObservedSeconds != 3600 || fragments[2].ObservedSeconds != 900 {
		t.Fatalf("hourly split = %+v", fragments)
	}
	if fragments[0].TrialSeconds != 1800 || fragments[0].IncumbentCounterfactualHashSeconds != 180000 {
		t.Fatalf("trial classification = %+v", fragments[0])
	}
	unknown := hourlyFragments(rootTestMAC, start, end, sample, false)
	if unknown[0].UnknownGapSeconds != 1800 || unknown[0].ActualHashSeconds != 0 {
		t.Fatalf("unknown classification = %+v", unknown[0])
	}
}

func TestBaselineEvidenceDeadlineTerminalizesBootstrapRow(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	state, _, err := store.BootstrapMiner(rootTestInfo(lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}, 100), "192.0.2.12", now, settings.RampUpTime, settings.EvaluationWindowTime, true)
	if err != nil {
		t.Fatal(err)
	}
	state.EvidenceDeadlineAt = now.Add(-time.Second)
	state.RampUntil = now.Add(-2 * time.Second)
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	if err := minerController.handleEvidenceDeadline(&state, settings, now); err != nil {
		t.Fatal(err)
	}
	points, err := store.ListPoints(rootTestMAC)
	if err != nil || len(points) != 1 || points[0].Status != lib.PointUnobservable {
		t.Fatalf("expired baseline = %+v, %v", points, err)
	}
	if state.Phase != lib.PhaseHold || state.HoldReason != lib.HoldBlocked {
		t.Fatalf("expired baseline state = %+v", state)
	}
}

func rootRecord(mac string, point lib.OperatingPoint, hash, temp, power, vr float64) lib.OperatingPointRecord {
	return lib.OperatingPointRecord{
		MacAddr: mac, Frequency: point.Frequency, CoreVoltage: point.CoreVoltage,
		Status: lib.PointValidated, MedianHash: hash, ExpectedHash: hash,
		Attainment: 1, MeanTemp: temp, P95Temp: temp, P95VRTemp: vr, P95Power: power,
		EnteredAt: time.Now().UTC().Add(-time.Hour), MeasuredAt: time.Now().UTC(), ReferenceHash: hash,
	}
}

type scriptedMutationDevice struct {
	asic         lib.ASICSettings
	source       lib.Info
	target       lib.Info
	patched      bool
	restarted    bool
	patchCount   int
	restartCount int
	restartErr   error
}

func (device *scriptedMutationDevice) GetSystemInfo(context.Context, string) (lib.Info, error) {
	if device.patched {
		return device.target, nil
	}
	return device.source, nil
}

func (device *scriptedMutationDevice) GetASICSettings(context.Context, string) (lib.ASICSettings, error) {
	return device.asic, nil
}

func (device *scriptedMutationDevice) PatchOperatingPoint(context.Context, lib.OperatingPoint, string) error {
	device.patched = true
	device.patchCount++
	return nil
}

func (device *scriptedMutationDevice) PatchOverheatRecovery(context.Context, lib.OperatingPoint, string) error {
	return device.PatchOperatingPoint(context.Background(), lib.OperatingPoint{}, "")
}

func (device *scriptedMutationDevice) PatchMiningConfiguration(context.Context, lib.MiningSettings, string, string, string) error {
	device.patched = true
	device.patchCount++
	return nil
}

func (device *scriptedMutationDevice) Restart(context.Context, string) error {
	device.restarted = true
	device.restartCount++
	return device.restartErr
}

func TestMutationUsesConfiguredReadbackBeforeOneRestart(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settingsFile := lib.SettingsFile{}
	settings, err := settingsFile.ForHost("")
	if err != nil {
		t.Fatal(err)
	}
	sourcePoint := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	targetPoint := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	now := time.Now().UTC()
	state, _, err := store.BootstrapMiner(rootTestInfo(sourcePoint, 100), "192.0.2.12", now, settings.RampUpTime, settings.EvaluationWindowTime, true)
	if err != nil {
		t.Fatal(err)
	}
	state.SetPendingMutation(lib.MutationOperatingPoint, targetPoint, now)
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	device := &scriptedMutationDevice{asic: rootTestASIC(), source: rootTestInfo(sourcePoint, 100), target: rootTestInfo(targetPoint, 100), restartErr: errors.New("restart response lost")}
	coordinator := newMutationCoordinator(
		device, store, settingsFile, []lib.DiscoveredMiner{{IP: "192.0.2.12", Info: device.source}}, nil, "",
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			postBoot := device.target
			postBoot.UpTimeSeconds = 1
			return lib.DiscoveredMiner{IP: "192.0.2.12", Info: postBoot}, nil
		}, nil, log.New(io.Discard, "", 0), nil,
	)
	coordinator.rediscoveryDelay = time.Millisecond
	coordinator.rebootDeadline = time.Second
	observation := &minerObservation{miner: lib.DiscoveredMiner{IP: "192.0.2.12", Info: device.source}, info: device.source, asic: device.asic, settings: settings, state: state}
	if err := coordinator.startLocked(context.Background(), observation, "", ""); err != nil {
		t.Fatal(err)
	}
	result := <-coordinator.results
	if result.err != nil {
		t.Fatalf("mutation result: %v", result.err)
	}
	if err := coordinator.completeMutationLocked(result); err != nil {
		t.Fatal(err)
	}
	if device.patchCount != 1 || device.restartCount != 1 {
		t.Fatalf("hardware requests patch/restart = %d/%d", device.patchCount, device.restartCount)
	}
	attempts, err := store.ListMutationAttempts(rootTestMAC)
	if err != nil || len(attempts) != 1 || attempts[0].ConfiguredVerifiedAt.IsZero() || attempts[0].RebootVerifiedAt.IsZero() || attempts[0].CompletedAt.IsZero() {
		t.Fatalf("mutation milestones = %+v, %v", attempts, err)
	}
}

func TestMutationReadbackDistinguishesUnavailableFromUnsafeMismatch(t *testing.T) {
	settings := rootTestSettings(t)
	target := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	request := mutationRequest{
		kind: lib.MutationOperatingPoint, point: target,
		settings: settings,
	}
	if coordinator := (&mutationCoordinator{}); coordinator.readbackNeedsSafetySupersession(request, lib.Info{}, rootTestASIC()) {
		t.Fatal("empty readback was treated as readable safety evidence")
	}
	wrong := rootTestInfo(lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}, 100)
	if err := verifyConfiguredReadbackV4(request, wrong, false); err == nil {
		t.Fatal("wrong configured pair was accepted")
	}
	unsafe := rootTestInfo(target, 100)
	unsafe.Temp = settings.TempLimit + 1
	if err := verifyConfiguredReadbackV4(request, unsafe, false); err == nil {
		t.Fatal("unsafe configured pair was accepted")
	}
}

func TestMutationReadbackMismatchSupersedesBeforeRestart(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settingsFile := lib.SettingsFile{}
	settings, err := settingsFile.ForHost("")
	if err != nil {
		t.Fatal(err)
	}
	sourcePoint := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	wantedPoint := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	wrongPoint := lib.OperatingPoint{Frequency: 490, CoreVoltage: 1000}
	now := time.Now().UTC()
	state, _, err := store.BootstrapMiner(rootTestInfo(sourcePoint, 100), "192.0.2.12", now, settings.RampUpTime, settings.EvaluationWindowTime, true)
	if err != nil {
		t.Fatal(err)
	}
	state.SetPendingMutation(lib.MutationOperatingPoint, wantedPoint, now)
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	device := &scriptedMutationDevice{asic: rootTestASIC(), source: rootTestInfo(sourcePoint, 100), target: rootTestInfo(wrongPoint, 100)}
	coordinator := newMutationCoordinator(
		device, store, settingsFile, []lib.DiscoveredMiner{{IP: "192.0.2.12", Info: device.source}}, nil, "",
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		nil, log.New(io.Discard, "", 0), nil,
	)
	coordinator.rediscoveryDelay = time.Millisecond
	observation := &minerObservation{miner: lib.DiscoveredMiner{IP: "192.0.2.12", Info: device.source}, info: device.source, asic: device.asic, settings: settings, state: state}
	if err := coordinator.startLocked(context.Background(), observation, "", ""); err != nil {
		t.Fatal(err)
	}
	result := <-coordinator.results
	if result.err == nil || result.failureStage == "" {
		t.Fatalf("mismatch result = id=%d attempt=%d failure=%q unavailable=%t err=%v", result.id, result.attemptID, result.failureStage, result.readbackUnavailable, result.err)
	}
	coordinator.results <- result
	if _, err := coordinator.applyResultsLocked(); err != nil {
		t.Fatal(err)
	}
	if device.restartCount != 0 {
		t.Fatalf("mismatch issued restart count %d", device.restartCount)
	}
	attempts, err := store.ListMutationAttempts(rootTestMAC)
	if err != nil || len(attempts) != 1 || attempts[0].FailureStage != lib.MutationFailureSafetySuperseded {
		t.Fatalf("mismatch attempt = %+v, %v", attempts, err)
	}
	loaded, err := store.LoadMiner(rootTestMAC)
	if err != nil || loaded.Phase != lib.PhaseOverheat || loaded.SafetyReason == "" {
		t.Fatalf("mismatch safety state = %+v, %v", loaded, err)
	}
}

func TestLostRestartResponseCanStillProveAReboot(t *testing.T) {
	if !proveNewBoot(120, 1, 30*time.Second) {
		t.Fatal("uptime discontinuity did not prove reboot")
	}
	if proveNewBoot(120, 150, 30*time.Second) {
		t.Fatal("continuous uptime was accepted as reboot proof")
	}
}

func TestRedactMutationErrorNeverLeaksPassword(t *testing.T) {
	err := redactMutationError(errors.New("secret-bearing value"), "secret-bearing value")
	if err == nil || err.Error() == "secret-bearing value" {
		t.Fatal("secret was not redacted")
	}
}
