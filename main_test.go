package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"math"
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
		{"--report", "one-arm", "one", "two", "2026-08-01T00:30:00-03:00"},
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

func TestSafetyRenderingDistinguishesFirmwareContainmentAndVerification(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	settings := rootTestSettings(t)
	controller := &controller{}
	state := lib.MinerState{Phase: lib.PhaseEmergency, PhaseStartedAt: now.Add(-time.Minute)}
	info := rootTestInfo(lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}, 100)
	tests := []struct {
		name       string
		reason     lib.SafetyReason
		pending    lib.MutationKind
		phase      lib.OptimizerPhase
		wantState  string
		wantWindow string
	}{
		{name: "firmware", reason: lib.SafetyReasonFirmwareOverheat, phase: lib.PhaseEmergency, wantState: "AXEOS", wantWindow: "firmware cool"},
		{name: "containment", reason: lib.SafetyReasonHostCutoff, pending: lib.MutationSafetyRollback, phase: lib.PhaseEmergency, wantState: "CONTAIN", wantWindow: "minimum"},
		{name: "verification", reason: lib.SafetyReasonTelemetryUnavailable, phase: lib.PhaseEmergency, wantState: "VERIFY", wantWindow: "verify"},
		{name: "ordinary backoff", reason: lib.SafetyReasonASICLimit, pending: lib.MutationSafetyRollback, phase: lib.PhaseCooldown, wantState: "BACKOFF", wantWindow: "backoff"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			current := state
			current.Phase = testCase.phase
			current.SafetyReason = testCase.reason
			if testCase.pending != "" {
				current.SetPendingMutation(testCase.pending, lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}, now)
			}
			if got := formatState(current, info, now); !strings.Contains(got, testCase.wantState) {
				t.Fatalf("state label = %q, want %q", got, testCase.wantState)
			}
			if got := controller.formatWindow(current, settings, now); !strings.Contains(got, testCase.wantWindow) {
				t.Fatalf("window label = %q, want %q", got, testCase.wantWindow)
			}
		})
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

func TestRollbackSelectsClosestValidatedPairThatLowersBothComponents(t *testing.T) {
	settings := rootTestSettings(t)
	failed := lib.OperatingPoint{Frequency: 625, CoreVoltage: 1200}
	records := []lib.OperatingPointRecord{
		rootRecord(rootTestMAC, lib.OperatingPoint{Frequency: 625, CoreVoltage: 1150}, 130, 55, 18, 70),
		rootRecord(rootTestMAC, lib.OperatingPoint{Frequency: 600, CoreVoltage: 1150}, 80, 55, 18, 70),
		rootRecord(rootTestMAC, lib.OperatingPoint{Frequency: 550, CoreVoltage: 1100}, 140, 55, 18, 70),
	}
	want := lib.OperatingPoint{Frequency: 600, CoreVoltage: 1150}
	got, found := selectRollbackPoint(records, failed, rootTestASIC(), settings)
	if !found || got != want {
		t.Fatalf("rollback target = %+v found:%t, want %+v", got, found, want)
	}
}

func TestEmergencyDispositionUsesLiveAndDurableRecoveryAuthority(t *testing.T) {
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	aboveMinimum := lib.OperatingPoint{Frequency: 490, CoreVoltage: 1060}
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	priorPending := now.Add(-time.Minute)

	type emergencyCase struct {
		name             string
		reason           lib.SafetyReason
		current          lib.OperatingPoint
		live             lib.OperatingPoint
		pending          lib.MutationKind
		pendingSince     time.Time
		polls            int
		configure        func(*lib.Info)
		wantPhase        lib.OptimizerPhase
		wantReason       lib.SafetyReason
		wantPending      lib.MutationKind
		wantPendingSince time.Time
	}
	tests := []emergencyCase{
		{
			name: "firmware owns powered-down cooling", current: aboveMinimum, live: lib.OperatingPoint{Frequency: 50, CoreVoltage: 1000},
			configure: func(info *lib.Info) { info.OverHeatMode, info.Temp, info.HashRate = 1, -1, 0 },
			wantPhase: lib.PhaseEmergency, wantReason: lib.SafetyReasonFirmwareOverheat,
		},
		{
			name: "active firmware marker remains firmware-owned", current: aboveMinimum, live: aboveMinimum,
			configure: func(info *lib.Info) { info.OverHeatMode = 1 },
			wantPhase: lib.PhaseEmergency, wantReason: lib.SafetyReasonFirmwareOverheat,
		},
		{
			name: "firmware episode already canonical", reason: lib.SafetyReasonFirmwareOverheat,
			current: minimum, live: minimum, wantPhase: lib.PhaseCooldown,
			wantReason: lib.SafetyReasonFirmwareOverheat,
		},
		{
			name: "production uncertain state already canonical", reason: lib.SafetyReasonTelemetryUnavailable,
			current: minimum, live: minimum, wantPhase: lib.PhaseCooldown,
			wantReason: lib.SafetyReasonTelemetryUnavailable,
		},
		{
			name: "firmware changed point behind durable state", reason: lib.SafetyReasonFirmwareOverheat,
			current: aboveMinimum, live: minimum, polls: 2, wantPhase: lib.PhaseCooldown,
			wantReason: lib.SafetyReasonFirmwareOverheat,
		},
		{
			name: "firmware reduction below grid normalizes to minimum", reason: lib.SafetyReasonFirmwareOverheat,
			current:   lib.OperatingPoint{Frequency: 390, CoreVoltage: 900},
			live:      lib.OperatingPoint{Frequency: 390, CoreVoltage: 900},
			wantPhase: lib.PhaseEmergency, wantReason: lib.SafetyReasonFirmwareOverheat,
			wantPending: lib.MutationFirmwareRecovery, wantPendingSince: now,
		},
		{
			name: "unsafe exact minimum is mutation free", reason: lib.SafetyReasonTelemetryUnavailable,
			current: minimum, live: minimum,
			configure: func(info *lib.Info) { info.Temp = settings.TempCutoff },
			wantPhase: lib.PhaseEmergency, wantReason: lib.SafetyReasonHostCutoff,
		},
		{
			name: "safe verified point retires false containment authority", reason: lib.SafetyReasonTelemetryUnavailable,
			current: aboveMinimum, live: aboveMinimum, pending: lib.MutationSafetyRollback,
			pendingSince: priorPending, wantPhase: lib.PhaseCooldown,
			wantReason: lib.SafetyReasonTelemetryUnavailable,
		},
		{
			name: "firmware fact supersedes rollback", reason: lib.SafetyReasonTelemetryUnavailable,
			current: aboveMinimum, live: aboveMinimum, pending: lib.MutationSafetyRollback,
			pendingSince: priorPending, configure: func(info *lib.Info) { info.OverHeatMode = 1 },
			wantPhase: lib.PhaseEmergency, wantReason: lib.SafetyReasonFirmwareOverheat,
		},
	}
	for _, reason := range []lib.SafetyReason{
		lib.SafetyReasonASICLimit,
		lib.SafetyReasonHostCutoff,
		lib.SafetyReasonFirmwareTrip,
		lib.SafetyReasonPowerLimit,
		lib.SafetyReasonVRLimit,
	} {
		tests = append(tests, emergencyCase{
			name: "settled minimum retires " + string(reason), reason: reason,
			current: minimum, live: minimum, wantPhase: lib.PhaseCooldown, wantReason: reason,
		})
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			state := lib.MinerState{
				MacAddr: rootTestMAC, Hostname: "root-test", Phase: lib.PhaseEmergency,
				PhaseStartedAt: now.Add(-time.Hour), SafetyReason: testCase.reason, EmergencyCount: 1,
			}
			state.SetCurrentPoint(testCase.current)
			if testCase.pending != "" {
				state.SetPendingMutation(testCase.pending, minimum, testCase.pendingSince)
			}
			info := rootTestInfo(testCase.live, 100)
			if testCase.configure != nil {
				testCase.configure(&info)
			}
			polls := max(1, testCase.polls)
			for poll := 0; poll < polls; poll++ {
				assessment := assessInstantaneousSafety(info, settings, testCase.live, minimum)
				if _, err := transitionEmergencyState(&state, info, asic, settings, now, assessment); err != nil {
					t.Fatal(err)
				}
			}
			if state.Phase != testCase.wantPhase || state.SafetyReason != testCase.wantReason ||
				state.PendingKind != testCase.wantPending || !state.PendingSince.Equal(testCase.wantPendingSince) {
				t.Fatalf("disposition = phase:%s reason:%s pending:%s since:%s; want phase:%s reason:%s pending:%s since:%s",
					state.Phase, state.SafetyReason, state.PendingKind, state.PendingSince,
					testCase.wantPhase, testCase.wantReason, testCase.wantPending, testCase.wantPendingSince)
			}
		})
	}
}

// mustWindow builds a lib.WindowAggregate through its one constructor, failing the test if the
// synthetic evidence supplied is itself invalid.
func mustWindow(t *testing.T, medianHash, expectedHash, meanTemp, p95Temp, p95VRTemp, p95Power float64, errorPercent *float64) lib.WindowAggregate {
	t.Helper()
	attainment := 0.0
	if expectedHash > 0 {
		attainment = medianHash / expectedHash
	}
	aggregate, err := lib.NewWindowAggregate(30, 5*time.Minute, medianHash, expectedHash, attainment, meanTemp, p95Temp, p95VRTemp, p95Power, errorPercent, 0, 0)
	if err != nil {
		t.Fatalf("build window aggregate: %v", err)
	}
	return aggregate
}

func TestConservativeWindowSummaryAndFixedFinalSelection(t *testing.T) {
	settings := rootTestSettings(t)
	firstError := 1.0
	secondError := 4.0
	first := mustWindow(t, 110, 110, 55, 55, 70, 18, &firstError)
	second := mustWindow(t, 100, 120, 60, 64, 80, 22, &secondError)
	combined, err := first.Combine(second)
	if err != nil || combined.MedianHash() != 100 || combined.ExpectedHash() != 120 || combined.P95Temp() != 64 || combined.P95Power() != 22 || combined.ErrorPercent() == nil || *combined.ErrorPercent() != 4 {
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

func TestWindowAggregateRejectsInvalidEvidence(t *testing.T) {
	if _, err := lib.NewWindowAggregate(30, 5*time.Minute, -1, 100, 1, 55, 56, 70, 18, nil, 0, 0); err == nil {
		t.Fatal("negative median hash was accepted")
	}
	if _, err := lib.NewWindowAggregate(30, 5*time.Minute, 100, math.NaN(), 1, 55, 56, 70, 18, nil, 0, 0); err == nil {
		t.Fatal("non-finite expected hash was accepted")
	}
	if _, err := lib.NewWindowAggregate(30, 5*time.Minute, 100, 100, 1, 55, 56, 70, -1, nil, 0, 0); err == nil {
		t.Fatal("negative power was accepted")
	}
	errorPercent := 101.0
	if _, err := lib.NewWindowAggregate(30, 5*time.Minute, 100, 100, 1, 55, 56, 70, 18, &errorPercent, 0, 0); err == nil {
		t.Fatal("invalid window error percentage was accepted")
	}
	if _, err := lib.NewWindowAggregate(0, 5*time.Minute, 100, 100, 1, 55, 56, 70, 18, nil, 0, 0); err == nil {
		t.Fatal("zero sample count was accepted")
	}
	if _, err := lib.NewWindowAggregate(30, -time.Second, 100, 100, 1, 55, 56, 70, 18, nil, 0, 0); err == nil {
		t.Fatal("negative span was accepted")
	}
}

func TestFinalSelectionRejectsOffGridAdvertisedPoint(t *testing.T) {
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	asic.FrequencyOptions = []int{400, 490, 500, 525, 550, 600, 625}
	record := rootRecord(rootTestMAC, lib.OperatingPoint{Frequency: 500, CoreVoltage: 1000}, 100, 55, 18, 70)
	if _, ok := selectFinalPoint([]lib.OperatingPointRecord{record}, asic, settings); ok {
		t.Fatal("off-grid advertised point became final authority")
	}
}

func TestSafetyAuthoritiesRequirePositiveEvidence(t *testing.T) {
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	point := lib.OperatingPoint{Frequency: 490, CoreVoltage: 1060}
	failed := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	zeroHash := rootRecord(rootTestMAC, point, 0, 55, 18, 70)
	if rollbackRecordEligible(zeroHash, failed, asic, settings) {
		t.Fatal("rollback selected a point without positive hash evidence")
	}
	zeroPower := rootRecord(rootTestMAC, point, 100, 55, 0, 70)
	if _, ok := selectFinalPoint([]lib.OperatingPointRecord{zeroPower}, asic, settings); ok {
		t.Fatal("final selection accepted zero power evidence")
	}
}

func TestEntryMarginRejectsInvalidFrozenEvidence(t *testing.T) {
	controller := &controller{}
	entry := rootRecord(rootTestMAC, lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}, 100, 55, 18, 70)
	entry.EntryAttemptID = 1
	entry.ReferenceHash = 0
	if controller.entryMarginPositive(&lib.MinerState{}, entry, 100, lib.Settings{}, time.Now().UTC()) {
		t.Fatal("entry margin accepted a missing frozen reference")
	}
}

func TestEntryMarginUsesCompletedTrialHash(t *testing.T) {
	now := time.Now().UTC()
	settings := rootTestSettings(t)
	attempt := lib.MutationAttempt{
		ID:               1,
		PatchRequestedAt: now.Add(-2 * time.Minute),
		MiningResumedAt:  now.Add(-time.Minute),
	}
	if !mutationMarginPositive(1075.88, 541.54, attempt, settings, now) {
		t.Fatal("profitable measured trial was rejected by mutation margin")
	}
	if mutationMarginPositive(0, 541.54, attempt, settings, now) {
		t.Fatal("missing measured trial hash was accepted")
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
	good := mustWindow(t, 103, 100, 0, 0, 0, 0, nil)
	badPerformance := mustWindow(t, 101, 100, 0, 0, 0, 0, nil)
	if !trialWindowPredicate(lib.PhaseFrequencyTest, good, prior, 100) || trialWindowPredicate(lib.PhaseFrequencyTest, badPerformance, prior, 100) {
		t.Fatal("performance predicate did not distinguish individual windows")
	}
	prior.P95Temp = 60
	prior.P95Power = 18
	prior.P95VRTemp = 70
	goodUndervolt := mustWindow(t, 103, 100, 0, 55, 65, 17, nil)
	badUndervolt := mustWindow(t, 97, 100, 0, 55, 65, 17, nil)
	if !trialWindowPredicate(lib.PhaseUndervolt, goodUndervolt, prior, 100) || trialWindowPredicate(lib.PhaseUndervolt, badUndervolt, prior, 100) {
		t.Fatal("undervolt predicate did not distinguish individual windows")
	}
}

func TestUnhealthyManualValidationBecomesRejectedMonitoring(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	result, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := closeInitialBaselineEpoch(t, store, result.State, now)
	result, err = store.Apply(lib.AdoptManualPoint{MacAddr: state.MacAddr, Point: point}, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	state = result.State
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	window := mustWindow(t, 0, 100, 55, 55, 70, 18, nil)
	epoch := mustOpenEpoch(t, store, state.MacAddr)
	progress := epoch.Progress
	if err := progress.CloseWindow(true, window); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply(lib.AdvanceEpoch{Epoch: epoch, Progress: progress}, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	epoch = mustOpenEpoch(t, store, state.MacAddr)
	combined, err := window.Combine(window)
	if err != nil {
		t.Fatal(err)
	}
	if err := minerController.evaluateMonitor(
		context.Background(), &state, epoch, combined, rootTestASIC(), rootTestSettings(t), now.Add(3*time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	if state.Phase != lib.PhaseMonitor || state.MonitorReason != lib.MonitorRejected || !state.SettledAt.IsZero() {
		t.Fatalf("unhealthy manual validation = %+v", state)
	}
	if epoch, open, err := store.OpenEvidenceEpochFor(state.MacAddr); err != nil || !open || epoch.Purpose != lib.EpochMonitor {
		t.Fatalf("rejected manual monitor lost evidence: open=%t err=%v", open, err)
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
			PostSettlementCoverageValid: true, NormalRestartBaselineObserved: true,
			Frontier24Audited: true, Frontier24Valid: true,
			Restart: lib.RestartExposure{NormalRequests: 2, NormalExposureSeconds: 31, SafetyRequests: 1, SafetyExposureSeconds: 12, UnresolvedAttempts: 0},
		},
		Control: lib.ReportMinerMetrics{
			Coverage: .98, ObservedSeconds: 590, UnknownGapSeconds: 16,
			ActualHashSeconds: 120000, NormalizedWork: 1,
			PreArmSettledHashRate: 100,
			Restart:               lib.RestartExposure{NormalRequests: 1, NormalExposureSeconds: 14},
		},
		Uplift: .03, Valid: true, Accepted: true,
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

func TestReportLoaderUsesHistoricalControlBoundaryAfterSecondRetune(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	oldPoint := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	info := rootTestInfo(oldPoint, 100)
	info.MacAddr = "aa:bb:cc:dd:ee:04"
	info.Hostname = "control-history"
	createdAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	bootstrapResult, err := store.Apply(lib.Bootstrap{
		Info: info, IP: "192.0.2.14", PairAdvertised: true,
	}, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrapResult.State
	epoch := mustOpenEpoch(t, store, info.MacAddr)
	baseline := lib.OperatingPointRecord{
		MacAddr: info.MacAddr, Frequency: oldPoint.Frequency, CoreVoltage: oldPoint.CoreVoltage,
		Status: lib.PointValidated, MedianHash: 100, ExpectedHash: 100, Attainment: 1,
		MeanTemp: 55, P95Temp: 56, P95VRTemp: 70, P95Power: 18,
		MeasuredAt: createdAt.Add(10 * time.Minute), EnteredAt: createdAt,
	}
	finalizeResult, err := store.Apply(lib.CompleteBaseline{
		State: state, Record: baseline, Epoch: epoch, Decision: lib.BaselinePlace,
		Selected: oldPoint, Best: oldPoint, BestHashRate: baseline.MedianHash,
	}, baseline.MeasuredAt)
	if err != nil {
		t.Fatal(err)
	}
	state = finalizeResult.State
	boundarySettledAt := createdAt.Add(23 * time.Hour)
	state = settleSelectedMonitor(t, store, state, boundarySettledAt)

	armStart := createdAt.Add(24 * time.Hour)
	passStart := armStart.Add(lib.ReportArmDuration)
	if _, err := store.Apply(lib.StartPass{
		MacAddr: info.MacAddr, Point: oldPoint, Trigger: lib.PassOperator,
	}, passStart); err != nil {
		t.Fatal(err)
	}
	input, err := loadReportMinerInput(store, info.Hostname, lib.ReportWindow{Start: armStart, End: passStart}, false)
	if err != nil {
		t.Fatal(err)
	}
	if input.PreArmSettledHashRate != 100 || input.BoundaryPoint != oldPoint ||
		!input.BoundarySettledAt.Equal(boundarySettledAt) || !input.PointStable {
		t.Fatalf("historical control boundary = %+v", input)
	}
	if len(input.PointRecords) != 1 || input.PointRecords[0].Status != lib.PointEntered {
		t.Fatalf("historical control consulted new pass rows: %+v", input.PointRecords)
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
	if len(fragments) != 3 || fragments[0].ObservedDuration != 30*time.Minute || fragments[1].ObservedDuration != time.Hour || fragments[2].ObservedDuration != 15*time.Minute {
		t.Fatalf("hourly split = %+v", fragments)
	}
	if fragments[0].TrialDuration != 30*time.Minute || fragments[0].IncumbentCounterfactualHashSeconds != 180000 {
		t.Fatalf("trial classification = %+v", fragments[0])
	}
	unknown := hourlyFragments(rootTestMAC, start, end, sample, false)
	if unknown[0].UnknownGapDuration != 30*time.Minute || unknown[0].ActualHashSeconds != 0 {
		t.Fatalf("unknown classification = %+v", unknown[0])
	}
}

func TestHourlyAccountingClassificationRejectsSamePhaseStateTransition(t *testing.T) {
	from := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	state := lib.MinerState{
		Phase:              lib.PhaseMonitor,
		MonitorReason:      lib.MonitorSelected,
		CurrentFrequency:   525,
		CurrentCoreVoltage: 1150,
		PassReferenceHash:  100,
		SettledAt:          from.Add(-time.Hour),
		AccountedThroughAt: from,
	}
	first := accountingSample{
		at:             from,
		validHash:      true,
		classification: classifyAccountingState(state, 0, true, false),
	}
	if !accountingSamplesCompatible(&first, accountingSample{
		at:             from.Add(time.Minute),
		validHash:      true,
		classification: classifyAccountingState(state, 0, true, false),
	}, from, time.Hour) {
		t.Fatal("unchanged accounting classification was rejected")
	}
	changed := state
	changed.PendingKind = lib.MutationSafetyRollback
	changed.PendingFrequency = 400
	changed.PendingCoreVoltage = 1000
	if accountingSamplesCompatible(&first, accountingSample{
		at:             from.Add(time.Minute),
		validHash:      true,
		classification: classifyAccountingState(changed, 0, false, false),
	}, from, time.Hour) {
		t.Fatal("same-phase pending transition was credited as actual")
	}
}

// pollSequence yields deterministic tick times at a given delivered-tick rate: every Nth tick is
// dropped, no RNG, so a failure reproduces exactly. rate 1.0 delivers every tick; 0.75 drops one in
// four.
func pollSequence(start time.Time, settings lib.Settings, rate float64, ticks int) []time.Time {
	if rate <= 0 {
		return nil
	}
	dropEvery := 0
	if rate < 1 {
		dropEvery = int(math.Round(1 / (1 - rate)))
	}
	times := make([]time.Time, 0, ticks)
	for tick := 0; tick < ticks; tick++ {
		if dropEvery > 0 && (tick+1)%dropEvery == 0 {
			continue
		}
		times = append(times, start.Add(time.Duration(tick)*settings.MetricsTime))
	}
	return times
}

// TestDegradedPollYieldStillAdmitsAWindow is the direct inverse of the pre-cutover
// TestBaselineEvidenceDeadlineTerminalizesBootstrapRow: the old wall-clock evidence deadline
// terminalized a baseline as PointUnobservable whenever the poll yield was imperfect (the exact
// RFC-diagnosed defect). Under the epoch/window machinery a degraded, but not catastrophic, delivered
// -tick rate still produces an admitted window instead.
func TestDegradedPollYieldStillAdmitsAWindow(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	bootstrapResult, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrapResult.State
	if state.Phase != lib.PhaseBaseline {
		t.Fatalf("bootstrap did not start a baseline pass: %+v", state)
	}
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	// 0.75 delivered-tick rate: enough ticks that ramp (6 settled samples) plus a full 30-sample
	// window both complete comfortably inside windowMaxSpan even with the drops.
	for _, tick := range pollSequence(now, settings, 0.75, 80) {
		info := rootTestInfo(point, 100)
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, info, asic), settings, tick, true); err != nil {
			t.Fatalf("control miner after safety at %s: %v", tick, err)
		}
	}
	epoch, open, err := store.OpenEvidenceEpochFor(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	if !open {
		t.Fatal("baseline epoch closed before a window admitted")
	}
	if epoch.Progress.ClosedWindows() < 1 {
		t.Fatalf("degraded poll yield admitted no window: progress=%+v", epoch.Progress)
	}
	points, err := store.ListPoints(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	if record, found := findRecord(points, point); !found || record.Status != lib.PointEntered {
		t.Fatalf("baseline row was terminalized as unobservable under a degraded poll yield: %+v", points)
	}
}

// TestWindowClosesOnTargetSampleCountAtHealthyRate exercises the first of window closure's two
// bounds: at a clean poll yield, a window closes once targetSampleCount samples have arrived, well
// inside windowMaxSpan, exactly as the pre-epoch fixed-30-sample window did.
func TestWindowClosesOnTargetSampleCountAtHealthyRate(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	bootstrapResult, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrapResult.State
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	for _, tick := range pollSequence(now, settings, 1.0, rampSamples(settings)+targetSampleCount(settings)) {
		info := rootTestInfo(point, 100)
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, info, asic), settings, tick, true); err != nil {
			t.Fatalf("control miner after safety at %s: %v", tick, err)
		}
	}
	epoch := mustOpenEpoch(t, store, rootTestMAC)
	window, ok := epoch.Progress.ClosedWindow()
	if !ok {
		t.Fatalf("no window closed: progress=%+v", epoch.Progress)
	}
	if window.SampleCount() != targetSampleCount(settings) {
		t.Fatalf("window closed with %d samples, want exactly targetSampleCount %d (should close on count, not span)", window.SampleCount(), targetSampleCount(settings))
	}
	if window.Span() >= windowMaxSpan(settings) {
		t.Fatalf("window span %s reached windowMaxSpan %s at a healthy rate", window.Span(), windowMaxSpan(settings))
	}
}

func TestFleetGatePreservesOneBaselineWindowBeforeFrontierDecision(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	bootstrap, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrap.State
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	at := now
	poll := mustReadablePoll(t, rootTestInfo(point, 100), asic)
	for index := 0; index < rampSamples(settings)+2*targetSampleCount(settings); index++ {
		at = at.Add(settings.MetricsTime)
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, poll, settings, at, false); err != nil {
			t.Fatal(err)
		}
	}
	epoch := mustOpenEpoch(t, store, rootTestMAC)
	if epoch.Progress.SettledSamples() < rampSamples(settings) || epoch.Progress.ClosedWindows() != 1 ||
		state.PendingKind != "" {
		t.Fatalf("closed fleet gate crossed its evidence boundary: state=%+v epoch=%+v", state, epoch)
	}
	if window := minerController.formatWindow(state, settings, at); !strings.HasPrefix(window, "1/2 ") {
		t.Fatalf("paused baseline window = %q", window)
	}
	for index := 0; index < 2*targetSampleCount(settings); index++ {
		at = at.Add(settings.MetricsTime)
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, poll, settings, at, true); err != nil {
			t.Fatal(err)
		}
	}
	if state.PendingKind != lib.MutationOperatingPoint || state.PendingPoint() == (lib.OperatingPoint{}) {
		t.Fatalf("opened fleet gate did not resume frontier: %+v", state)
	}
}

// TestWindowClosesOnSpanAndAdmitsAtWindowMinSamples exercises window closure's second bound: below
// a 0.5 delivered-tick rate, a window never accumulates targetSampleCount samples before
// windowMaxSpan elapses, so the span backstop closes it instead — and it is still admitted, with
// at least windowMinSamples samples and no single gap exceeding windowMaxGap.
func TestWindowClosesOnSpanAndAdmitsAtWindowMinSamples(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	bootstrapResult, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrapResult.State
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	// A period-5 "deliver, drop, drop, deliver, drop" pattern (delivered at tick%5 == 0 or 3): a 0.4
	// delivered-tick rate (below the 0.5 threshold windowMinSamples binds under), no run longer than
	// two consecutive drops (a 30s gap, exactly at windowMaxGap, still admissible), spread evenly so
	// windowMaxSpan (600s) elapses with fewer than targetSampleCount (30) but at least
	// windowMinSamples (24) delivered.
	for tick := 0; tick <= 75; tick++ {
		if tick%5 != 0 && tick%5 != 3 {
			continue
		}
		at := now.Add(time.Duration(tick) * settings.MetricsTime)
		info := rootTestInfo(point, 100)
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, info, asic), settings, at, true); err != nil {
			t.Fatalf("control miner after safety at tick %d (%s): %v", tick, at, err)
		}
	}
	epoch := mustOpenEpoch(t, store, rootTestMAC)
	window, ok := epoch.Progress.ClosedWindow()
	if !ok {
		t.Fatalf("degraded rate admitted no window: progress=%+v", epoch.Progress)
	}
	if window.SampleCount() >= targetSampleCount(settings) {
		t.Fatalf("window closed with the full sample count %d under a degraded rate; span backstop should have fired first", window.SampleCount())
	}
	if window.SampleCount() < windowMinSamples(settings) {
		t.Fatalf("window admitted below windowMinSamples: got %d samples, want >= %d", window.SampleCount(), windowMinSamples(settings))
	}
	if window.Span() < windowMaxSpan(settings) {
		t.Fatalf("window closed before windowMaxSpan elapsed: span=%s, want >= %s", window.Span(), windowMaxSpan(settings))
	}
}

// TestRejectedWindowIncrementsCountWithoutDiscardingStoredFirstWindow admits a clean first window
// (baseline requires two), then forces the second window to fail admission via a single gap beyond
// windowMaxGap. The epoch's rejected-window count must advance and the durably stored first window
// must survive untouched — a rejected window is a data-quality event, never a reason to discard
// evidence already earned.
func TestRejectedWindowIncrementsCountWithoutDiscardingStoredFirstWindow(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	bootstrapResult, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrapResult.State
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	// Ramp (6 ticks) plus a clean 30-sample first window: ticks 0..35.
	for _, tick := range pollSequence(now, settings, 1.0, rampSamples(settings)+targetSampleCount(settings)) {
		info := rootTestInfo(point, 100)
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, info, asic), settings, tick, true); err != nil {
			t.Fatalf("control miner after safety at %s: %v", tick, err)
		}
	}
	epoch := mustOpenEpoch(t, store, rootTestMAC)
	admittedWindow, ok := epoch.Progress.ClosedWindow()
	if !ok || epoch.Progress.ClosedWindows() != 1 {
		t.Fatalf("first window did not close cleanly: progress=%+v", epoch.Progress)
	}
	// A 5-tick (50s) gap before the second window's first sample exceeds windowMaxGap (30s), so
	// whatever closes this window will fail admission regardless of its eventual sample count.
	// Deliver 30 consecutive samples after the gap so it closes on targetSampleCount quickly.
	for tick := 41; tick <= 70; tick++ {
		at := now.Add(time.Duration(tick) * settings.MetricsTime)
		info := rootTestInfo(point, 100)
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, info, asic), settings, at, true); err != nil {
			t.Fatalf("control miner after safety at tick %d (%s): %v", tick, at, err)
		}
	}
	epoch = mustOpenEpoch(t, store, rootTestMAC)
	if epoch.Progress.RejectedWindows() != 1 {
		t.Fatalf("rejected-window count = %d, want 1", epoch.Progress.RejectedWindows())
	}
	if epoch.Progress.ClosedWindows() != 1 {
		t.Fatalf("closed-window count changed by a rejection: %d, want 1", epoch.Progress.ClosedWindows())
	}
	survivingWindow, ok := epoch.Progress.ClosedWindow()
	if !ok || survivingWindow.SampleCount() != admittedWindow.SampleCount() || survivingWindow.MedianHash() != admittedWindow.MedianHash() {
		t.Fatalf("stored first window changed after a later rejection: before=%+v after=%+v", admittedWindow, survivingWindow)
	}
}

// TestRestartMidEpochResumesAgainstDurableWindow closes a baseline's first window, then simulates a
// process restart by discarding the in-memory runtime map (a fresh controller, exactly as a real
// restart would produce) before the second window arrives. A process restart, resetRuntime, or a
// mutation gate must cost at most one partial window of exposure — the durable first window is
// resumed against, not lost.
func TestRestartMidEpochResumesAgainstDurableWindow(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	bootstrapResult, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrapResult.State
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	for _, tick := range pollSequence(now, settings, 1.0, rampSamples(settings)+targetSampleCount(settings)) {
		info := rootTestInfo(point, 100)
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, info, asic), settings, tick, true); err != nil {
			t.Fatalf("control miner after safety at %s: %v", tick, err)
		}
	}
	epoch := mustOpenEpoch(t, store, rootTestMAC)
	if epoch.Progress.ClosedWindows() != 1 {
		t.Fatalf("first window did not close cleanly: progress=%+v", epoch.Progress)
	}
	// Simulate a process restart: a brand-new controller over the same durable store, with no
	// in-memory sample buffer at all — everything addSample had accumulated is gone.
	restarted := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	reloaded, err := store.LoadMiner(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	state = reloaded
	for tick := 41; tick <= 70; tick++ {
		at := now.Add(time.Duration(tick) * settings.MetricsTime)
		info := rootTestInfo(point, 100)
		if err := restarted.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, info, asic), settings, at, true); err != nil {
			t.Fatalf("control miner after safety at tick %d (%s): %v", tick, at, err)
		}
	}
	points, err := store.ListPoints(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	record, found := findRecord(points, point)
	if !found || record.Status != lib.PointValidated {
		t.Fatalf("baseline did not validate after resuming against the durable first window post-restart: %+v", points)
	}
	if record.EvidenceEpochID <= 0 {
		t.Fatalf("validated baseline has no resolvable evidence epoch: %+v", record)
	}
}

// starveExhaustBaseline drives a fresh baseline epoch through ramp and exactly maxRejectedWindows
// rejected windows, none ever admitted, so no measurement ever informs the outcome and the epoch
// closes starved rather than rejected. Each rejected window is two polls a single windowMaxSpan-plus
// gap apart: the gap alone exceeds windowMaxGap (failing admission) and the span alone reaches
// windowMaxSpan (closing the window on the span bound with only two samples), so every window in the
// sequence rejects identically and cheaply. It returns the absolute time of the last poll applied.
func starveExhaustBaseline(
	t *testing.T,
	minerController *controller,
	state *lib.MinerState,
	point lib.OperatingPoint,
	asic lib.ASICSettings,
	settings lib.Settings,
	start time.Time,
) time.Time {
	t.Helper()
	cursor := start
	for _, tick := range pollSequence(cursor, settings, 1.0, rampSamples(settings)) {
		info := rootTestInfo(point, 100)
		if err := minerController.controlMinerAfterSafety(context.Background(), state, mustReadablePoll(t, info, asic), settings, tick, true); err != nil {
			t.Fatalf("control miner after safety (ramp) at %s: %v", tick, err)
		}
		cursor = tick
	}
	gap := windowMaxSpan(settings) + 20*time.Second
	for window := 0; window < maxRejectedWindows; window++ {
		cursor = cursor.Add(gap)
		info := rootTestInfo(point, 100)
		if err := minerController.controlMinerAfterSafety(context.Background(), state, mustReadablePoll(t, info, asic), settings, cursor, true); err != nil {
			t.Fatalf("control miner after safety (reject window start %d) at %s: %v", window, cursor, err)
		}
		cursor = cursor.Add(gap)
		info = rootTestInfo(point, 100)
		if err := minerController.controlMinerAfterSafety(context.Background(), state, mustReadablePoll(t, info, asic), settings, cursor, true); err != nil {
			t.Fatalf("control miner after safety (reject window close %d) at %s: %v", window, cursor, err)
		}
	}
	return cursor
}

// TestMaxRejectedWindowsStartsStarvedMonitoring proves an exhausted baseline closes as starved and
// atomically opens continuous two-window monitoring at the same point.
func TestMaxRejectedWindowsStartsStarvedMonitoring(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := lib.OpenOptimizerStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	bootstrapResult, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrapResult.State
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}

	starveExhaustBaseline(t, minerController, &state, point, asic, settings, now)

	if state.Phase != lib.PhaseMonitor || state.MonitorReason != lib.MonitorStarved || !state.SettledAt.IsZero() {
		t.Fatalf("starved baseline did not enter starved monitoring: %+v", state)
	}
	if state.CurrentPoint() != point {
		t.Fatalf("starvation changed the durable current point: %+v", state)
	}
	epoch, open, err := store.OpenEvidenceEpochFor(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	if !open || epoch.Purpose != lib.EpochMonitor || epoch.Point != point || epoch.RequiredWindows != 2 {
		t.Fatalf("starved baseline did not open monitoring at the same point: open=%t epoch=%+v", open, epoch)
	}
	if epoch.Progress.SettledSamples() != 0 {
		t.Fatalf("fresh monitor epoch has nonzero settled samples: %+v", epoch.Progress)
	}
	points, err := store.ListPoints(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	if record, found := findRecord(points, point); !found || record.Status != lib.PointEntered {
		t.Fatalf("starvation wrote or discarded the baseline's entered row: %+v", points)
	}

	// Reopening re-runs the full starved-monitor cross-table invariant.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := lib.OpenOptimizerStore(dbPath)
	if err != nil {
		t.Fatalf("reopen after starvation failed cross-table validation: %v", err)
	}
	defer reopened.Close()
}

// TestStarvedMonitorAutoExitsAfterTwoHealthyWindows proves starvation is not terminal: continuous
// monitoring starts a new environmental pass once it can produce a complete healthy assessment.
func TestStarvedMonitorAutoExitsAfterTwoHealthyWindows(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	bootstrapResult, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrapResult.State
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}

	cursor := starveExhaustBaseline(t, minerController, &state, point, asic, settings, now)
	if state.Phase != lib.PhaseMonitor || state.MonitorReason != lib.MonitorStarved {
		t.Fatalf("setup did not reach starved monitoring: %+v", state)
	}

	target := rampSamples(settings) + 2*targetSampleCount(settings)
	for i, tick := range pollSequence(cursor.Add(settings.MetricsTime), settings, 1, target) {
		cursor = tick
		info := rootTestInfo(point, 100)
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, info, asic), settings, cursor, true); err != nil {
			t.Fatalf("control miner after safety (monitor sample %d) at %s: %v", i+1, cursor, err)
		}
		if i+1 < target {
			if state.Phase != lib.PhaseMonitor || state.MonitorReason != lib.MonitorStarved {
				t.Fatalf("starved monitor exited before two windows completed (at sample %d of %d): %+v", i+1, target, state)
			}
		}
	}

	if state.Phase != lib.PhaseBaseline || state.MonitorReason != "" {
		t.Fatalf("starved monitor did not start an environmental pass: %+v", state)
	}
	epoch, open, err := store.OpenEvidenceEpochFor(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	if !open || epoch.Purpose != lib.EpochBaseline || epoch.Point != point || epoch.RequiredWindows != 2 {
		t.Fatalf("monitor did not open a fresh baseline evaluation: open=%t epoch=%+v", open, epoch)
	}
	if epoch.Progress.SettledSamples() != 0 || epoch.Progress.ClosedWindows() != 0 {
		t.Fatalf("reopened baseline epoch is not fresh: %+v", epoch.Progress)
	}
	points, err := store.ListPoints(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	if record, found := findRecord(points, point); !found || record.Status != lib.PointEntered {
		t.Fatalf("baseline reopen wrote or discarded the entered row: %+v", points)
	}
}

// TestRejectedMonitorAutoExitsWhenQualityRecovers proves a measured rejection remains monitored and
// automatically starts a fresh pass after two healthy windows reverse the failed condition.
func TestRejectedMonitorAutoExitsWhenQualityRecovers(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	bootstrapResult, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrapResult.State
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}

	unhealthyInfo := func() lib.Info {
		info := rootTestInfo(point, 100)
		info.HashRate = 0
		info.ExpectedHashRate = 0
		return info
	}
	// Two full admitted windows (ramp + two full targetSampleCount windows) at a clean rate, every
	// sample reporting zero hash: qualityHealthy fails on the combined aggregate and evaluateBaseline
	// records a real (if bad) measurement and enters rejected monitoring.
	var lastTick time.Time
	for _, tick := range pollSequence(now, settings, 1.0, rampSamples(settings)+2*targetSampleCount(settings)) {
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, unhealthyInfo(), asic), settings, tick, true); err != nil {
			t.Fatalf("control miner after safety (quality failure setup) at %s: %v", tick, err)
		}
		lastTick = tick
	}
	if state.Phase != lib.PhaseMonitor || state.MonitorReason != lib.MonitorRejected || !state.SettledAt.IsZero() {
		t.Fatalf("setup did not reach rejected monitoring: %+v", state)
	}
	points, err := store.ListPoints(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	record, found := findRecord(points, point)
	if !found || record.Status != lib.PointUnstable || record.EvidenceEpochID <= 0 {
		t.Fatalf("rejected baseline did not carry a real measured record: %+v", points)
	}

	cursor := lastTick
	for i, tick := range pollSequence(cursor.Add(settings.MetricsTime), settings, 1,
		rampSamples(settings)+2*targetSampleCount(settings)) {
		cursor = tick
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, rootTestInfo(point, 100), asic), settings, cursor, true); err != nil {
			t.Fatalf("control miner after safety (rejected monitor poll %d) at %s: %v", i, cursor, err)
		}
	}
	if state.Phase != lib.PhaseBaseline || state.PassTrigger != lib.PassEnvironment {
		t.Fatalf("rejected monitor did not start an environmental pass: %+v", state)
	}
	if epoch, open, err := store.OpenEvidenceEpochFor(rootTestMAC); err != nil || !open || epoch.Purpose != lib.EpochBaseline {
		t.Fatalf("environmental pass has no baseline evidence: open=%t epoch=%+v err=%v", open, epoch, err)
	}
}

func TestSelectedMonitorContinuouslyReopensAfterStableAssessment(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	bootstrap, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := closeInitialBaselineEpoch(t, store, bootstrap.State, now.Add(time.Minute))
	referenceID := state.MonitorReferenceEpochID
	settledAt := state.SettledAt
	firstSuccessor := mustOpenEpoch(t, store, state.MacAddr).ID
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	if window := minerController.formatWindow(state, settings, now.Add(time.Minute)); !strings.HasPrefix(window, "selected 0/") {
		t.Fatalf("selected monitor progress is hidden: %q", window)
	}
	cursor := now.Add(time.Minute)
	for _, tick := range pollSequence(cursor.Add(settings.MetricsTime), settings, 1, 2*targetSampleCount(settings)) {
		cursor = tick
		if err := minerController.controlMinerAfterSafety(
			context.Background(), &state, mustReadablePoll(t, rootTestInfo(point, 100), asic), settings, tick, true,
		); err != nil {
			t.Fatalf("stable monitor poll at %s: %v", tick, err)
		}
	}
	if state.Phase != lib.PhaseMonitor || state.MonitorReason != lib.MonitorSelected ||
		state.MonitorReferenceEpochID != referenceID || !state.SettledAt.Equal(settledAt) {
		t.Fatalf("stable assessment changed selected monitor authority: %+v", state)
	}
	successor := mustOpenEpoch(t, store, state.MacAddr)
	if successor.ID == firstSuccessor || successor.Purpose != lib.EpochMonitor ||
		successor.Progress.ClosedWindows() != 0 {
		t.Fatalf("stable assessment did not open a fresh monitor cycle: %+v", successor)
	}
}

func TestSafetyInterruptClosesMonitorAndClearsLiveReference(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	bootstrap, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := closeInitialBaselineEpoch(t, store, bootstrap.State, now.Add(time.Minute))
	monitor := mustOpenEpoch(t, store, state.MacAddr)
	info := rootTestInfo(point, 100)
	info.Temp = settings.TempLimit + .5
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	handled, err := minerController.enforceMinerSafety(
		context.Background(), &state, info, asic, settings, now.Add(2*time.Minute),
	)
	if err != nil || !handled {
		t.Fatalf("safety interrupt = handled:%t err:%v", handled, err)
	}
	if state.Phase != lib.PhaseCooldown || state.PendingKind != lib.MutationSafetyRollback ||
		state.MonitorReason != "" || state.MonitorReferenceEpochID != 0 || !state.SettledAt.IsZero() {
		t.Fatalf("safety interrupt retained monitor authority: %+v", state)
	}
	if _, open, err := store.OpenEvidenceEpochFor(state.MacAddr); err != nil || open {
		t.Fatalf("safety interrupt retained open monitor evidence: open=%t err=%v", open, err)
	}
	closed, err := store.EvidenceEpochByID(monitor.ID)
	if err != nil || closed.Outcome != lib.EpochContradicted || closed.ClosedAt.IsZero() {
		t.Fatalf("interrupted monitor epoch = %+v, %v", closed, err)
	}
}

func TestWarmBaselineStillExploresSafeUndervolt(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	bootstrap, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrap.State
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	for _, tick := range pollSequence(now.Add(settings.MetricsTime), settings, 1,
		rampSamples(settings)+2*targetSampleCount(settings)) {
		info := rootTestInfo(point, 100)
		info.Temp = settings.TargetTemp
		if err := minerController.controlMinerAfterSafety(
			context.Background(), &state, mustReadablePoll(t, info, asic), settings, tick, true,
		); err != nil {
			t.Fatalf("warm baseline poll at %s: %v", tick, err)
		}
	}
	want := lib.OperatingPoint{Frequency: point.Frequency, CoreVoltage: 1100}
	if state.Phase != lib.PhaseUndervolt || state.PendingKind != lib.MutationOperatingPoint ||
		state.PendingPoint() != want {
		t.Fatalf("warm baseline did not choose the safe lower-voltage pair: %+v", state)
	}
}

func TestSelectedMonitorHashDriftStartsEnvironmentalPass(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	bootstrap, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := closeInitialBaselineEpoch(t, store, bootstrap.State, now.Add(time.Minute))
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	for _, tick := range pollSequence(now.Add(time.Minute+settings.MetricsTime), settings, 1, 2*targetSampleCount(settings)) {
		info := rootTestInfo(point, 100)
		info.HashRate = 90
		if err := minerController.controlMinerAfterSafety(
			context.Background(), &state, mustReadablePoll(t, info, asic), settings, tick, true,
		); err != nil {
			t.Fatalf("drift monitor poll at %s: %v", tick, err)
		}
	}
	if state.Phase != lib.PhaseBaseline || state.PassTrigger != lib.PassEnvironment ||
		state.MonitorReason != "" || state.MonitorReferenceEpochID != 0 || !state.SettledAt.IsZero() {
		t.Fatalf("hash drift did not start a fresh environmental pass: %+v", state)
	}
	epoch := mustOpenEpoch(t, store, state.MacAddr)
	if epoch.Purpose != lib.EpochBaseline || epoch.Point != point || epoch.Progress.ClosedWindows() != 0 {
		t.Fatalf("environmental pass baseline = %+v", epoch)
	}
	points, err := store.ListPoints(state.MacAddr)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 1 || points[0].Point() != point || points[0].Status != lib.PointEntered {
		t.Fatalf("environmental pass did not reset the finite frontier: %+v", points)
	}
}

func TestSelectedMonitorResumesSecondWindowAfterRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := lib.OpenOptimizerStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	bootstrap, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := closeInitialBaselineEpoch(t, store, bootstrap.State, now.Add(time.Minute))
	referenceID := state.MonitorReferenceEpochID
	controllerBefore := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	cursor := now.Add(time.Minute)
	for _, tick := range pollSequence(cursor.Add(settings.MetricsTime), settings, 1, targetSampleCount(settings)) {
		cursor = tick
		if err := controllerBefore.controlMinerAfterSafety(
			context.Background(), &state, mustReadablePoll(t, rootTestInfo(point, 100), asic), settings, tick, true,
		); err != nil {
			t.Fatal(err)
		}
	}
	interrupted := mustOpenEpoch(t, store, state.MacAddr)
	if interrupted.Progress.ClosedWindows() != 1 {
		t.Fatalf("first monitor window was not durable before restart: %+v", interrupted.Progress)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = lib.OpenOptimizerStore(dbPath)
	if err != nil {
		t.Fatalf("reopen after first monitor window: %v", err)
	}
	defer store.Close()
	state, err = store.LoadMiner(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	controllerAfter := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	for _, tick := range pollSequence(cursor.Add(settings.MetricsTime), settings, 1, targetSampleCount(settings)) {
		cursor = tick
		if err := controllerAfter.controlMinerAfterSafety(
			context.Background(), &state, mustReadablePoll(t, rootTestInfo(point, 100), asic), settings, tick, true,
		); err != nil {
			t.Fatal(err)
		}
	}
	if state.Phase != lib.PhaseMonitor || state.MonitorReason != lib.MonitorSelected ||
		state.MonitorReferenceEpochID != referenceID {
		t.Fatalf("restart lost selected monitor authority: %+v", state)
	}
	successor := mustOpenEpoch(t, store, state.MacAddr)
	if successor.ID == interrupted.ID || successor.Purpose != lib.EpochMonitor {
		t.Fatalf("restart did not complete and replace the interrupted monitor epoch: %+v", successor)
	}
}

func TestOffGridMonitorStartsPassOnlyAfterExactPairBecomesAdvertised(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 490, CoreVoltage: 1000}
	bootstrap, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: false,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrap.State
	if state.Phase != lib.PhaseMonitor || state.MonitorReason != lib.MonitorOffGrid {
		t.Fatalf("setup did not enter off-grid monitoring: %+v", state)
	}
	asic := rootTestASIC()
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	for _, tick := range pollSequence(now.Add(settings.MetricsTime), settings, 1,
		rampSamples(settings)+2*targetSampleCount(settings)) {
		if err := minerController.controlMinerAfterSafety(
			context.Background(), &state, mustReadablePoll(t, rootTestInfo(point, 100), asic), settings, tick, true,
		); err != nil {
			t.Fatalf("new-grid monitor poll at %s: %v", tick, err)
		}
	}
	if state.Phase != lib.PhaseBaseline || state.PassTrigger != lib.PassEnvironment ||
		state.CurrentPoint() != point || state.PendingKind != "" {
		t.Fatalf("newly advertised exact pair did not begin an evidence-only pass: %+v", state)
	}
	points, err := store.ListPoints(state.MacAddr)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 1 || points[0].Point() != point || points[0].Status != lib.PointEntered {
		t.Fatalf("newly advertised pair produced unexpected frontier authority: %+v", points)
	}
}

// mustOpenEpoch and closeInitialBaselineEpoch mirror the lib-package test helpers.
func mustOpenEpoch(t *testing.T, store *lib.OptimizerStore, macAddr string) lib.EvidenceEpoch {
	t.Helper()
	epoch, open, err := store.OpenEvidenceEpochFor(macAddr)
	if err != nil {
		t.Fatalf("open evidence epoch: %v", err)
	}
	if !open {
		t.Fatalf("miner %s has no open evidence epoch", macAddr)
	}
	return epoch
}

func closeInitialBaselineEpoch(t *testing.T, store *lib.OptimizerStore, state lib.MinerState, at time.Time) lib.MinerState {
	t.Helper()
	epoch := mustOpenEpoch(t, store, state.MacAddr)
	record := lib.OperatingPointRecord{
		MacAddr: state.MacAddr, Frequency: state.CurrentFrequency, CoreVoltage: state.CurrentCoreVoltage,
		Status: lib.PointValidated, MedianHash: 100, ExpectedHash: 100, Attainment: 1,
		MeanTemp: 55, P95Temp: 56, P95VRTemp: 70, P95Power: 18,
		MeasuredAt: at, EnteredAt: state.PassStartedAt,
	}
	result, err := store.Apply(lib.CompleteBaseline{
		State: state, Record: record, Epoch: epoch, Decision: lib.BaselinePlace,
		Selected: state.CurrentPoint(), Best: state.CurrentPoint(), BestHashRate: record.MedianHash,
	}, at)
	if err != nil {
		t.Fatalf("close initial baseline epoch: %v", err)
	}
	state = result.State
	return settleSelectedMonitor(t, store, state, at)
}

func settleSelectedMonitor(t *testing.T, store *lib.OptimizerStore, state lib.MinerState, at time.Time) lib.MinerState {
	t.Helper()
	monitorEpoch := mustOpenEpoch(t, store, state.MacAddr)
	window := mustWindow(t, 100, 100, 55, 56, 70, 18, nil)
	progress := monitorEpoch.Progress
	if err := progress.CloseWindow(true, window); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply(lib.AdvanceEpoch{Epoch: monitorEpoch, Progress: progress}, at); err != nil {
		t.Fatal(err)
	}
	monitorEpoch = mustOpenEpoch(t, store, state.MacAddr)
	progress = monitorEpoch.Progress
	if err := progress.CloseWindow(true, window); err != nil {
		t.Fatal(err)
	}
	combined, err := window.Combine(window)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Apply(lib.CompleteMonitor{
		State: state, Epoch: monitorEpoch, Progress: progress, Aggregate: combined,
		Decision: lib.MonitorContinue, NextReason: lib.MonitorSelected,
	}, at)
	if err != nil {
		t.Fatalf("settle initial selected monitor: %v", err)
	}
	return result.State
}

func admitTestTrial(
	t *testing.T,
	store *lib.OptimizerStore,
	state lib.MinerState,
	candidate lib.OperatingPoint,
	phase lib.OptimizerPhase,
	referenceHash float64,
	at time.Time,
) lib.TransitionResult {
	t.Helper()
	epoch := mustOpenEpoch(t, store, state.MacAddr)
	record := rootRecord(state.MacAddr, state.CurrentPoint(), 100, 55, 18, 70)
	record.EnteredAt = state.PassStartedAt
	record.MeasuredAt = at
	record.ReferenceHash = 0
	result, err := store.Apply(lib.CompleteBaseline{
		State: state, Record: record, Epoch: epoch, Decision: lib.BaselineContinue,
		Candidate: candidate, CandidatePhase: phase, ReferenceHash: referenceHash,
	}, at)
	if err != nil {
		t.Fatalf("admit test trial: %v", err)
	}
	return result
}

func mustReadablePoll(t *testing.T, info lib.Info, asic lib.ASICSettings) readablePoll {
	t.Helper()
	poll, ok := newReadablePoll(info, asic)
	if !ok {
		t.Fatal("test telemetry failed to construct a readable poll")
	}
	return poll
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
	infoCalls    int
	asicCalls    int
	patchCount   int
	restartCount int
	restartErr   error
}

type stagedReadbackMutationDevice struct {
	*scriptedMutationDevice
	infoCalls int
}

func (device *stagedReadbackMutationDevice) GetSystemInfo(context.Context, string) (lib.Info, error) {
	device.infoCalls++
	if device.infoCalls == 2 {
		return lib.Info{}, errors.New("device temporarily unavailable")
	}
	return device.target, nil
}

func (device *scriptedMutationDevice) GetSystemInfo(context.Context, string) (lib.Info, error) {
	device.infoCalls++
	if device.patched {
		return device.target, nil
	}
	return device.source, nil
}

func (device *scriptedMutationDevice) GetASICSettings(context.Context, string) (lib.ASICSettings, error) {
	device.asicCalls++
	return device.asic, nil
}

func (device *scriptedMutationDevice) PatchOperatingPoint(context.Context, lib.OperatingPoint, string) error {
	device.patched = true
	device.patchCount++
	return nil
}

func (device *scriptedMutationDevice) PatchFirmwareRecovery(context.Context, lib.OperatingPoint, string) error {
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
	bootstrapResult, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(sourcePoint, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrapResult.State
	state = closeInitialBaselineEpoch(t, store, state, now)
	state.SetPendingMutation(lib.MutationOperatingPoint, targetPoint, now)
	if _, err := store.Apply(lib.SaveState{State: state}, now); err != nil {
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

func TestReopenedConfiguredStageUsesItsOwnDeadline(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	target := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
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
	patchAt := now.Add(10 * time.Second)
	configuredAt := now.Add(20 * time.Second)
	if err := store.AdvanceMutationAttempt(attemptID, lib.MutationMilestonePatchRequested, patchAt); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordConfiguredVerification(attemptID, configuredAt, 120); err != nil {
		t.Fatal(err)
	}
	device := &stagedReadbackMutationDevice{scriptedMutationDevice: &scriptedMutationDevice{
		asic: rootTestASIC(), source: rootTestInfo(state.CurrentPoint(), 120),
		target: rootTestInfo(target, 120), patched: true,
	}}
	coordinator := newMutationCoordinator(
		device, store, lib.SettingsFile{}, []lib.DiscoveredMiner{{IP: state.IP, Info: device.source}}, nil, "",
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			postBoot := device.target
			postBoot.UpTimeSeconds = 1
			return lib.DiscoveredMiner{IP: state.IP, Info: postBoot}, nil
		}, nil, log.New(io.Discard, "", 0), nil,
	)
	coordinator.rediscoveryDelay = time.Millisecond
	coordinator.now = func() time.Time { return now.Add(131 * time.Second) }
	loaded, err := store.LoadMiner(state.MacAddr)
	if err != nil {
		t.Fatal(err)
	}
	observation := &minerObservation{
		miner: lib.DiscoveredMiner{IP: state.IP, Info: device.source},
		info:  device.source, asic: device.asic, settings: settings, state: loaded,
	}
	if err := coordinator.startLocked(context.Background(), observation, "", ""); err != nil {
		t.Fatal(err)
	}
	result := <-coordinator.results
	if result.err != nil {
		t.Fatalf("reopened configured stage: %v", result.err)
	}
	if device.patchCount != 0 || device.restartCount != 1 {
		t.Fatalf("reopened hardware requests patch/restart = %d/%d", device.patchCount, device.restartCount)
	}
}

func TestReopenedConfiguredStageGetsFreshWorkerDeadline(t *testing.T) {
	startedAt := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	target := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	device := &scriptedMutationDevice{
		asic:   rootTestASIC(),
		source: rootTestInfo(target, 100),
		target: rootTestInfo(target, 100),
	}
	coordinator := newMutationCoordinator(
		device, nil, lib.SettingsFile{}, nil, nil, "", nil, nil,
		log.New(io.Discard, "", 0), nil,
	)
	workerStartedAt := startedAt.Add(10 * defaultRebootDeadline)
	coordinator.now = func() time.Time { return workerStartedAt }
	info, unsafe, err := coordinator.waitForConfiguredReadback(context.Background(), mutationRequest{
		macAddr: rootTestMAC, ip: "192.0.2.12", kind: lib.MutationOperatingPoint,
		point: target, settings: rootTestSettings(t),
	})
	if err != nil || unsafe || operatingPointFromInfo(info) != target {
		t.Fatalf("fresh configured worker = info:%+v unsafe:%t err:%v", info, unsafe, err)
	}
	if device.infoCalls != 1 {
		t.Fatalf("configured worker info calls = %d", device.infoCalls)
	}
}

func TestConfiguredReadbackAfterDeadlineIsNotAccepted(t *testing.T) {
	settings := rootTestSettings(t)
	startedAt := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	target := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	device := &scriptedMutationDevice{source: rootTestInfo(target, 100), target: rootTestInfo(target, 100)}
	nowCalls := 0
	coordinator := &mutationCoordinator{
		devices: device,
		now: func() time.Time {
			nowCalls++
			if nowCalls <= 2 {
				return startedAt
			}
			return startedAt.Add(defaultRebootDeadline)
		},
	}
	info, unsafe, err := coordinator.waitForConfiguredReadback(
		context.Background(),
		mutationRequest{
			macAddr: rootTestMAC, ip: "192.0.2.12", kind: lib.MutationOperatingPoint,
			point: target, settings: settings,
		},
	)
	if err == nil || unsafe || info.MacAddr != rootTestMAC {
		t.Fatalf("late configured readback = info:%+v unsafe:%t err:%v", info, unsafe, err)
	}
}

func TestVerifiedBootAfterDeadlineIsNotAccepted(t *testing.T) {
	settings := rootTestSettings(t)
	restartAt := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	configuredAt := restartAt.Add(-time.Second)
	target := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	device := &scriptedMutationDevice{asic: rootTestASIC()}
	nowCalls := 0
	coordinator := &mutationCoordinator{
		devices: device,
		discover: func(context.Context, string) (lib.DiscoveredMiner, error) {
			info := rootTestInfo(target, 1)
			return lib.DiscoveredMiner{IP: "192.0.2.12", Info: info}, nil
		},
		rebootDeadline: defaultRebootDeadline,
		now: func() time.Time {
			nowCalls++
			if nowCalls <= 2 {
				return restartAt
			}
			return restartAt.Add(defaultRebootDeadline)
		},
	}
	readback, err := coordinator.waitForVerifiedBoot(
		context.Background(),
		mutationRequest{
			macAddr: rootTestMAC, ip: "192.0.2.12", kind: lib.MutationOperatingPoint,
			point: target, settings: settings, bootProofSameProcess: true,
		},
		100, configuredAt,
	)
	if err == nil || readback.miner.Info.MacAddr != rootTestMAC {
		t.Fatalf("late reboot proof = readback:%+v err:%v", readback, err)
	}
}

func TestMutationReadbackDistinguishesUnavailableFromUnsafeMismatch(t *testing.T) {
	settings := rootTestSettings(t)
	target := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	request := mutationRequest{
		macAddr: rootTestMAC, kind: lib.MutationOperatingPoint, point: target,
		settings: settings,
	}
	coordinator := &mutationCoordinator{}
	if coordinator.readbackNeedsSafetySupersession(request, lib.Info{}, rootTestASIC()) {
		t.Fatal("empty readback was treated as readable safety evidence")
	}
	wrong := rootTestInfo(lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}, 100)
	if err := verifyConfiguredReadback(request, wrong); err == nil {
		t.Fatal("wrong configured pair was accepted")
	}
	unsafe := rootTestInfo(target, 100)
	unsafe.Temp = settings.TempLimit + 1
	if err := verifyConfiguredReadback(request, unsafe); err == nil {
		t.Fatal("unsafe configured pair was accepted")
	}
	if !coordinator.readbackNeedsSafetySupersession(request, unsafe, rootTestASIC()) {
		t.Fatal("unsafe configured pair did not supersede the mutation")
	}
	request.kind = lib.MutationMiningConfiguration
	if !coordinator.preflightNeedsSafetySupersession(request, unsafe, rootTestASIC()) {
		t.Fatal("unsafe mining preflight did not supersede the mutation")
	}
}

func TestPostBootReadbackRetriesIncompleteTelemetry(t *testing.T) {
	settings := rootTestSettings(t)
	target := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	at := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	reads := 0
	coordinator := &mutationCoordinator{
		devices: &scriptedMutationDevice{asic: rootTestASIC()},
		discover: func(context.Context, string) (lib.DiscoveredMiner, error) {
			reads++
			info := rootTestInfo(target, 1)
			if reads == 1 {
				info.Temp = 0
			}
			return lib.DiscoveredMiner{IP: "192.0.2.12", Info: info}, nil
		},
		rebootDeadline: time.Minute, rediscoveryDelay: time.Millisecond,
		now: func() time.Time { return at },
	}
	readback, err := coordinator.waitForVerifiedBoot(context.Background(), mutationRequest{
		macAddr: rootTestMAC, kind: lib.MutationOperatingPoint, point: target,
		settings: settings, bootProofSameProcess: true,
	}, 100, at.Add(-time.Second))
	if err != nil || readback.disposition != rebootReadbackVerified || reads != 2 {
		t.Fatalf("post-boot retry = disposition:%d reads:%d err:%v", readback.disposition, reads, err)
	}
}

func TestPostBootReadbackClassifiesStableSafeMismatch(t *testing.T) {
	settings := rootTestSettings(t)
	at := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		target   lib.OperatingPoint
		observed lib.OperatingPoint
		want     rebootReadbackDisposition
	}{
		{
			name: "AxeOS paired reduction", target: lib.OperatingPoint{Frequency: 625, CoreVoltage: 1100},
			observed: lib.OperatingPoint{Frequency: 525, CoreVoltage: 1000}, want: rebootReadbackFirmwareReduction,
		},
		{
			name: "other safe change", target: lib.OperatingPoint{Frequency: 600, CoreVoltage: 1100},
			observed: lib.OperatingPoint{Frequency: 550, CoreVoltage: 1060}, want: rebootReadbackExternalPoint,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			reads := 0
			coordinator := &mutationCoordinator{
				devices: &scriptedMutationDevice{asic: rootTestASIC()},
				discover: func(context.Context, string) (lib.DiscoveredMiner, error) {
					reads++
					return lib.DiscoveredMiner{IP: "192.0.2.12", Info: rootTestInfo(testCase.observed, 1)}, nil
				},
				rebootDeadline: time.Minute, rediscoveryDelay: time.Millisecond,
				now: func() time.Time { return at },
			}
			readback, err := coordinator.waitForVerifiedBoot(context.Background(), mutationRequest{
				macAddr: rootTestMAC, kind: lib.MutationOperatingPoint, point: testCase.target,
				settings: settings, bootProofSameProcess: true,
			}, 100, at.Add(-time.Second))
			if err != nil || readback.disposition != testCase.want || reads != manualConfirmationPolls {
				t.Fatalf("stable mismatch = disposition:%d reads:%d err:%v", readback.disposition, reads, err)
			}
		})
	}
}

func TestFirmwareRecoveryTargetFloorsCompletePair(t *testing.T) {
	asic := rootTestASIC()
	tests := []struct {
		name    string
		reduced lib.OperatingPoint
		want    lib.OperatingPoint
	}{
		{name: "already advertised", reduced: lib.OperatingPoint{Frequency: 525, CoreVoltage: 1000}, want: lib.OperatingPoint{Frequency: 525, CoreVoltage: 1000}},
		{name: "component floor", reduced: lib.OperatingPoint{Frequency: 590, CoreVoltage: 1090}, want: lib.OperatingPoint{Frequency: 550, CoreVoltage: 1060}},
		{name: "below grid", reduced: lib.OperatingPoint{Frequency: 390, CoreVoltage: 900}, want: lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := firmwareRecoveryTarget(asic, testCase.reduced)
			if err != nil || got != testCase.want {
				t.Fatalf("firmware recovery target = %+v, %v; want %+v", got, err, testCase.want)
			}
		})
	}
}

func TestFirmwareReductionReconcilesToCooldownAndResumesBaseline(t *testing.T) {
	store, settings, state, now := newRootMutationStore(t)
	state = closeInitialBaselineEpoch(t, store, state, now)
	target := lib.OperatingPoint{Frequency: 625, CoreVoltage: 1100}
	reduced := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1000}
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
	coordinator := newMutationCoordinator(nil, store, lib.SettingsFile{}, nil, nil, "", nil, nil, log.New(io.Discard, "", 0), nil)
	coordinator.now = func() time.Time { return now.Add(time.Minute) }
	request := mutationRequest{
		attemptID: attemptID, macAddr: state.MacAddr, kind: lib.MutationOperatingPoint,
		point: target, settings: settings,
	}
	readback := rebootReadback{
		miner: lib.DiscoveredMiner{IP: state.IP, Info: rootTestInfo(reduced, 1)},
		asic:  rootTestASIC(), disposition: rebootReadbackFirmwareReduction,
	}
	if err := coordinator.reconcileFirmwareReduction(request, readback); err != nil {
		t.Fatal(err)
	}
	state, err = store.LoadMiner(state.MacAddr)
	if err != nil || state.Phase != lib.PhaseCooldown || state.CurrentPoint() != reduced ||
		state.PendingKind != "" || state.SafetyReason != lib.SafetyReasonFirmwareOverheat {
		t.Fatalf("reconciled firmware reduction = %+v, %v", state, err)
	}
	attempts, err := store.ListMutationAttempts(state.MacAddr)
	if err != nil || len(attempts) != 1 || attempts[0].FailureStage != lib.MutationFailureSafetySuperseded {
		t.Fatalf("superseded request = %+v, %v", attempts, err)
	}

	controller := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	info := rootTestInfo(reduced, 1)
	info.Temp = settings.RecoveryTemp - 1
	poll := mustReadablePoll(t, info, rootTestASIC())
	at := now.Add(time.Minute)
	for index := 0; index < recoveryHealthyPolls(settings)+rampSamples(settings)+targetSampleCount(settings)+2; index++ {
		at = at.Add(settings.MetricsTime)
		if err := controller.controlMinerAfterSafety(context.Background(), &state, poll, settings, at, true); err != nil {
			t.Fatal(err)
		}
		if state.Phase == lib.PhaseBaseline {
			break
		}
	}
	if state.Phase != lib.PhaseBaseline || state.CurrentPoint() != reduced || state.SafetyReason != "" {
		t.Fatalf("firmware recovery did not resume optimization: %+v", state)
	}
	epoch := mustOpenEpoch(t, store, state.MacAddr)
	if epoch.Purpose != lib.EpochBaseline || epoch.Point != reduced || epoch.RequiredWindows != 2 {
		t.Fatalf("recovery baseline = %+v", epoch)
	}
}

func TestSafeConfiguredMismatchRemainsRetryableBeforeRestart(t *testing.T) {
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
	bootstrapResult, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(sourcePoint, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrapResult.State
	state = closeInitialBaselineEpoch(t, store, state, now)
	state.SetPendingMutation(lib.MutationOperatingPoint, wantedPoint, now)
	if _, err := store.Apply(lib.SaveState{State: state}, now); err != nil {
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
	nowCalls := 0
	coordinator.now = func() time.Time {
		nowCalls++
		return now.Add(time.Duration(nowCalls) * 30 * time.Second)
	}
	observation := &minerObservation{miner: lib.DiscoveredMiner{IP: "192.0.2.12", Info: device.source}, info: device.source, asic: device.asic, settings: settings, state: state}
	if err := coordinator.startLocked(context.Background(), observation, "", ""); err != nil {
		t.Fatal(err)
	}
	result := <-coordinator.results
	if result.err == nil || result.failureStage != "" {
		t.Fatalf("mismatch result = id=%d attempt=%d failure=%q err=%v", result.id, result.attemptID, result.failureStage, result.err)
	}
	coordinator.results <- result
	if _, err := coordinator.applyResultsLocked(); err != nil {
		t.Fatal(err)
	}
	if device.restartCount != 0 {
		t.Fatalf("mismatch issued restart count %d", device.restartCount)
	}
	attempts, err := store.ListMutationAttempts(rootTestMAC)
	if err != nil || len(attempts) != 1 || !attempts[0].FailedAt.IsZero() || attempts[0].PatchRequestedAt.IsZero() {
		t.Fatalf("mismatch attempt = %+v, %v", attempts, err)
	}
	loaded, err := store.LoadMiner(rootTestMAC)
	if err != nil || loaded.Phase == lib.PhaseEmergency || loaded.SafetyReason != "" ||
		loaded.PendingKind != lib.MutationOperatingPoint || loaded.PendingPoint() != wantedPoint {
		t.Fatalf("mismatch retry state = %+v, %v", loaded, err)
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

func TestSummarizePollCycleAttributionComputesPercentiles(t *testing.T) {
	samples := []pollCycleAttribution{
		{httpFanOut: 1 * time.Second, hourlyAccounting: 10 * time.Millisecond, safetyAndControl: 5 * time.Millisecond, mutationAndRender: 1 * time.Millisecond},
		{httpFanOut: 2 * time.Second, hourlyAccounting: 20 * time.Millisecond, safetyAndControl: 6 * time.Millisecond, mutationAndRender: 2 * time.Millisecond},
		{httpFanOut: 3 * time.Second, hourlyAccounting: 30 * time.Millisecond, safetyAndControl: 7 * time.Millisecond, mutationAndRender: 3 * time.Millisecond},
		{httpFanOut: 4 * time.Second, hourlyAccounting: 40 * time.Millisecond, safetyAndControl: 8 * time.Millisecond, mutationAndRender: 4 * time.Millisecond},
	}
	summary := summarizePollCycleAttribution(samples)
	if summary.count != len(samples) {
		t.Fatalf("mismatch count = %d", summary.count)
	}
	if summary.httpP50 != 2500*time.Millisecond {
		t.Fatalf("mismatch http p50 = %s", summary.httpP50)
	}
	if summary.httpP95 != 4*time.Second {
		t.Fatalf("mismatch http p95 = %s", summary.httpP95)
	}
	if summary.totalP50 != samples[1].total()/2+samples[2].total()/2 {
		t.Fatalf("mismatch total p50 = %s", summary.totalP50)
	}
}

func TestSummarizePollCycleAttributionEmptyIsZero(t *testing.T) {
	summary := summarizePollCycleAttribution(nil)
	if summary.count != 0 || summary.totalP50 != 0 || summary.totalP95 != 0 {
		t.Fatalf("mismatch empty summary = %+v", summary)
	}
}

func TestRecordPollCycleAttributionLogsEveryCycleAndFlushesHourly(t *testing.T) {
	var buffer bytes.Buffer
	minerController := &controller{logger: log.New(&buffer, "", 0)}
	start := time.Date(2026, 8, 7, 0, 0, 0, 0, time.UTC)

	minerController.recordPollCycleAttribution(start, pollCycleAttribution{httpFanOut: 10 * time.Millisecond})
	if !strings.Contains(buffer.String(), "poll cycle attribution: total=") {
		t.Fatalf("mismatch per-cycle log = %q", buffer.String())
	}
	if len(minerController.attributionWindow) != 1 {
		t.Fatalf("mismatch window length = %d", len(minerController.attributionWindow))
	}
	if strings.Contains(buffer.String(), "hourly") {
		t.Fatalf("mismatch: hourly summary emitted before an hour elapsed: %q", buffer.String())
	}

	buffer.Reset()
	minerController.recordPollCycleAttribution(start.Add(59*time.Minute), pollCycleAttribution{httpFanOut: 20 * time.Millisecond})
	if strings.Contains(buffer.String(), "hourly") {
		t.Fatalf("mismatch: hourly summary emitted before an hour elapsed: %q", buffer.String())
	}
	if len(minerController.attributionWindow) != 2 {
		t.Fatalf("mismatch window length = %d", len(minerController.attributionWindow))
	}

	buffer.Reset()
	minerController.recordPollCycleAttribution(start.Add(time.Hour), pollCycleAttribution{httpFanOut: 30 * time.Millisecond})
	if !strings.Contains(buffer.String(), "poll cycle attribution (hourly, n=3)") {
		t.Fatalf("mismatch hourly summary log = %q", buffer.String())
	}
	if minerController.attributionWindow != nil {
		t.Fatalf("mismatch window was not reset after flush: %+v", minerController.attributionWindow)
	}
	if !minerController.attributionSince.Equal(start.Add(time.Hour)) {
		t.Fatalf("mismatch attribution window restart = %s", minerController.attributionSince)
	}
}

// TestRecoveryInstrumentationLogsWithoutActingOnPhase covers logRecoveryInstrumentation
// specifically: it must log safeToRecover transitions and temperature slope during
// COOLDOWN/EMERGENCY, must stay silent otherwise, and must never itself change durable state. The
// recovery predicate that acts on safeToRecover is a separate mechanism (recoveryHealthyPolls in
// controlMinerAfterSafety, see TestCooldownExitsAfterConsecutiveHealthyPollsAndClearsSafetyReason);
// this instrumentation remains purely observational.
func TestRecoveryInstrumentationLogsWithoutActingOnPhase(t *testing.T) {
	var buffer bytes.Buffer
	minerController := &controller{
		logger: log.New(&buffer, "", 0), runtimes: make(map[string]*minerRuntime),
	}
	settings := rootTestSettings(t)
	state := lib.MinerState{MacAddr: rootTestMAC, Hostname: "root-test", Phase: lib.PhaseBaseline}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	minerController.logRecoveryInstrumentation(state, rootTestInfo(state.CurrentPoint(), 100), settings, now)
	if buffer.Len() != 0 {
		t.Fatalf("instrumentation logged outside COOLDOWN/EMERGENCY: %q", buffer.String())
	}

	state.Phase = lib.PhaseCooldown
	hot := rootTestInfo(state.CurrentPoint(), 100)
	hot.Temp = settings.RecoveryTemp + 10
	minerController.logRecoveryInstrumentation(state, hot, settings, now)
	if buffer.Len() != 0 {
		t.Fatalf("first poll logged before any prior sample existed: %q", buffer.String())
	}

	cool := rootTestInfo(state.CurrentPoint(), 100)
	cool.Temp = settings.RecoveryTemp - 1
	minerController.logRecoveryInstrumentation(state, cool, settings, now.Add(settings.MetricsTime))
	log := buffer.String()
	if !strings.Contains(log, "safeToRecover false -> true") {
		t.Fatalf("safeToRecover transition not logged: %q", log)
	}
	if !strings.Contains(log, "slope") {
		t.Fatalf("temperature slope not logged: %q", log)
	}
	if strings.Contains(log, hot.MacAddr) || strings.Contains(log, "Password") {
		t.Fatalf("instrumentation log leaked a credential-shaped field: %q", log)
	}
}

// cooldownTestState bootstraps a miner, then forces it into COOLDOWN with a durable SafetyReason
// still set, exactly as transitionEmergencyState leaves a miner once live has returned to the
// safety-rollback minimum. This is a direct state mutation, not a real safety episode, because
// driving enforceMinerSafety through a full firmware overheat and recovery is not this test's
// concern — only what controlMinerAfterSafety does once COOLDOWN is reached. Bootstrap opens a
// baseline epoch as part of the same transition (applyBootstrap), so that epoch must be closed
// here too — EpochContradicted, the RFC's own outcome for "the epoch's subject changed underneath
// it" — or OpenEvidenceEpochFor would still report it open and the recovery predicate below would
// never see the "no epoch open yet" branch it is meant to count against.
func cooldownTestState(t *testing.T, store *lib.OptimizerStore, point lib.OperatingPoint, now time.Time) lib.MinerState {
	t.Helper()
	bootstrapResult, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	baseline := mustOpenEpoch(t, store, bootstrapResult.State.MacAddr)
	state := bootstrapResult.State
	state.Phase = lib.PhaseCooldown
	state.PhaseStartedAt = now
	state.SafetyReason = lib.SafetyReasonFirmwareOverheat
	result, err := store.Apply(lib.CloseEpoch{State: state, Epoch: baseline, Outcome: lib.EpochContradicted}, now)
	if err != nil {
		t.Fatal(err)
	}
	return result.State
}

// TestCooldownExitsAfterConsecutiveHealthyPollsAndClearsSafetyReason drives COOLDOWN through
// exactly recoveryHealthyPolls consecutive safeToRecover-satisfying polls. The predicate must open
// a safety_validation epoch on the poll that reaches the threshold, and clear SafetyReason in that
// same transition — not before it (that would let a not-yet-proven-safe miner report as clear) and
// not as a separate step (that would leave a window where the threshold was met but the durable
// cause was still latched).
func TestCooldownExitsAfterConsecutiveHealthyPollsAndClearsSafetyReason(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	state := cooldownTestState(t, store, point, now)
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	threshold := recoveryHealthyPolls(settings)
	if threshold < 2 {
		t.Fatalf("test requires a threshold of at least 2 consecutive polls, got %d", threshold)
	}
	info := rootTestInfo(point, 100)
	if !safeToRecover(info, settings) {
		t.Fatalf("test telemetry is not safeToRecover under default settings: %+v", info)
	}
	for i := 0; i < threshold-1; i++ {
		at := now.Add(time.Duration(i+1) * settings.MetricsTime)
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, info, asic), settings, at, true); err != nil {
			t.Fatalf("control miner after safety at poll %d: %v", i, err)
		}
		if state.Phase != lib.PhaseCooldown || state.SafetyReason == "" {
			t.Fatalf("poll %d left COOLDOWN or cleared SafetyReason before the threshold: %+v", i, state)
		}
		if state.RecoveryHealthyCount != i+1 {
			t.Fatalf("poll %d recovery healthy count = %d, want %d", i, state.RecoveryHealthyCount, i+1)
		}
		if _, open, err := store.OpenEvidenceEpochFor(rootTestMAC); err != nil {
			t.Fatal(err)
		} else if open {
			t.Fatalf("poll %d opened an epoch before the threshold was reached", i)
		}
	}
	// The threshold-reaching poll.
	at := now.Add(time.Duration(threshold) * settings.MetricsTime)
	if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, info, asic), settings, at, true); err != nil {
		t.Fatalf("control miner after safety at threshold poll: %v", err)
	}
	if state.Phase != lib.PhaseCooldown {
		t.Fatalf("threshold poll changed phase away from COOLDOWN: %+v", state)
	}
	if state.SafetyReason != "" {
		t.Fatalf("SafetyReason was not cleared on the threshold poll: %q", state.SafetyReason)
	}
	if state.RecoveryHealthyCount != 0 {
		t.Fatalf("recovery healthy count was not reset on the threshold poll: %d", state.RecoveryHealthyCount)
	}
	epoch := mustOpenEpoch(t, store, rootTestMAC)
	if epoch.Purpose != lib.EpochSafetyValidation {
		t.Fatalf("threshold poll opened an epoch with purpose %q, want %q", epoch.Purpose, lib.EpochSafetyValidation)
	}
	if epoch.RequiredWindows != 1 {
		t.Fatalf("safety_validation epoch required %d windows, want 1", epoch.RequiredWindows)
	}
	if epoch.Point != point {
		t.Fatalf("safety_validation epoch point = %+v, want %+v", epoch.Point, point)
	}
}

// TestCooldownRecoveryCountResetsOnUnhealthyPoll proves the predicate is a genuine consecutive
// dwell, not a cumulative counter: a single non-satisfying poll between two healthy runs must
// reset the count to zero and leave SafetyReason latched, however close to the threshold the
// count had already gotten.
func TestCooldownRecoveryCountResetsOnUnhealthyPoll(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	state := cooldownTestState(t, store, point, now)
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	threshold := recoveryHealthyPolls(settings)
	if threshold < 2 {
		t.Fatalf("test requires a threshold of at least 2 consecutive polls, got %d", threshold)
	}
	healthy := rootTestInfo(point, 100)
	unhealthy := rootTestInfo(point, 100)
	unhealthy.Temp = settings.RecoveryTemp + 10
	if safeToRecover(unhealthy, settings) {
		t.Fatalf("test telemetry meant to be unsafe was safeToRecover: %+v", unhealthy)
	}
	tick := 0
	nextTick := func() time.Time {
		tick++
		return now.Add(time.Duration(tick) * settings.MetricsTime)
	}
	// Advance to one poll short of the threshold.
	for i := 0; i < threshold-1; i++ {
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, healthy, asic), settings, nextTick(), true); err != nil {
			t.Fatalf("control miner after safety: %v", err)
		}
	}
	if state.RecoveryHealthyCount != threshold-1 {
		t.Fatalf("recovery healthy count before the unhealthy poll = %d, want %d", state.RecoveryHealthyCount, threshold-1)
	}
	// One unhealthy poll must reset the count instead of merely pausing it.
	if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, unhealthy, asic), settings, nextTick(), true); err != nil {
		t.Fatalf("control miner after safety (unhealthy poll): %v", err)
	}
	if state.RecoveryHealthyCount != 0 {
		t.Fatalf("recovery healthy count after an unhealthy poll = %d, want 0", state.RecoveryHealthyCount)
	}
	if state.Phase != lib.PhaseCooldown || state.SafetyReason == "" {
		t.Fatalf("unhealthy poll left COOLDOWN or cleared SafetyReason: %+v", state)
	}
	if _, open, err := store.OpenEvidenceEpochFor(rootTestMAC); err != nil {
		t.Fatal(err)
	} else if open {
		t.Fatal("unhealthy poll opened an epoch")
	}
	// A full fresh run of threshold consecutive healthy polls, after the reset, must still reach
	// the threshold — the reset must not have left the predicate permanently stuck.
	for i := 0; i < threshold; i++ {
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, healthy, asic), settings, nextTick(), true); err != nil {
			t.Fatalf("control miner after safety: %v", err)
		}
	}
	if state.SafetyReason != "" {
		t.Fatalf("SafetyReason still set after a full post-reset healthy run: %q", state.SafetyReason)
	}
	mustOpenEpoch(t, store, rootTestMAC)
}

// TestRecoveryHealthyPollsDerivesFromDwellTimeNotAConstant pins recoveryHealthyPolls to its
// derivation (ceil(recoveryDwellTime / MetricsTime)) so a future edit to either constant is caught
// here instead of silently changing how long COOLDOWN dwells.
func TestRecoveryHealthyPollsDerivesFromDwellTimeNotAConstant(t *testing.T) {
	for _, testCase := range []struct {
		metricsTime time.Duration
		want        int
	}{
		{metricsTime: 10 * time.Second, want: 6},
		{metricsTime: 30 * time.Second, want: 2},
		{metricsTime: 60 * time.Second, want: 1},
		{metricsTime: 90 * time.Second, want: 1},
		{metricsTime: 0, want: 1},
	} {
		settings := lib.Settings{MetricsTime: testCase.metricsTime}
		if got := recoveryHealthyPolls(settings); got != testCase.want {
			t.Fatalf("recoveryHealthyPolls(MetricsTime=%s) = %d, want %d", testCase.metricsTime, got, testCase.want)
		}
	}
}

// TestFreshEmergencyEpisodeResetsStaleRecoveryHealthyCount is a regression test for a bug found in
// adversarial review of the recovery predicate: RecoveryHealthyCount only ever advances while
// COOLDOWN is actively counting toward recoveryHealthyPolls, but nothing reset it when a second,
// distinct emergency interrupted a COOLDOWN dwell already in progress. A stale nonzero count would
// then resume, letting the new episode's recovery predicate reach its threshold after fewer than
// recoveryHealthyPolls polls actually proving THIS episode safe — a real weakening of the safety
// control. transitionEmergencyState is the shared computation every emergency escalation path
// (enforceMinerSafety's unsupported-grid branch, recordUnreadablePoll, rollbackForSafety,
// mutation.go's supersedeReadback/handleTerminalMutationFailureLocked) eventually funnels a fresh
// episode through, so proving it resets the count here covers all of them.
func TestFreshEmergencyEpisodeResetsStaleRecoveryHealthyCount(t *testing.T) {
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	state := lib.MinerState{
		MacAddr: rootTestMAC, Hostname: "root-test", Phase: lib.PhaseCooldown,
		PhaseStartedAt: now, SafetyReason: lib.SafetyReasonFirmwareOverheat,
		RecoveryHealthyCount: 3,
	}
	state.SetCurrentPoint(point)
	info := rootTestInfo(point, 100)
	info.Temp = settings.TempCutoff + 5
	assessment := safetyAssessment{
		action:  safetyEmergencyHold,
		failure: safetyFailure{status: string(lib.PointThermal), reason: "test-induced fresh emergency"},
	}
	if _, err := transitionEmergencyState(&state, info, asic, settings, now, assessment); err != nil {
		t.Fatalf("transitionEmergencyState: %v", err)
	}
	if state.Phase != lib.PhaseEmergency {
		t.Fatalf("fresh emergency did not enter EMERGENCY: %+v", state)
	}
	if state.RecoveryHealthyCount != 0 {
		t.Fatalf("stale RecoveryHealthyCount survived a fresh emergency episode: got %d, want 0", state.RecoveryHealthyCount)
	}
}

// TestSafetyRecoveryResumesPassAndIsNotSettled drives the complete durable recovery path. A
// successful safety-validation window must open a fresh baseline for the same pass, not manufacture
// a monitor conclusion.
func TestSafetyRecoveryResumesPassAndIsNotSettled(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	state := cooldownTestState(t, store, point, now)
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	info := rootTestInfo(point, 100)
	threshold := recoveryHealthyPolls(settings)
	tick := 0
	nextTick := func() time.Time {
		tick++
		return now.Add(time.Duration(tick) * settings.MetricsTime)
	}
	for i := 0; i < threshold; i++ {
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, info, asic), settings, nextTick(), true); err != nil {
			t.Fatalf("control miner after safety (recovery poll %d): %v", i, err)
		}
	}
	if state.SafetyReason != "" {
		t.Fatalf("SafetyReason not cleared after the recovery threshold: %+v", state)
	}
	mustOpenEpoch(t, store, rootTestMAC) // the safety_validation epoch
	for i := 0; i < rampSamples(settings)+targetSampleCount(settings); i++ {
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, info, asic), settings, nextTick(), true); err != nil {
			t.Fatalf("control miner after safety (window poll %d): %v", i, err)
		}
	}
	if state.Phase != lib.PhaseBaseline || state.MonitorReason != "" || !state.SettledAt.IsZero() {
		t.Fatalf("miner did not resume its pass baseline: %+v", state)
	}
	if state.SafetyReason != "" {
		t.Fatalf("SafetyReason reappeared during pass resumption: %q", state.SafetyReason)
	}
	recoveryBaseline := mustOpenEpoch(t, store, rootTestMAC)
	if recoveryBaseline.Purpose != lib.EpochBaseline || recoveryBaseline.RequiredWindows != 2 ||
		recoveryBaseline.Point != state.CurrentPoint() {
		t.Fatalf("recovery baseline = %+v", recoveryBaseline)
	}
	observedAt := nextTick()
	if qualifiesSettledObservation(store, state, info, asic, settings, observedAt, false) {
		t.Fatal("resumed recovery baseline qualified as settled")
	}
	if minerController.verifiedSettledObservation(state, info, asic, settings, observedAt) {
		t.Fatal("resumed recovery baseline was classified as settled accounting")
	}
}

// TestSafetyInterruptedCandidateIsConsumedAndRecoveryContinuesFrontier reproduces the incident
// shape: 400/1060 was reserved but never became observable before emergency recovery returned the
// miner to 400/1000. Recovery must preserve 400/1060 as consumed, revalidate the safe incumbent,
// and continue with the next unseen frequency candidate instead of manufacturing a monitor result or
// retrying the interrupted voltage point.
func TestSafetyInterruptedCandidateIsConsumedAndRecoveryContinuesFrontier(t *testing.T) {
	store, err := lib.OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 10, 10, 18, 39, 0, time.UTC)
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	interrupted := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}
	next := lib.OperatingPoint{Frequency: 490, CoreVoltage: 1000}
	bootstrap, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(minimum, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	admitted := admitTestTrial(t, store, bootstrap.State, interrupted, lib.PhaseVoltageTest, 100, now.Add(time.Second))
	state := admitted.State
	expected := state
	state.SetFallbackPoint(lib.OperatingPoint{})
	state.ClearPendingMutation()
	state.Phase = lib.PhaseCooldown
	state.PhaseStartedAt = now.Add(2 * time.Second)
	state.SafetyReason = lib.SafetyReasonFirmwareOverheat
	result, err := store.Apply(lib.SafetyTransition{
		Expected: expected, State: state,
	}, state.PhaseStartedAt)
	if err != nil {
		t.Fatal(err)
	}
	state = result.State
	records, err := store.ListPoints(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	consumed, found := findRecord(records, interrupted)
	if !found || consumed.Status != lib.PointUnobservable || consumed.EvidenceEpochID != 0 {
		t.Fatalf("safety-interrupted point = %+v, found=%t", consumed, found)
	}

	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	info := rootTestInfo(minimum, 100)
	at := state.PhaseStartedAt
	for poll := 0; poll < recoveryHealthyPolls(settings); poll++ {
		at = at.Add(settings.MetricsTime)
		if err := minerController.controlMinerAfterSafety(
			context.Background(), &state, mustReadablePoll(t, info, asic), settings, at, true,
		); err != nil {
			t.Fatalf("recovery poll %d: %v", poll, err)
		}
	}
	safetyEpoch := mustOpenEpoch(t, store, rootTestMAC)
	if safetyEpoch.Purpose != lib.EpochSafetyValidation {
		t.Fatalf("recovery epoch = %+v", safetyEpoch)
	}
	at = at.Add(settings.EvaluationWindowTime)
	if err := minerController.finishSafetyValidation(
		&state, safetyEpoch, mustWindow(t, 100, 100, 48, 48, 44, 12, nil), settings, at,
	); err != nil {
		t.Fatal(err)
	}
	recoveryBaseline := mustOpenEpoch(t, store, rootTestMAC)
	at = at.Add(settings.EvaluationWindowTime)
	if err := minerController.evaluateBaseline(
		context.Background(), &state, recoveryBaseline,
		mustWindow(t, 100, 100, 48, 48, 44, 12, nil), asic, settings, at,
	); err != nil {
		t.Fatal(err)
	}
	if state.Phase != lib.PhaseFrequencyTest || state.PendingKind != lib.MutationOperatingPoint ||
		state.PendingPoint() != next || state.PendingPoint() == interrupted || state.MonitorReason != "" {
		t.Fatalf("frontier did not continue past interrupted point: %+v", state)
	}
	records, err = store.ListPoints(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	consumed, found = findRecord(records, interrupted)
	if !found || consumed.Status != lib.PointUnobservable {
		t.Fatalf("interrupted point was reopened: %+v, found=%t", consumed, found)
	}
	entered, found := findRecord(records, next)
	if !found || entered.Status != lib.PointEntered || entered.EntryAttemptID <= admitted.AttemptID {
		t.Fatalf("next frontier point = %+v, found=%t", entered, found)
	}
}

func TestSafetyRecoveryAtUnseenMinimumPreservesPassAnchorAndReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := lib.OpenOptimizerStore(path)
	if err != nil {
		t.Fatal(err)
	}
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 10, 11, 0, 0, 0, time.UTC)
	rootPoint := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1150}
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	bootstrap, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(rootPoint, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	expected := bootstrap.State
	state := expected
	state.Phase = lib.PhaseCooldown
	state.PhaseStartedAt = now.Add(time.Second)
	state.SafetyReason = lib.SafetyReasonASICLimit
	state.SetPendingMutation(lib.MutationSafetyRollback, minimum, state.PhaseStartedAt)
	result, err := store.Apply(lib.SafetyTransition{Expected: expected, State: state}, state.PhaseStartedAt)
	if err != nil {
		t.Fatal(err)
	}
	state = result.State
	attempt := lib.MutationAttempt{
		MacAddr: state.MacAddr, Kind: lib.MutationSafetyRollback, Reason: state.SafetyReason,
		FromFrequency: rootPoint.Frequency, FromCoreVoltage: rootPoint.CoreVoltage,
		TargetFrequency: minimum.Frequency, TargetCoreVoltage: minimum.CoreVoltage,
		IntentCreatedAt: state.PendingSince, StartedAt: state.PendingSince,
		ConfiguredVerifiedUptimeSeconds: -1,
	}
	id, err := store.StartMutationAttempt(&attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, lib.MutationMilestonePatchRequested, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordConfiguredVerification(id, now.Add(3*time.Second), 101); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, lib.MutationMilestoneRestartRequested, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, lib.MutationMilestoneRebootVerified, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	result, err = store.Apply(lib.CompleteMutation{
		MacAddr: state.MacAddr, IP: state.IP, AttemptID: id,
	}, now.Add(6*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	state = result.State
	if err := store.RecordFirstPositive(id, now.Add(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	result, err = store.Apply(lib.CompleteResume{MacAddr: state.MacAddr, AttemptID: id}, now.Add(8*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	state = result.State
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	info := rootTestInfo(minimum, 10)
	at := now.Add(8 * time.Second)
	for poll := 0; poll < recoveryHealthyPolls(settings); poll++ {
		at = at.Add(settings.MetricsTime)
		if err := minerController.controlMinerAfterSafety(
			context.Background(), &state, mustReadablePoll(t, info, asic), settings, at, true,
		); err != nil {
			t.Fatal(err)
		}
	}
	safetyEpoch := mustOpenEpoch(t, store, state.MacAddr)
	at = at.Add(settings.EvaluationWindowTime)
	if err := minerController.finishSafetyValidation(
		&state, safetyEpoch, mustWindow(t, 100, 100, 48, 48, 44, 12, nil), settings, at,
	); err != nil {
		t.Fatal(err)
	}
	recoveryEpoch := mustOpenEpoch(t, store, state.MacAddr)
	at = at.Add(settings.EvaluationWindowTime)
	if err := minerController.evaluateBaseline(
		context.Background(), &state, recoveryEpoch,
		mustWindow(t, 100, 100, 48, 48, 44, 12, nil), asic, settings, at,
	); err != nil {
		t.Fatal(err)
	}
	records, err := store.ListPoints(state.MacAddr)
	if err != nil {
		t.Fatal(err)
	}
	root, found := findRecord(records, rootPoint)
	if !found || root.Status != lib.PointUnobservable || !root.EnteredAt.Equal(state.PassStartedAt) {
		t.Fatalf("interrupted pass anchor = %+v, found=%t", root, found)
	}
	recovery, found := findRecord(records, minimum)
	if !found || recovery.Status != lib.PointValidated || !recovery.EnteredAt.After(state.PassStartedAt) ||
		recovery.EvidenceEpochID != recoveryEpoch.ID {
		t.Fatalf("recovery point = %+v, found=%t", recovery, found)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := lib.OpenOptimizerStore(path)
	if err != nil {
		t.Fatalf("reopen recovery pass: %v", err)
	}
	defer reopened.Close()
	loaded, err := reopened.LoadMiner(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.PendingKind != lib.MutationOperatingPoint || loaded.PendingPoint() != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}) {
		t.Fatalf("reopened frontier state = %+v", loaded)
	}
}

// --- validated-point safety demotion ------------------------------------------------------------
//
// The incident these tests pin: once a point reached status "validated" nothing could contradict it
// again. A point that had become unsafe kept its historical statistics, final placement kept
// re-selecting it, the ASIC exceeded the hard limit again within minutes, and the miner looped
// through rollback and recovery every half hour without converging. Demotion ends the loop: the
// hard-limit verdict that rolled the miner back is durable evidence about the point, so the point
// leaves the feasible set and the next-best one is selected instead.

func mustPointRecord(t *testing.T, store *lib.OptimizerStore, macAddr string, point lib.OperatingPoint) lib.OperatingPointRecord {
	t.Helper()
	records, err := store.ListPoints(macAddr)
	if err != nil {
		t.Fatal(err)
	}
	record, found := findRecord(records, point)
	if !found {
		t.Fatalf("miner %s has no durable row at %d/%d", macAddr, point.Frequency, point.CoreVoltage)
	}
	return record
}

// revalidatedStore reopens the database, which is the only way to run lib's full-state invariant
// checker: it runs on every open, over every table at once. The reopened store replaces the closed
// one so a test can keep driving the same miner afterwards.
func revalidatedStore(t *testing.T, store *lib.OptimizerStore, path string) *lib.OptimizerStore {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatalf("close store before revalidation: %v", err)
	}
	reopened, err := lib.OpenOptimizerStore(path)
	if err != nil {
		t.Fatalf("durable state failed full-state invariant validation: %v", err)
	}
	return reopened
}

// baselineRecordForIncumbent mirrors evaluateBaseline's own record construction: a completed
// baseline restates the durable incumbent's entry provenance, which differs between the pass
// baseline row (no entry attempt, no reference hash) and the row a promoted trial left behind. An
// incumbent that already carries a terminal verdict keeps its stored measurements — CompleteBaseline
// leaves such a row untouched — so the record only has to stay consistent with what is stored.
func baselineRecordForIncumbent(
	t *testing.T,
	store *lib.OptimizerStore,
	state lib.MinerState,
	hash float64,
	p95Temp float64,
	at time.Time,
) lib.OperatingPointRecord {
	t.Helper()
	incumbent := mustPointRecord(t, store, state.MacAddr, state.CurrentPoint())
	record := lib.OperatingPointRecord{
		MacAddr: state.MacAddr, Frequency: state.CurrentFrequency, CoreVoltage: state.CurrentCoreVoltage,
		Status: lib.PointValidated, MedianHash: hash, ExpectedHash: hash, Attainment: 1,
		MeanTemp: p95Temp - 2, P95Temp: p95Temp, P95VRTemp: 70, P95Power: 18, MeasuredAt: at,
		EnteredAt: incumbent.EnteredAt, EntryAttemptID: incumbent.EntryAttemptID,
		ReferenceHash: incumbent.ReferenceHash,
	}
	if incumbent.Status != lib.PointEntered {
		record.MedianHash, record.ExpectedHash = incumbent.MedianHash, incumbent.ExpectedHash
		record.Attainment = incumbent.Attainment
		record.MeanTemp, record.P95Temp = incumbent.MeanTemp, incumbent.P95Temp
		record.P95VRTemp, record.P95Power = incumbent.P95VRTemp, incumbent.P95Power
	}
	return record
}

// completeTestMutation walks one durable mutation attempt through the milestone ledger the mutation
// coordinator writes against real hardware — patch, configured readback, restart, reboot proof,
// first positive share — and lands the miner on the attempt's target point.
func completeTestMutation(
	t *testing.T,
	store *lib.OptimizerStore,
	state lib.MinerState,
	attemptID int64,
	at time.Time,
) (lib.MinerState, time.Time) {
	t.Helper()
	if err := store.AdvanceMutationAttempt(attemptID, lib.MutationMilestonePatchRequested, at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordConfiguredVerification(attemptID, at.Add(2*time.Second), 101); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(attemptID, lib.MutationMilestoneRestartRequested, at.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(attemptID, lib.MutationMilestoneRebootVerified, at.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	result, err := store.Apply(lib.CompleteMutation{
		MacAddr: state.MacAddr, IP: state.IP, AttemptID: attemptID,
	}, at.Add(5*time.Second))
	if err != nil {
		t.Fatalf("complete test mutation: %v", err)
	}
	if err := store.RecordFirstPositive(attemptID, at.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	result, err = store.Apply(lib.CompleteResume{MacAddr: result.State.MacAddr, AttemptID: attemptID}, at.Add(7*time.Second))
	if err != nil {
		t.Fatalf("resume test mutation: %v", err)
	}
	return result.State, at.Add(7 * time.Second)
}

// promoteTestTrial drives one complete trial to promotion: it closes the open baseline epoch at the
// durable incumbent, admits candidate, completes the mutation, and finalizes the candidate as a
// validated point the miner is now running. It is the only way to produce a trial-derived validated
// row (EntryAttemptID > 0), one of the two shapes a demotion has to handle, and the candidate is
// named explicitly rather than taken from nextCandidate so a test can build an exhausted frontier
// without walking all thirty-six pairs.
func promoteTestTrial(
	t *testing.T,
	store *lib.OptimizerStore,
	state lib.MinerState,
	candidate lib.OperatingPoint,
	hash float64,
	p95Temp float64,
	at time.Time,
) (lib.MinerState, time.Time) {
	t.Helper()
	epoch := mustOpenEpoch(t, store, state.MacAddr)
	baseline := baselineRecordForIncumbent(t, store, state, 100, 55, at)
	referenceHash := state.BestHashRate
	if referenceHash <= 0 {
		referenceHash = baseline.MedianHash
	}
	admitted, err := store.Apply(lib.CompleteBaseline{
		State: state, Record: baseline, Epoch: epoch, Decision: lib.BaselineContinue,
		Candidate: candidate, CandidatePhase: lib.PhaseVoltageTest, ReferenceHash: referenceHash,
	}, at)
	if err != nil {
		t.Fatalf("admit trial at %d/%d: %v", candidate.Frequency, candidate.CoreVoltage, err)
	}
	state, at = completeTestMutation(t, store, admitted.State, admitted.AttemptID, at)
	at = at.Add(time.Second)
	entered := mustPointRecord(t, store, state.MacAddr, candidate)
	measured := lib.OperatingPointRecord{
		MacAddr: state.MacAddr, Frequency: candidate.Frequency, CoreVoltage: candidate.CoreVoltage,
		Status: lib.PointValidated, MedianHash: hash, ExpectedHash: hash, Attainment: 1,
		MeanTemp: p95Temp - 2, P95Temp: p95Temp, P95VRTemp: 70, P95Power: 18, MeasuredAt: at,
		EnteredAt: entered.EnteredAt, EntryAttemptID: entered.EntryAttemptID,
		ReferenceHash: entered.ReferenceHash,
	}
	promoted, err := store.Apply(lib.FinalizeTrial{
		State: state, Record: measured, Decision: lib.TrialPromote,
		Epoch: mustOpenEpoch(t, store, state.MacAddr),
	}, at)
	if err != nil {
		t.Fatalf("promote trial at %d/%d: %v", candidate.Frequency, candidate.CoreVoltage, err)
	}
	return promoted.State, at
}

// startTestMutationAttempt records the durable attempt the mutation coordinator would open for
// whatever operating-point intent the miner is currently carrying.
func startTestMutationAttempt(t *testing.T, store *lib.OptimizerStore, state lib.MinerState) int64 {
	t.Helper()
	attempt := lib.MutationAttempt{
		MacAddr: state.MacAddr, Kind: state.PendingKind, Reason: state.SafetyReason,
		FromFrequency: state.CurrentFrequency, FromCoreVoltage: state.CurrentCoreVoltage,
		TargetFrequency: state.PendingFrequency, TargetCoreVoltage: state.PendingCoreVoltage,
		IntentCreatedAt: state.PendingSince, StartedAt: state.PendingSince,
		ConfiguredVerifiedUptimeSeconds: -1,
	}
	if state.PendingKind == lib.MutationOperatingPoint {
		attempt.Reason = ""
	}
	id, err := store.StartMutationAttempt(&attempt)
	if err != nil {
		t.Fatalf("start %s attempt: %v", state.PendingKind, err)
	}
	return id
}

// hardLimitPoll is telemetry above the hard temperature limit but below the host cutoff: the exact
// reading that produced every asic_limit rollback in the incident.
func hardLimitPoll(point lib.OperatingPoint, settings lib.Settings) lib.Info {
	info := rootTestInfo(point, 600)
	info.Temp = settings.TempLimit + 2
	return info
}

// TestPlacedValidatedPointDemotedByHardLimitLeavesTheSelectableSet is the incident's mineiro shape:
// final placement settles on the best validated point, the ASIC exceeds the hard limit there, and
// the miner rolls back. The point must end the episode demoted, so the recovery baseline that
// follows selects the next-best point instead of steering straight back into the same overheat.
func TestPlacedValidatedPointDemotedByHardLimitLeavesTheSelectableSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := lib.OpenOptimizerStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)
	baselinePoint := lib.OperatingPoint{Frequency: 625, CoreVoltage: 1150}
	survivor := lib.OperatingPoint{Frequency: 625, CoreVoltage: 1200}
	unsafePoint := lib.OperatingPoint{Frequency: 625, CoreVoltage: 1250}
	bootstrap, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(baselinePoint, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	state := bootstrap.State
	at := now
	state, at = promoteTestTrial(t, store, state, survivor, 120, 60, at.Add(time.Minute))
	state, at = promoteTestTrial(t, store, state, unsafePoint, 140, 62, at.Add(time.Minute))
	// 1250 mV is the highest advertised voltage and 625 MHz the highest advertised frequency, and the
	// pair below is already measured, so the frontier is exhausted: this baseline places instead of
	// admitting another candidate.
	at = at.Add(time.Minute)
	if err := minerController.evaluateBaseline(
		context.Background(), &state, mustOpenEpoch(t, store, rootTestMAC),
		mustWindow(t, 140, 140, 60, 62, 70, 18, nil), asic, settings, at,
	); err != nil {
		t.Fatal(err)
	}
	if state.Phase != lib.PhaseMonitor || state.MonitorReason != lib.MonitorSelected ||
		state.CurrentPoint() != unsafePoint || state.PendingKind != "" {
		t.Fatalf("final placement did not settle on the best validated point: %+v", state)
	}
	placed := mustPointRecord(t, store, rootTestMAC, unsafePoint)
	if placed.Status != lib.PointValidated || placed.EntryAttemptID <= 0 || placed.EvidenceEpochID <= 0 {
		t.Fatalf("placed point is not a trial-derived validated row: %+v", placed)
	}

	at = at.Add(settings.MetricsTime)
	handled, err := minerController.enforceMinerSafety(
		context.Background(), &state, hardLimitPoll(unsafePoint, settings), asic, settings, at,
	)
	if err != nil || !handled {
		t.Fatalf("hard-limit poll handled=%t: %v", handled, err)
	}
	demoted := mustPointRecord(t, store, rootTestMAC, unsafePoint)
	if demoted.Status != lib.PointThermal {
		t.Fatalf("validated point survived a hard-limit trip as %q: %+v", demoted.Status, demoted)
	}
	if demoted.EntryAttemptID != placed.EntryAttemptID || !demoted.EnteredAt.Equal(placed.EnteredAt) ||
		demoted.ReferenceHash != placed.ReferenceHash || demoted.EvidenceEpochID != placed.EvidenceEpochID {
		t.Fatalf("demotion did not preserve entry provenance: before=%+v after=%+v", placed, demoted)
	}
	if state.BestPoint() != survivor || state.BestHashRate != 120 {
		t.Fatalf("best summary still points at the demoted point: %+v", state)
	}
	if state.Phase != lib.PhaseCooldown || state.PendingKind != lib.MutationSafetyRollback ||
		state.PendingPoint() != survivor || state.SafetyReason != lib.SafetyReasonASICLimit {
		t.Fatalf("hard-limit trip did not roll back onto the surviving point: %+v", state)
	}
	records, err := store.ListPoints(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	if selected, ok := selectFinalPoint(records, asic, settings); !ok || selected.Point() != survivor {
		t.Fatalf("final selection = %+v ok=%t, want %+v", selected.Point(), ok, survivor)
	}
	if target, err := minerController.bestRollbackPoint(&state, unsafePoint, asic, settings); err != nil || target != survivor {
		t.Fatalf("rollback candidate = %+v: %v", target, err)
	}
	store = revalidatedStore(t, store, path)
	minerController.states = store

	// The rest of the episode: the rollback lands, COOLDOWN dwells, and the recovery baseline
	// completes. Before the fix this is where the loop closed — the demoted point was still the
	// feasible maximum and placement steered right back into it.
	state, at = completeTestMutation(t, store, state, startTestMutationAttempt(t, store, state), at)
	if state.Phase != lib.PhaseCooldown || state.CurrentPoint() != survivor {
		t.Fatalf("rollback did not land on the surviving point: %+v", state)
	}
	for poll := 0; poll < recoveryHealthyPolls(settings); poll++ {
		at = at.Add(settings.MetricsTime)
		if err := minerController.controlMinerAfterSafety(
			context.Background(), &state, mustReadablePoll(t, rootTestInfo(survivor, 100), asic), settings, at, true,
		); err != nil {
			t.Fatalf("recovery poll %d: %v", poll, err)
		}
	}
	at = at.Add(settings.EvaluationWindowTime)
	if err := minerController.finishSafetyValidation(
		&state, mustOpenEpoch(t, store, rootTestMAC), mustWindow(t, 120, 120, 58, 60, 70, 18, nil), settings, at,
	); err != nil {
		t.Fatal(err)
	}
	at = at.Add(settings.EvaluationWindowTime)
	if err := minerController.evaluateBaseline(
		context.Background(), &state, mustOpenEpoch(t, store, rootTestMAC),
		mustWindow(t, 120, 120, 58, 60, 70, 18, nil), asic, settings, at,
	); err != nil {
		t.Fatal(err)
	}
	if state.PendingPoint() == unsafePoint {
		t.Fatalf("recovery baseline steered back into the demoted point: %+v", state)
	}
	if state.Phase != lib.PhaseMonitor || state.MonitorReason != lib.MonitorSelected ||
		state.CurrentPoint() != survivor || state.PendingKind != "" {
		t.Fatalf("recovery placement did not settle on the surviving point: %+v", state)
	}
	store = revalidatedStore(t, store, path)
	minerController.states = store
}

// TestSafetySupersededPlacementDemotesItsValidatedTarget is the incident's mineira shape: the hard
// limit is reached at the placement target while the placement mutation is still unfinished, so
// durable current never advanced. The attempt must close superseded and the target — the point the
// device was actually running when it overheated — must still be demoted.
func TestSafetySupersededPlacementDemotesItsValidatedTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := lib.OpenOptimizerStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)
	baselinePoint := lib.OperatingPoint{Frequency: 625, CoreVoltage: 1150}
	incumbent := lib.OperatingPoint{Frequency: 625, CoreVoltage: 1200}
	unsafePoint := lib.OperatingPoint{Frequency: 625, CoreVoltage: 1250}
	bootstrap, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(baselinePoint, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	state := bootstrap.State
	at := now
	// The higher-hash point is measured first, so the miner ends the pass running the lower one and
	// final placement is a real mutation rather than a hold.
	state, at = promoteTestTrial(t, store, state, unsafePoint, 140, 62, at.Add(time.Minute))
	state, at = promoteTestTrial(t, store, state, incumbent, 120, 60, at.Add(time.Minute))
	at = at.Add(time.Minute)
	if err := minerController.evaluateBaseline(
		context.Background(), &state, mustOpenEpoch(t, store, rootTestMAC),
		mustWindow(t, 120, 120, 58, 60, 70, 18, nil), asic, settings, at,
	); err != nil {
		t.Fatal(err)
	}
	if state.PendingKind != lib.MutationOperatingPoint || state.PendingPoint() != unsafePoint ||
		state.CurrentPoint() != incumbent {
		t.Fatalf("final placement did not target the best validated point: %+v", state)
	}
	attemptID := startTestMutationAttempt(t, store, state)

	// The device has already taken the new pair and overheats there while the ledger still shows the
	// mutation unfinished: the failure is observed at the target, not at durable current.
	at = at.Add(settings.MetricsTime)
	handled, err := minerController.enforceMinerSafety(
		context.Background(), &state, hardLimitPoll(unsafePoint, settings), asic, settings, at,
	)
	if err != nil || !handled {
		t.Fatalf("hard-limit poll handled=%t: %v", handled, err)
	}
	demoted := mustPointRecord(t, store, rootTestMAC, unsafePoint)
	if demoted.Status != lib.PointThermal {
		t.Fatalf("mid-placement target survived a hard-limit trip as %q: %+v", demoted.Status, demoted)
	}
	attempts, err := store.ListMutationAttempts(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	var placement lib.MutationAttempt
	for _, attempt := range attempts {
		if attempt.ID == attemptID {
			placement = attempt
		}
	}
	if placement.ID != attemptID || placement.FailedAt.IsZero() ||
		placement.FailureStage != lib.MutationFailureSafetySuperseded {
		t.Fatalf("placement attempt did not close superseded: %+v", placement)
	}
	if state.BestPoint() != incumbent || state.BestHashRate != 120 {
		t.Fatalf("best summary still points at the demoted target: %+v", state)
	}
	if state.Phase != lib.PhaseCooldown || state.PendingKind != lib.MutationSafetyRollback ||
		state.PendingPoint() != incumbent {
		t.Fatalf("superseded placement did not roll back onto the incumbent: %+v", state)
	}
	records, err := store.ListPoints(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	if selected, ok := selectFinalPoint(records, asic, settings); !ok || selected.Point() != incumbent {
		t.Fatalf("final selection = %+v ok=%t, want %+v", selected.Point(), ok, incumbent)
	}
	store = revalidatedStore(t, store, path)
	minerController.states = store
}

// TestBaselineDerivedValidatedPointIsDemotedByItsOwnOverheat covers the second durable shape a
// validated row has: the pass baseline itself, which carries no entry attempt. The incumbent
// overheating must demote it without tripping entry-authority validation, and must clear the best
// summary rather than leave it pointing at a point no longer validated.
func TestBaselineDerivedValidatedPointIsDemotedByItsOwnOverheat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := lib.OpenOptimizerStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	settings := rootTestSettings(t)
	asic := rootTestASIC()
	now := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)
	point := lib.OperatingPoint{Frequency: 490, CoreVoltage: 1060}
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	bootstrap, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(point, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	minerController := &controller{states: store, runtimes: make(map[string]*minerRuntime)}
	state := closeInitialBaselineEpoch(t, store, bootstrap.State, now.Add(time.Minute))
	baseline := mustPointRecord(t, store, rootTestMAC, point)
	if baseline.Status != lib.PointValidated || baseline.EntryAttemptID != 0 || baseline.EvidenceEpochID <= 0 {
		t.Fatalf("pass baseline is not a baseline-derived validated row: %+v", baseline)
	}
	at := now.Add(2 * time.Minute)
	handled, err := minerController.enforceMinerSafety(
		context.Background(), &state, hardLimitPoll(point, settings), asic, settings, at,
	)
	if err != nil || !handled {
		t.Fatalf("hard-limit poll handled=%t: %v", handled, err)
	}
	demoted := mustPointRecord(t, store, rootTestMAC, point)
	if demoted.Status != lib.PointThermal || demoted.EntryAttemptID != 0 ||
		demoted.EvidenceEpochID != baseline.EvidenceEpochID || !demoted.EnteredAt.Equal(baseline.EnteredAt) {
		t.Fatalf("baseline-derived demotion = %+v, want thermal keeping baseline epoch %d",
			demoted, baseline.EvidenceEpochID)
	}
	if state.BestPoint() != (lib.OperatingPoint{}) || state.BestHashRate != 0 {
		t.Fatalf("best summary outlived the only validated point: %+v", state)
	}
	if state.Phase != lib.PhaseCooldown || state.PendingKind != lib.MutationSafetyRollback ||
		state.PendingPoint() != minimum {
		t.Fatalf("overheating incumbent did not roll back to the advertised minimum: %+v", state)
	}
	records, err := store.ListPoints(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	if selected, ok := selectFinalPoint(records, asic, settings); ok {
		t.Fatalf("demoted point is still selectable: %+v", selected)
	}
	store = revalidatedStore(t, store, path)
	minerController.states = store
}
