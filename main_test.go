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
	if controller.entryMarginPositive(&lib.MinerState{}, entry, lib.Settings{}, time.Now().UTC()) {
		t.Fatal("entry margin accepted a missing frozen reference")
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
	finalizeResult, err := store.Apply(lib.FinalizeBaseline{State: state, Record: baseline, Block: false, Epoch: epoch}, baseline.MeasuredAt)
	if err != nil {
		t.Fatal(err)
	}
	state = finalizeResult.State
	boundarySettledAt := createdAt.Add(23 * time.Hour)
	state.Phase = lib.PhaseHold
	state.HoldReason = lib.HoldOptimized
	state.SettledAt = boundarySettledAt
	if _, err := store.Apply(lib.SaveState{State: state}, createdAt); err != nil {
		t.Fatal(err)
	}

	armStart := createdAt.Add(24 * time.Hour)
	passStart := armStart.Add(lib.ReportArmDuration)
	if _, err := store.Apply(lib.ResetPass{
		MacAddr: info.MacAddr, Point: oldPoint,
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
		Phase:              lib.PhaseHold,
		HoldReason:         lib.HoldOptimized,
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

// TestMaxRejectedWindowsStarvesBaselineAndOpensProbation drives a baseline evidence epoch's
// rejected-window budget to exhaustion and asserts the epoch-lifecycle consequence the RFC's
// "Terminal States Get Exit Predicates" describes: the epoch closes starved (verified indirectly, by
// reopening the durable store afterward and confirming validateCrossTableState's starved-HOLD
// invariants all hold), the miner lands in HoldStarved/PhaseHold exactly as a rejected baseline would
// except for the reason, and a probation successor opens at the same point immediately, in the same
// transaction.
func TestMaxRejectedWindowsStarvesBaselineAndOpensProbation(t *testing.T) {
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

	if state.Phase != lib.PhaseHold || state.HoldReason != lib.HoldStarved || !state.SettledAt.IsZero() {
		t.Fatalf("starved baseline did not land in HoldStarved/HOLD: %+v", state)
	}
	if state.CurrentPoint() != point {
		t.Fatalf("starvation changed the durable current point: %+v", state)
	}
	epoch, open, err := store.OpenEvidenceEpochFor(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	if !open || epoch.Purpose != lib.EpochProbation || epoch.Point != point || epoch.RequiredWindows != 1 {
		t.Fatalf("starved baseline did not open a probation successor at the same point: open=%t epoch=%+v", open, epoch)
	}
	if epoch.Progress.SettledSamples() != 0 {
		t.Fatalf("fresh probation epoch has nonzero settled samples: %+v", epoch.Progress)
	}
	points, err := store.ListPoints(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	if record, found := findRecord(points, point); !found || record.Status != lib.PointEntered {
		t.Fatalf("starvation wrote or discarded the baseline's entered row: %+v", points)
	}

	// Reopening the store re-runs full schema and cross-table validation (validateStoredOptimizerData
	// -> validateCrossTableState), including this commit's starved-HOLD invariants (an open epoch
	// exists and is exactly Probation). A structurally invalid starved/probation shape would fail
	// here even though every direct assertion above passed.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := lib.OpenOptimizerStore(dbPath)
	if err != nil {
		t.Fatalf("reopen after starvation failed cross-table validation: %v", err)
	}
	defer reopened.Close()
}

// TestStarvedHoldAutoExitsOnceEnvironmentRecovers is the direct behavioral counterpart of
// TestMaxRejectedWindowsStarvesBaselineAndOpensProbation: once a probation epoch is open, it
// continues driving polls at a clean rate and asserts the RFC's exit predicate fires exactly at
// windowMinSamples consecutive admitted samples — no timer, a sample count — reopening the identical
// baseline evaluation (same point, EpochBaseline, 2 required windows) that starvation interrupted.
func TestStarvedHoldAutoExitsOnceEnvironmentRecovers(t *testing.T) {
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
	if state.Phase != lib.PhaseHold || state.HoldReason != lib.HoldStarved {
		t.Fatalf("setup did not reach a starved HOLD: %+v", state)
	}

	// windowMinSamples consecutive clean polls: enough for probation's exit predicate, one short of
	// which must not exit (checked at each step below).
	target := windowMinSamples(settings)
	for i := 1; i <= target; i++ {
		cursor = cursor.Add(settings.MetricsTime)
		info := rootTestInfo(point, 100)
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, info, asic), settings, cursor, true); err != nil {
			t.Fatalf("control miner after safety (probation sample %d) at %s: %v", i, cursor, err)
		}
		if i < target {
			if state.Phase != lib.PhaseHold || state.HoldReason != lib.HoldStarved {
				t.Fatalf("starved HOLD exited before windowMinSamples samples were admitted (at sample %d of %d): %+v", i, target, state)
			}
		}
	}

	if state.Phase != lib.PhaseBaseline || state.HoldReason != "" {
		t.Fatalf("starved HOLD did not auto-exit at windowMinSamples samples: %+v", state)
	}
	epoch, open, err := store.OpenEvidenceEpochFor(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	if !open || epoch.Purpose != lib.EpochBaseline || epoch.Point != point || epoch.RequiredWindows != 2 {
		t.Fatalf("probation did not reopen the interrupted baseline evaluation: open=%t epoch=%+v", open, epoch)
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

// TestRejectedHoldNeverAutoExits is the negative counterpart the RFC's starvation-exit verification
// explicitly requires alongside the positive case: a HoldRejected miner (a real measured, terminal
// conclusion about the hardware) must never re-arm on its own, unlike HoldStarved. It drives a
// baseline to a genuine quality failure (median hash rate of exactly zero, an always-unhealthy
// window), then polls it for far longer than the starved case's exit predicate would ever need, and
// asserts the miner never leaves HoldRejected and no evidence epoch ever reopens.
func TestRejectedHoldNeverAutoExits(t *testing.T) {
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
	// blocks with a real (if bad) measurement, landing HoldRejected.
	var lastTick time.Time
	for _, tick := range pollSequence(now, settings, 1.0, rampSamples(settings)+2*targetSampleCount(settings)) {
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, unhealthyInfo(), asic), settings, tick, true); err != nil {
			t.Fatalf("control miner after safety (quality failure setup) at %s: %v", tick, err)
		}
		lastTick = tick
	}
	if state.Phase != lib.PhaseHold || state.HoldReason != lib.HoldRejected || !state.SettledAt.IsZero() {
		t.Fatalf("setup did not reach a rejected HOLD: %+v", state)
	}
	points, err := store.ListPoints(rootTestMAC)
	if err != nil {
		t.Fatal(err)
	}
	record, found := findRecord(points, point)
	if !found || record.Status != lib.PointUnstable || record.EvidenceEpochID <= 0 {
		t.Fatalf("rejected baseline did not carry a real measured record: %+v", points)
	}

	// Drive far more polls than the starved case's windowMinSamples exit predicate would ever need,
	// including a run of clean samples at the exact rejected point, to confirm nothing re-arms.
	cursor := lastTick
	for i := 0; i < 4*windowMinSamples(settings); i++ {
		cursor = cursor.Add(settings.MetricsTime)
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, rootTestInfo(point, 100), asic), settings, cursor, true); err != nil {
			t.Fatalf("control miner after safety (rejected hold poll %d) at %s: %v", i, cursor, err)
		}
	}
	if state.Phase != lib.PhaseHold || state.HoldReason != lib.HoldRejected || !state.SettledAt.IsZero() {
		t.Fatalf("rejected HOLD did not remain terminal: %+v", state)
	}
	if _, open, err := store.OpenEvidenceEpochFor(rootTestMAC); err != nil || open {
		t.Fatalf("rejected HOLD reopened an evidence epoch: open=%t err=%v", open, err)
	}
}

// TestProbationAtDegradedRateDoesNotOscillate answers the RFC's own flagged material uncertainty
// (the report accompanying this commit calls it out explicitly): at a delivered-tick rate low enough
// that individual windows would fail admission (rate = 0.4, the same period-5 deliver/drop/drop/
// deliver/drop pattern TestWindowClosesOnSpanAndAdmitsAtWindowMinSamples uses), does a probation
// epoch itself starve and reopen another probation, oscillating indefinitely? By this commit's
// mechanics it structurally cannot: probation never evaluates a window or a rejected-window budget at
// all, only a plain consecutive-sample count with no reset except contradiction, so a degraded but
// nonzero delivery rate can only ever slow the exit down, never restart or oscillate it. This test
// observes and records that directly, without needing to add a rescue.
func TestProbationAtDegradedRateDoesNotOscillate(t *testing.T) {
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
	if state.Phase != lib.PhaseHold || state.HoldReason != lib.HoldStarved {
		t.Fatalf("setup did not reach a starved HOLD: %+v", state)
	}
	probationEpoch, open, err := store.OpenEvidenceEpochFor(rootTestMAC)
	if err != nil || !open || probationEpoch.Purpose != lib.EpochProbation {
		t.Fatalf("setup did not open a probation epoch: open=%t epoch=%+v err=%v", open, probationEpoch, err)
	}
	probationEpochID := probationEpoch.ID

	// Period-5 "deliver, drop, drop, deliver, drop" (delivered at tick%5 == 0 or 3): exactly the 0.4
	// delivered-tick rate the RFC's own verification section calls out for this measurement, well
	// below the 0.5 threshold windows need to survive admission at all.
	var reopenedAt time.Time
	reopened := false
	for tick := 1; tick <= 300; tick++ {
		if tick%5 != 0 && tick%5 != 3 {
			continue
		}
		cursor = cursor.Add(settings.MetricsTime)
		if err := minerController.controlMinerAfterSafety(context.Background(), &state, mustReadablePoll(t, rootTestInfo(point, 100), asic), settings, cursor, true); err != nil {
			t.Fatalf("control miner after safety (degraded probation tick %d) at %s: %v", tick, cursor, err)
		}
		epoch, open, err := store.OpenEvidenceEpochFor(rootTestMAC)
		if err != nil {
			t.Fatal(err)
		}
		if open && epoch.Purpose == lib.EpochProbation && epoch.ID != probationEpochID {
			t.Fatalf("probation reopened a second probation epoch at tick %d instead of accumulating on the first: original=%d new=%+v", tick, probationEpochID, epoch)
		}
		if open && epoch.Purpose == lib.EpochProbation && epoch.Progress.RejectedWindows() != 0 {
			t.Fatalf("probation epoch accumulated a rejected window at tick %d: %+v", tick, epoch.Progress)
		}
		if state.Phase == lib.PhaseBaseline {
			reopened = true
			reopenedAt = cursor
			break
		}
	}
	if !reopened {
		t.Fatalf("probation never exited at a 0.4 delivered-tick rate within the poll budget: state=%+v", state)
	}
	t.Logf("observation: at a 0.4 delivered-tick rate, the single probation epoch (id %d) opened by starvation at %s exited cleanly at %s with no reopen and no rejected window ever recorded against it — probation's pure sample-count race is immune to the admission-rate degradation that would starve a normal window-evaluating epoch", probationEpochID, now, reopenedAt)
}

// mustOpenEpoch and closeInitialBaselineEpoch mirror the lib-package test helpers of the same
// purpose: production code always closes a miner's baseline epoch (FinalizeBaseline) before any
// later transition that requires no epoch be open, and tests that skip straight to that later
// transition need to do the same.
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
	result, err := store.Apply(lib.FinalizeBaseline{State: state, Record: record, Block: false, Epoch: epoch}, at)
	if err != nil {
		t.Fatalf("close initial baseline epoch: %v", err)
	}
	return result.State
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
	bootstrapResult, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(sourcePoint, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrapResult.State
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

func TestReopenedPatchStageOwnsConfiguredReadbackDeadline(t *testing.T) {
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
	coordinator.now = func() time.Time { return startedAt.Add(defaultRebootDeadline) }
	result := coordinator.execute(context.Background(), mutationRequest{
		macAddr: rootTestMAC, ip: "192.0.2.12", kind: lib.MutationOperatingPoint,
		point: target, settings: rootTestSettings(t), attempt: lib.MutationAttempt{
			MacAddr: rootTestMAC, Kind: lib.MutationOperatingPoint,
			FromFrequency: 525, FromCoreVoltage: 1150,
			TargetFrequency: 525, TargetCoreVoltage: 1100,
			IntentCreatedAt: startedAt, StartedAt: startedAt,
			PatchRequestedAt: startedAt, ConfiguredVerifiedUptimeSeconds: -1,
		},
	})
	if result.err == nil || result.failureStage != lib.MutationFailureConfiguredVerification {
		t.Fatalf("expired configured stage result = %+v", result)
	}
	if device.asicCalls != 0 || device.infoCalls != 0 {
		t.Fatalf("expired configured stage re-ran preflight: ASIC=%d info=%d", device.asicCalls, device.infoCalls)
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
			if nowCalls == 1 {
				return startedAt
			}
			return startedAt.Add(defaultRebootDeadline)
		},
	}
	info, terminal, err := coordinator.waitForConfiguredReadback(
		context.Background(),
		mutationRequest{
			macAddr: rootTestMAC, ip: "192.0.2.12", kind: lib.MutationOperatingPoint,
			point: target, settings: settings,
		},
		startedAt,
	)
	if err == nil || !terminal || info.MacAddr != rootTestMAC {
		t.Fatalf("late configured readback = info:%+v terminal:%t err:%v", info, terminal, err)
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
	miner, _, err := coordinator.waitForVerifiedBoot(
		context.Background(),
		mutationRequest{
			macAddr: rootTestMAC, ip: "192.0.2.12", kind: lib.MutationOperatingPoint,
			point: target, settings: settings, bootProofSameProcess: true,
		},
		100, configuredAt, restartAt,
	)
	if err == nil || miner.Info.MacAddr != rootTestMAC {
		t.Fatalf("late reboot proof = miner:%+v err:%v", miner, err)
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
	if err := verifyConfiguredReadback(request, wrong); err == nil {
		t.Fatal("wrong configured pair was accepted")
	}
	unsafe := rootTestInfo(target, 100)
	unsafe.Temp = settings.TempLimit + 1
	if err := verifyConfiguredReadback(request, unsafe); err == nil {
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
	bootstrapResult, err := store.Apply(lib.Bootstrap{
		Info: rootTestInfo(sourcePoint, 100), IP: "192.0.2.12", PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	state := bootstrapResult.State
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
// COOLDOWN/OVERHEAT, must stay silent otherwise, and must never itself change durable state. The
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
		t.Fatalf("instrumentation logged outside COOLDOWN/OVERHEAT: %q", buffer.String())
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
	if _, err := transitionEmergencyState(&state, info, asic, settings, now, assessment, true); err != nil {
		t.Fatalf("transitionEmergencyState: %v", err)
	}
	if state.Phase != lib.PhaseOverheat {
		t.Fatalf("fresh emergency did not enter OVERHEAT: %+v", state)
	}
	if state.RecoveryHealthyCount != 0 {
		t.Fatalf("stale RecoveryHealthyCount survived a fresh emergency episode: got %d, want 0", state.RecoveryHealthyCount)
	}
}

// TestSettledHoldSafetyQualifiesForRetuneAndSettledAccounting is a regression test for a bug found
// in adversarial review: qualifiesSettledObservation's HoldSafety branch used to require
// SafetyReason != "", an invariant from before this session's change, when SafetyReason stayed
// latched through HoldSafety until an operator acted. Now that the COOLDOWN recovery predicate
// clears SafetyReason in the same transition that opens the safety_validation epoch
// finishSafetyHold later closes, every real settled HoldSafety has SafetyReason == "" — the old
// check was permanently false for the real path, silently breaking --retune (advanceRetuneLocked
// calls this same function) and hourly "settled" accounting classification for any miner that ever
// overheats and recovers. This test drives the real end-to-end path (COOLDOWN -> recovery
// predicate -> safety_validation epoch -> closed window -> HoldSafety) instead of hand-constructing
// the state, so it fails if that invariant regresses again.
func TestSettledHoldSafetyQualifiesForRetuneAndSettledAccounting(t *testing.T) {
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
	if state.Phase != lib.PhaseHold || state.HoldReason != lib.HoldSafety || state.SettledAt.IsZero() {
		t.Fatalf("miner did not settle into HoldSafety: %+v", state)
	}
	if state.SafetyReason != "" {
		t.Fatalf("SafetyReason reappeared by settlement: %q", state.SafetyReason)
	}
	settledAt := nextTick()
	if !qualifiesSettledObservation(store, state, info, asic, settings, settledAt, false) {
		t.Fatal("a genuinely recovered, settled HoldSafety miner did not qualify as a settled observation")
	}
	if !minerController.verifiedSettledObservation(state, info, asic, settings, settledAt) {
		t.Fatal("verifiedSettledObservation rejected a genuinely recovered, settled HoldSafety miner")
	}
}
