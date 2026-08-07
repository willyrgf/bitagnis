package lib

import (
	"math"
	"testing"
	"time"
)

func TestSummarizeReportMinerUsesFullWallDurationAndMergesExposure(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	window := ReportWindow{Start: from, End: from.Add(24 * time.Hour)}
	metrics, err := SummarizeReportMiner(ReportMinerInput{
		Hostname:              "treatment",
		PreArmSettledHashRate: 100,
		Hourly: []HourlyAggregate{
			{MacAddr: testMAC, HourStartedAt: from, ObservedDuration: time.Hour, ActualHashSeconds: 360000},
			{MacAddr: testMAC, HourStartedAt: from.Add(time.Hour), UnknownGapDuration: time.Hour},
		},
		MutationAttempts: []MutationAttempt{
			{ID: 1, MacAddr: testMAC, Kind: MutationOperatingPoint, IntentCreatedAt: from, StartedAt: from, RestartRequestedAt: from.Add(30 * time.Minute), MiningResumedAt: from.Add(90 * time.Minute)},
			{ID: 2, MacAddr: testMAC, Kind: MutationMiningConfiguration, IntentCreatedAt: from, StartedAt: from, RestartRequestedAt: from.Add(2 * time.Hour), MiningResumedAt: from.Add(3 * time.Hour)},
		},
	}, window)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Coverage != 3600.0/86400.0 || metrics.NormalizedWork != 360000.0/(100*86400) {
		t.Fatalf("normalization = %+v", metrics)
	}
	if metrics.Restart.NormalRequests != 1 || metrics.Restart.SafetyRequests != 0 || metrics.Restart.NormalExposureSeconds != 3600 {
		t.Fatalf("restart exposure = %+v", metrics.Restart)
	}
}

func TestReportBoundaryRequiresCompleteCanonicalHistoricalEvidence(t *testing.T) {
	start := time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)
	valid := ReportMinerInput{
		PreArmSettledHashRate: 100,
		BoundaryPoint:         OperatingPoint{Frequency: 525, CoreVoltage: 1150},
		BoundarySettledAt:     start.Add(-time.Minute),
	}
	cases := []struct {
		name  string
		input ReportMinerInput
		want  bool
	}{
		{name: "valid", input: valid, want: true},
		{name: "off grid", input: func() ReportMinerInput { value := valid; value.BoundaryPoint.Frequency = 500; return value }(), want: false},
		{name: "sentinel", input: func() ReportMinerInput { value := valid; value.BoundaryPoint.Frequency = 50; return value }(), want: false},
		{name: "settlement after arm start", input: func() ReportMinerInput {
			value := valid
			value.BoundarySettledAt = start.Add(time.Second)
			return value
		}(), want: false},
		{name: "non-UTC settlement", input: func() ReportMinerInput {
			value := valid
			value.BoundarySettledAt = time.Date(2026, 1, 7, 19, 0, 0, 0, time.FixedZone("offset", -5*60*60))
			return value
		}(), want: false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := validReportBoundary(testCase.input, start); got != testCase.want {
				t.Fatalf("validReportBoundary() = %t, want %t", got, testCase.want)
			}
		})
	}
}

func TestSummarizeRestartExposureMergesOverlappingIntervals(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	window := ReportWindow{Start: from, End: from.Add(time.Hour)}
	result, err := SummarizeRestartExposure([]MutationAttempt{
		{Kind: MutationOperatingPoint, RestartRequestedAt: from, MiningResumedAt: from.Add(40 * time.Minute)},
		{Kind: MutationOperatingPoint, RestartRequestedAt: from.Add(20 * time.Minute), MiningResumedAt: from.Add(50 * time.Minute)},
		{Kind: MutationSafetyRollback, RestartRequestedAt: from.Add(10 * time.Minute), MiningResumedAt: from.Add(30 * time.Minute)},
		{Kind: MutationOperatingPoint, RestartRequestedAt: from.Add(50 * time.Minute)},
	}, window)
	if err != nil {
		t.Fatal(err)
	}
	if result.NormalExposureSeconds != 3600 || result.SafetyExposureSeconds != 1200 || result.UnresolvedAttempts != 1 {
		t.Fatalf("merged restart exposure = %+v", result)
	}
}

func TestEvaluateArmAndCrossover(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	makeInput := func(host string, rate float64, stable bool) ReportMinerInput {
		return ReportMinerInput{
			Hostname: host, PreArmSettledHashRate: rate,
			PassStartedAt: from, PassReferenceHash: rate, PointStable: stable,
		}
	}
	ab, err := EvaluateArm(ReportArmInput{
		Start:     from,
		Treatment: makeInput("a", 100, true),
		Control:   makeInput("b", 100, true),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Populate the summaries through the public arm input contract without
	// relying on observed-time normalization.
	ab.Treatment.Coverage = .99
	ab.Control.Coverage = .99
	ab.Treatment.PreArmSettledHashRate = 100
	ab.Control.PreArmSettledHashRate = 100
	ab.Treatment.ActualHashSeconds = 100 * ReportArmDuration.Seconds() * 1.03
	ab.Control.ActualHashSeconds = 100 * ReportArmDuration.Seconds()
	ab.Treatment.NormalizedWork = 1.03
	ab.Control.NormalizedWork = 1
	// EvaluateArm is intentionally the only constructor for arm validity; the
	// next assertion uses a real hourly fixture.
	if ab.Valid {
		t.Fatal("empty arm unexpectedly evaluated as valid")
	}
	makeHourly := func(mac string, hash float64, start time.Time) []HourlyAggregate {
		rows := make([]HourlyAggregate, 0, 168)
		for index := 0; index < 168; index++ {
			var settled time.Duration
			if index >= 24 {
				settled = time.Hour
			}
			rows = append(rows, HourlyAggregate{MacAddr: mac, HourStartedAt: start.Add(time.Duration(index) * time.Hour), ObservedDuration: time.Hour, ActualHashSeconds: hash * 3600, SettledDuration: settled})
		}
		return rows
	}
	makeRecords := func(mac string, start time.Time) []OperatingPointRecord {
		return []OperatingPointRecord{{MacAddr: mac, Frequency: 525, CoreVoltage: 1150, Status: PointValidated, EnteredAt: start}}
	}
	armInput := func(start time.Time, treatment, control string, treatmentHash, controlHash float64) ReportArmInput {
		treatmentMAC := testMAC
		controlMAC := "aa:bb:cc:dd:ee:03"
		if treatment == "b" {
			treatmentMAC, controlMAC = controlMAC, treatmentMAC
		}
		return ReportArmInput{
			Start:     start,
			Treatment: ReportMinerInput{MacAddr: treatmentMAC, Hostname: treatment, PreArmSettledHashRate: 100, PassStartedAt: start, PassReferenceHash: 100, BoundaryPoint: OperatingPoint{Frequency: 525, CoreVoltage: 1150}, BoundarySettledAt: start.Add(-time.Hour), SettledAt: start.Add(24 * time.Hour), PointStable: true, PointRecords: makeRecords(treatmentMAC, start), NormalRestartBaselineObserved: true, Hourly: makeHourly(treatmentMAC, treatmentHash, start)},
			// The control has no current point records in this fixture. Its
			// complete boundary snapshot is the historical evidence for the arm.
			Control: ReportMinerInput{MacAddr: controlMAC, Hostname: control, PreArmSettledHashRate: 100, BoundaryPoint: OperatingPoint{Frequency: 525, CoreVoltage: 1150}, BoundarySettledAt: start.Add(-time.Hour), SettledAt: start.Add(-time.Hour), PointStable: true, Hourly: makeHourly(controlMAC, controlHash, start)},
		}
	}
	ab, err = EvaluateArm(armInput(from, "a", "b", 103, 100))
	if err != nil || !ab.Valid || math.Abs(ab.Uplift-.03) > 1e-12 {
		t.Fatalf("AB arm = %+v, %v", ab, err)
	}
	if !ab.Treatment.ConvergedBy48Hours || !ab.Treatment.NormalRestartReductionValid ||
		!ab.Treatment.NormalExposureValid || !ab.Treatment.Frontier24Valid ||
		ab.Treatment.Coverage != 1 {
		t.Fatalf("treatment metrics were not retained in arm result: %+v", ab.Treatment)
	}
	baStart := from.Add(ReportArmDuration)
	ba, err := EvaluateArm(armInput(baStart, "b", "a", 103, 100))
	if err != nil || !ba.Valid {
		t.Fatalf("BA arm = %+v, %v", ba, err)
	}
	crossover, err := EvaluateCrossover(CrossoverInput{
		AB: ReportArmInput{Start: from, Treatment: armInput(from, "a", "b", 103, 100).Treatment, Control: armInput(from, "a", "b", 103, 100).Control},
		BA: ReportArmInput{Start: baStart, Treatment: armInput(baStart, "b", "a", 103, 100).Treatment, Control: armInput(baStart, "b", "a", 103, 100).Control},
	})
	if err != nil || !crossover.Valid || math.Abs(crossover.CrossoverUplift-.03) > 1e-12 {
		t.Fatalf("crossover = %+v, %v", crossover, err)
	}
}

func TestEvaluateArmRejectsLowCoverage(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []HourlyAggregate{{MacAddr: testMAC, HourStartedAt: from, ObservedDuration: time.Hour, ActualHashSeconds: 360000}}
	controlRows := []HourlyAggregate{{MacAddr: "aa:bb:cc:dd:ee:03", HourStartedAt: from, ObservedDuration: time.Hour, ActualHashSeconds: 360000}}
	result, err := EvaluateArm(ReportArmInput{
		Start:     from,
		Treatment: ReportMinerInput{MacAddr: testMAC, Hostname: "a", PreArmSettledHashRate: 100, PassStartedAt: from, PassReferenceHash: 100, BoundaryPoint: OperatingPoint{Frequency: 525, CoreVoltage: 1150}, BoundarySettledAt: from.Add(-time.Hour), SettledAt: from.Add(24 * time.Hour), PointStable: true, NormalRestartBaselineObserved: true, Hourly: rows},
		Control:   ReportMinerInput{MacAddr: "aa:bb:cc:dd:ee:03", Hostname: "b", PreArmSettledHashRate: 100, BoundaryPoint: OperatingPoint{Frequency: 525, CoreVoltage: 1150}, BoundarySettledAt: from.Add(-time.Hour), SettledAt: from.Add(-time.Hour), PointStable: true, Hourly: controlRows},
	})
	if err != nil || result.Valid {
		t.Fatalf("low coverage arm = %+v, %v", result, err)
	}
}

func TestAuditFrontier24RejectsDuplicateAndTimeCreatedEntries(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	window := ReportWindow{Start: from, End: from.Add(ReportArmDuration)}
	records := []OperatingPointRecord{
		{Frequency: 400, CoreVoltage: 1000, Status: PointEntered, EnteredAt: from},
		{Frequency: 490, CoreVoltage: 1000, Status: PointEntered, EnteredAt: from.Add(time.Hour)},
		{Frequency: 550, CoreVoltage: 1100, Status: PointEntered, EnteredAt: from.Add(2 * time.Hour), EntryAttemptID: 1},
		{Frequency: 550, CoreVoltage: 1100, Status: PointEntered, EnteredAt: from.Add(3 * time.Hour), EntryAttemptID: 2},
	}
	duplicates, timeCreated := auditFrontier24(records, window, from)
	if duplicates != 1 || timeCreated != 1 {
		t.Fatalf("frontier audit = duplicates %d, time-created %d", duplicates, timeCreated)
	}
}

func TestSummarizeReportMinerTreatsPartialBoundaryHoursAsUnknown(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	window := ReportWindow{Start: start, End: start.Add(2 * time.Hour)}
	metrics, err := SummarizeReportMiner(ReportMinerInput{
		MacAddr:               testMAC,
		PreArmSettledHashRate: 100,
		Hourly: []HourlyAggregate{
			{MacAddr: testMAC, HourStartedAt: start.Add(-30 * time.Minute), ObservedDuration: time.Hour, ActualHashSeconds: 360000},
			{MacAddr: testMAC, HourStartedAt: start.Add(30 * time.Minute), ObservedDuration: time.Hour, ActualHashSeconds: 360000},
			{MacAddr: testMAC, HourStartedAt: start.Add(90 * time.Minute), ObservedDuration: time.Hour, ActualHashSeconds: 360000},
		},
	}, window)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.ObservedSeconds != 3600 || metrics.UnknownGapSeconds != 3600 || metrics.ActualHashSeconds != 360000 || metrics.Coverage != .5 {
		t.Fatalf("partial boundary accounting = %+v", metrics)
	}
}

func TestSummarizeReportMinerTreatsMissingHourlyBucketsAsUnknown(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	metrics, err := SummarizeReportMiner(ReportMinerInput{
		MacAddr:               testMAC,
		PreArmSettledHashRate: 100,
		Hourly: []HourlyAggregate{
			{MacAddr: testMAC, HourStartedAt: start, ObservedDuration: time.Hour, ActualHashSeconds: 360000},
			{MacAddr: testMAC, HourStartedAt: start.Add(2 * time.Hour), ObservedDuration: time.Hour, ActualHashSeconds: 360000},
		},
	}, ReportWindow{Start: start, End: start.Add(3 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if metrics.ObservedSeconds != 7200 || metrics.UnknownGapSeconds != 3600 || metrics.ActualHashSeconds != 720000 {
		t.Fatalf("sparse hourly accounting = %+v", metrics)
	}
}

func TestEvaluateArmSeparatesCoverageValidityFromAcceptance(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]HourlyAggregate, 0, 168)
	for index := 0; index < 168; index++ {
		rows = append(rows, HourlyAggregate{
			MacAddr: testMAC, HourStartedAt: from.Add(time.Duration(index) * time.Hour),
			ObservedDuration: time.Hour, ActualHashSeconds: 360000, SettledDuration: time.Hour,
		})
	}
	controlRows := make([]HourlyAggregate, 0, 168)
	for index := 0; index < 168; index++ {
		controlRows = append(controlRows, HourlyAggregate{
			MacAddr: "aa:bb:cc:dd:ee:03", HourStartedAt: from.Add(time.Duration(index) * time.Hour),
			ObservedDuration: time.Hour, ActualHashSeconds: 360000,
		})
	}
	input := ReportArmInput{
		Start: from,
		Treatment: ReportMinerInput{
			MacAddr: testMAC, Hostname: "treatment", PreArmSettledHashRate: 100,
			PassStartedAt: from, PassReferenceHash: 100, BoundaryPoint: OperatingPoint{Frequency: 525, CoreVoltage: 1150}, BoundarySettledAt: from.Add(-time.Hour), SettledAt: from.Add(time.Hour),
			PointStable: true, PointRecords: []OperatingPointRecord{{MacAddr: testMAC, Frequency: 525, CoreVoltage: 1150, Status: PointValidated, EnteredAt: from}}, Hourly: rows,
		},
		Control: ReportMinerInput{
			MacAddr: "aa:bb:cc:dd:ee:03", Hostname: "control", PreArmSettledHashRate: 100,
			BoundaryPoint: OperatingPoint{Frequency: 525, CoreVoltage: 1150}, BoundarySettledAt: from.Add(-time.Hour), SettledAt: from.Add(-time.Hour), PointStable: true,
			Hourly: controlRows,
		},
	}
	report, err := EvaluateArm(input)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Valid || !report.UpliftValid || report.Accepted || report.Uplift != 0 {
		t.Fatalf("coverage/economic validity was conflated: %+v", report)
	}
	input.Control.BoundarySettledAt = time.Time{}
	input.Control.PointStable = false
	unproven, err := EvaluateArm(input)
	if err != nil {
		t.Fatal(err)
	}
	if !unproven.Valid || unproven.UpliftValid || unproven.Accepted {
		t.Fatalf("unproven control boundary was accepted: %+v", unproven)
	}
}

func TestEvaluateArmRejectsSameMinerAndCrossoverReversal(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := EvaluateArm(ReportArmInput{
		Start:     from,
		Treatment: ReportMinerInput{MacAddr: testMAC, Hostname: "a"},
		Control:   ReportMinerInput{MacAddr: testMAC, Hostname: "b"},
	}); err == nil {
		t.Fatal("same-miner arm was accepted")
	}
	arm := func(start time.Time, treatment, control string) ReportArmInput {
		return ReportArmInput{
			Start:     start,
			Treatment: ReportMinerInput{Hostname: treatment},
			Control:   ReportMinerInput{Hostname: control},
		}
	}
	if _, err := EvaluateCrossover(CrossoverInput{
		AB: arm(from.Add(ReportArmDuration), "a", "b"),
		BA: arm(from, "b", "a"),
	}); err == nil {
		t.Fatal("reversed crossover arms were accepted")
	}
}
