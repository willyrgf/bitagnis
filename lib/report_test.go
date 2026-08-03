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
			{MacAddr: testMAC, HourStartedAt: from, ObservedSeconds: 3600, ActualHashSeconds: 360000},
			{MacAddr: testMAC, HourStartedAt: from.Add(time.Hour), UnknownGapSeconds: 3600},
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
			rows = append(rows, HourlyAggregate{MacAddr: mac, HourStartedAt: start.Add(time.Duration(index) * time.Hour), ObservedSeconds: 3600, ActualHashSeconds: hash * 3600, SettledSeconds: 3600})
		}
		return rows
	}
	armInput := func(start time.Time, treatment, control string, treatmentHash, controlHash float64) ReportArmInput {
		return ReportArmInput{
			Start:     start,
			Treatment: ReportMinerInput{Hostname: treatment, PreArmSettledHashRate: 100, PassStartedAt: start, PassReferenceHash: 100, SettledAt: start.Add(24 * time.Hour), PointStable: true, Hourly: makeHourly(testMAC, treatmentHash, start)},
			Control:   ReportMinerInput{Hostname: control, PreArmSettledHashRate: 100, SettledAt: start.Add(-time.Hour), PointStable: true, Hourly: makeHourly("aa:bb:cc:dd:ee:03", controlHash, start)},
		}
	}
	ab, err = EvaluateArm(armInput(from, "a", "b", 103, 100))
	if err != nil || !ab.Valid || math.Abs(ab.Uplift-.03) > 1e-12 {
		t.Fatalf("AB arm = %+v, %v", ab, err)
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
	rows := []HourlyAggregate{{MacAddr: testMAC, HourStartedAt: from, ObservedSeconds: 3600, ActualHashSeconds: 360000}}
	result, err := EvaluateArm(ReportArmInput{
		Start:     from,
		Treatment: ReportMinerInput{Hostname: "a", PreArmSettledHashRate: 100, PassStartedAt: from, PassReferenceHash: 100, SettledAt: from.Add(24 * time.Hour), PointStable: true, Hourly: rows},
		Control:   ReportMinerInput{Hostname: "b", PreArmSettledHashRate: 100, SettledAt: from.Add(-time.Hour), PointStable: true, Hourly: rows},
	})
	if err != nil || result.Valid {
		t.Fatalf("low coverage arm = %+v, %v", result, err)
	}
}
