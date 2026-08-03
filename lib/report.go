package lib

import (
	"fmt"
	"math"
	"sort"
	"time"
)

const (
	// ReportArmDuration is the fixed duration of every treatment arm.
	ReportArmDuration = 168 * time.Hour
	// ReportMinimumCoverage is the minimum observed fraction for a valid arm.
	ReportMinimumCoverage = 0.95
	// ReportConvergenceDeadline is the RFC acceptance deadline for settling.
	ReportConvergenceDeadline = 48 * time.Hour
	// ReportNormalRestartReduction is the minimum reduction from the prior
	// observed normal-restart count.
	ReportNormalRestartReduction = 0.90
	// ReportNormalExposureLimit is the maximum normal restart exposure as a
	// fraction of arm wall time.
	ReportNormalExposureLimit = 0.01
	// ReportPostSettlementCoverage is the minimum selected-point coverage after
	// the treatment reaches verified settlement.
	ReportPostSettlementCoverage = 0.95
)

// ReportWindow is a UTC-hour-aligned half-open reporting interval.
type ReportWindow struct {
	Start time.Time
	End   time.Time
}

// RestartExposure summarizes restart requests and the time they kept a miner
// from reaching healthy mining. Mining-configuration attempts are omitted;
// these metrics describe optimizer operating-point and safety economics.
type RestartExposure struct {
	NormalRequests        int
	NormalExposureSeconds float64
	SafetyRequests        int
	SafetyExposureSeconds float64
	UnresolvedAttempts    int
}

// ReportMinerInput is the durable, credential-free input to report
// calculations. PreArmSettledHashRate is frozen at the arm boundary.
type ReportMinerInput struct {
	Hostname              string
	PreArmSettledHashRate float64
	PassStartedAt         time.Time
	PassReferenceHash     float64
	SettledAt             time.Time
	SelectedPoint         OperatingPoint
	PointStable           bool
	Hourly                []HourlyAggregate
	MutationAttempts      []MutationAttempt
}

// ReportMinerMetrics contains the full-wall-duration economic summary for one
// miner in one report arm.
type ReportMinerMetrics struct {
	Hostname                           string
	SettledAt                          time.Time
	Coverage                           float64
	ObservedSeconds                    float64
	UnknownGapSeconds                  float64
	ActualHashSeconds                  float64
	TrialActualHashSeconds             float64
	IncumbentCounterfactualHashSeconds float64
	SettledSeconds                     float64
	TrialSeconds                       float64
	PreArmSettledHashRate              float64
	NormalizedWork                     float64
	PostSettlementCoverage             float64
	NormalRestartBaselineRequests      int
	NormalRestartReduction             float64
	NormalRestartReductionValid        bool
	NormalExposureValid                bool
	ConvergedBy48Hours                 bool
	Restart                            RestartExposure
}

// ReportArmInput describes one treatment/control comparison. End is derived
// from Start and cannot be shortened or extended by the caller.
type ReportArmInput struct {
	Start     time.Time
	Treatment ReportMinerInput
	Control   ReportMinerInput
}

// ArmReport is the result of one fixed 168-hour comparison.
type ArmReport struct {
	Window          ReportWindow
	TreatmentHost   string
	ControlHost     string
	Treatment       ReportMinerMetrics
	Control         ReportMinerMetrics
	Uplift          float64
	CoverageValid   bool
	UpliftValid     bool
	Valid           bool
	PracticalTarget bool
}

// CrossoverInput contains the sequential AB and BA arms.
type CrossoverInput struct {
	AB ReportArmInput
	BA ReportArmInput
}

// CrossoverReport is the result of an AB/BA crossover.
type CrossoverReport struct {
	AB              ArmReport
	BA              ArmReport
	CrossoverUplift float64
	Valid           bool
	PracticalTarget bool
}

// QueryLongTerm reads retained hourly aggregates and mutation history and
// calculates one report-only miner summary.
func (store *OptimizerStore) QueryLongTerm(
	macAddr string,
	window ReportWindow,
	preArmSettledHashRate float64,
) (ReportMinerMetrics, error) {
	if store == nil {
		return ReportMinerMetrics{}, fmt.Errorf("query long-term report: store is nil")
	}
	hourly, err := store.ListHourly(macAddr, window.Start, window.End)
	if err != nil {
		return ReportMinerMetrics{}, fmt.Errorf("query long-term report: hourly aggregates: %w", err)
	}
	attempts, err := store.ListMutationAttempts(macAddr)
	if err != nil {
		return ReportMinerMetrics{}, fmt.Errorf("query long-term report: mutation attempts: %w", err)
	}
	return SummarizeReportMiner(ReportMinerInput{
		PreArmSettledHashRate: preArmSettledHashRate,
		Hourly:                hourly,
		MutationAttempts:      attempts,
	}, window)
}

// SummarizeReportMiner computes metrics over full UTC hour buckets and the
// complete wall interval. Unknown time lowers coverage and contributes zero
// work; it is never discarded by observed-time normalization.
func SummarizeReportMiner(input ReportMinerInput, window ReportWindow) (ReportMinerMetrics, error) {
	if err := validateReportWindow(window); err != nil {
		return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: %w", err)
	}
	if !finiteReportValue(input.PreArmSettledHashRate) || input.PreArmSettledHashRate < 0 {
		return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: pre-arm rate is invalid")
	}
	result := ReportMinerMetrics{
		Hostname:              input.Hostname,
		PreArmSettledHashRate: input.PreArmSettledHashRate,
		SettledAt:             input.SettledAt,
	}
	seenHours := make(map[int64]bool, len(input.Hourly))
	for _, aggregate := range input.Hourly {
		if aggregate.HourStartedAt.Before(window.Start.UTC()) || !aggregate.HourStartedAt.Before(window.End.UTC()) ||
			aggregate.MacAddr == "" {
			return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: hourly row is outside the requested scope")
		}
		if !aggregate.HourStartedAt.Equal(aggregate.HourStartedAt.UTC()) {
			return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: hourly row is not UTC")
		}
		if seenHours[aggregate.HourStartedAt.Unix()] {
			return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: duplicate hourly bucket")
		}
		seenHours[aggregate.HourStartedAt.Unix()] = true
		if err := validateHourlyAggregate(aggregate); err != nil {
			return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: %w", err)
		}
		result.ObservedSeconds += aggregate.ObservedSeconds
		result.UnknownGapSeconds += aggregate.UnknownGapSeconds
		result.ActualHashSeconds += aggregate.ActualHashSeconds
		result.TrialActualHashSeconds += aggregate.TrialActualHashSeconds
		result.IncumbentCounterfactualHashSeconds += aggregate.IncumbentCounterfactualHashSeconds
		result.SettledSeconds += aggregate.SettledSeconds
		result.TrialSeconds += aggregate.TrialSeconds
	}
	duration := window.End.Sub(window.Start).Seconds()
	if result.ObservedSeconds+result.UnknownGapSeconds > duration+1e-9 ||
		result.SettledSeconds > result.ObservedSeconds+1e-9 ||
		result.TrialSeconds > result.ObservedSeconds+1e-9 ||
		result.TrialActualHashSeconds > result.ActualHashSeconds+1e-9 {
		return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: hourly totals violate bounds")
	}
	result.Coverage = result.ObservedSeconds / duration
	if result.PreArmSettledHashRate > 0 {
		result.NormalizedWork = result.ActualHashSeconds /
			(result.PreArmSettledHashRate * duration)
	}
	restart, err := SummarizeRestartExposure(input.MutationAttempts, window)
	if err != nil {
		return ReportMinerMetrics{}, err
	}
	result.Restart = restart
	return result, nil
}

// SummarizeRestartExposure counts only restart lifecycles relevant to
// optimizer economics and merges overlapping exposure intervals by kind.
func SummarizeRestartExposure(attempts []MutationAttempt, window ReportWindow) (RestartExposure, error) {
	if err := validateReportWindow(window); err != nil {
		return RestartExposure{}, fmt.Errorf("summarize restart exposure: %w", err)
	}
	var normal, safety []reportInterval
	result := RestartExposure{}
	for _, attempt := range attempts {
		if err := validateReportAttempt(attempt); err != nil {
			return RestartExposure{}, err
		}
		kind := classifyReportMutation(attempt.Kind)
		if kind == reportMutationIgnored || attempt.RestartRequestedAt.IsZero() {
			continue
		}
		if !attempt.RestartRequestedAt.Before(window.Start) && attempt.RestartRequestedAt.Before(window.End) {
			if kind == reportMutationSafety {
				result.SafetyRequests++
			} else {
				result.NormalRequests++
			}
		}
		start := attempt.RestartRequestedAt
		end := window.End
		unresolved := attempt.MiningResumedAt.IsZero() && attempt.FailedAt.IsZero()
		if !attempt.MiningResumedAt.IsZero() {
			end = attempt.MiningResumedAt
		} else if !attempt.FailedAt.IsZero() {
			end = attempt.FailedAt
		}
		if start.Before(window.Start) {
			start = window.Start
		}
		if end.After(window.End) {
			end = window.End
		}
		if end.After(start) {
			interval := reportInterval{start: start, end: end}
			if kind == reportMutationSafety {
				safety = append(safety, interval)
			} else {
				normal = append(normal, interval)
			}
		}
		if unresolved && start.Before(window.End) {
			result.UnresolvedAttempts++
		}
	}
	result.NormalExposureSeconds = mergedReportSeconds(normal)
	result.SafetyExposureSeconds = mergedReportSeconds(safety)
	return result, nil
}

func validateReportAttempt(attempt MutationAttempt) error {
	if !attempt.RestartRequestedAt.IsZero() {
		if !attempt.PatchRequestedAt.IsZero() && attempt.RestartRequestedAt.Before(attempt.PatchRequestedAt) {
			return fmt.Errorf("summarize restart exposure: restart milestone precedes PATCH")
		}
		if !attempt.MiningResumedAt.IsZero() && attempt.MiningResumedAt.Before(attempt.RestartRequestedAt) {
			return fmt.Errorf("summarize restart exposure: mining resumption precedes restart")
		}
		if !attempt.FailedAt.IsZero() && attempt.FailedAt.Before(attempt.RestartRequestedAt) {
			return fmt.Errorf("summarize restart exposure: failure precedes restart")
		}
		if !attempt.MiningResumedAt.IsZero() && !attempt.FailedAt.IsZero() {
			return fmt.Errorf("summarize restart exposure: attempt is both resumed and failed")
		}
	}
	return nil
}

// EvaluateArm compares treatment and control over exactly 168 hours. An arm
// is valid at zero uplift when both cover at least 95% and the control point
// was unchanged. The practical two-percent target is reported separately.
func EvaluateArm(input ReportArmInput) (ArmReport, error) {
	if input.Start.IsZero() || input.Start.UTC().Truncate(time.Hour) != input.Start.UTC() {
		return ArmReport{}, fmt.Errorf("evaluate arm: start must be a UTC hour")
	}
	window := ReportWindow{Start: input.Start.UTC(), End: input.Start.UTC().Add(ReportArmDuration)}
	treatment, err := SummarizeReportMiner(input.Treatment, window)
	if err != nil {
		return ArmReport{}, err
	}
	control, err := SummarizeReportMiner(input.Control, window)
	if err != nil {
		return ArmReport{}, err
	}
	result := ArmReport{
		Window: window, TreatmentHost: input.Treatment.Hostname, ControlHost: input.Control.Hostname,
		Treatment: treatment, Control: control,
		CoverageValid: treatment.Coverage >= ReportMinimumCoverage && control.Coverage >= ReportMinimumCoverage,
	}
	baselineWindow := ReportWindow{Start: window.Start.Add(-ReportArmDuration), End: window.Start}
	baseline, err := SummarizeRestartExposure(input.Treatment.MutationAttempts, baselineWindow)
	if err != nil {
		return ArmReport{}, err
	}
	treatment.NormalRestartBaselineRequests = baseline.NormalRequests
	if baseline.NormalRequests == 0 {
		treatment.NormalRestartReduction = 1
		treatment.NormalRestartReductionValid = treatment.Restart.NormalRequests == 0
	} else {
		treatment.NormalRestartReduction = 1 - float64(treatment.Restart.NormalRequests)/float64(baseline.NormalRequests)
		treatment.NormalRestartReductionValid = treatment.NormalRestartReduction >= ReportNormalRestartReduction
	}
	treatment.NormalExposureValid = treatment.Restart.NormalExposureSeconds <=
		ReportNormalExposureLimit*ReportArmDuration.Seconds()
	if !input.Treatment.SettledAt.IsZero() && !input.Treatment.SettledAt.After(window.End) {
		treatment.ConvergedBy48Hours = !input.Treatment.SettledAt.After(window.Start.Add(ReportConvergenceDeadline))
		postSettlementStart := input.Treatment.SettledAt
		if postSettlementStart.Before(window.Start) {
			postSettlementStart = window.Start
		}
		postSettlementSeconds := window.End.Sub(postSettlementStart).Seconds()
		if postSettlementSeconds > 0 {
			treatment.PostSettlementCoverage = treatment.SettledSeconds / postSettlementSeconds
		}
	}
	if treatment.PreArmSettledHashRate <= 0 || control.PreArmSettledHashRate <= 0 {
		return result, nil
	}
	if !input.Treatment.PassStartedAt.Equal(window.Start) ||
		input.Treatment.PassReferenceHash <= 0 ||
		input.Treatment.PassReferenceHash != treatment.PreArmSettledHashRate {
		return result, nil
	}
	result.Uplift = treatment.NormalizedWork - control.NormalizedWork
	controlBoundarySettled := !input.Control.SettledAt.IsZero() && !input.Control.SettledAt.After(window.Start)
	result.UpliftValid = result.Uplift >= 0 && input.Control.PointStable && controlBoundarySettled
	result.Valid = result.CoverageValid && result.UpliftValid &&
		treatment.ConvergedBy48Hours && treatment.NormalRestartReductionValid &&
		treatment.NormalExposureValid && treatment.PostSettlementCoverage >= ReportPostSettlementCoverage
	result.PracticalTarget = result.Valid && result.Uplift >= .02
	return result, nil
}

// EvaluateCrossover evaluates sequential, non-overlapping AB and BA arms and
// requires the treatment/control roles to be symmetric.
func EvaluateCrossover(input CrossoverInput) (CrossoverReport, error) {
	ab, err := EvaluateArm(input.AB)
	if err != nil {
		return CrossoverReport{}, err
	}
	ba, err := EvaluateArm(input.BA)
	if err != nil {
		return CrossoverReport{}, err
	}
	if input.AB.Treatment.Hostname != input.BA.Control.Hostname ||
		input.AB.Control.Hostname != input.BA.Treatment.Hostname {
		return CrossoverReport{}, fmt.Errorf("evaluate crossover: treatment/control roles are not symmetric")
	}
	if ab.Window.Start.Before(ba.Window.End) && ba.Window.Start.Before(ab.Window.End) {
		return CrossoverReport{}, fmt.Errorf("evaluate crossover: arm windows overlap")
	}
	result := CrossoverReport{AB: ab, BA: ba}
	if !ab.Valid || !ba.Valid {
		return result, nil
	}
	result.CrossoverUplift = (ab.Uplift + ba.Uplift) / 2
	result.Valid = result.CrossoverUplift >= 0
	result.PracticalTarget = result.Valid && result.CrossoverUplift >= .02
	return result, nil
}

type reportMutationClass uint8

const (
	reportMutationIgnored reportMutationClass = iota
	reportMutationNormal
	reportMutationSafety
)

type reportInterval struct {
	start time.Time
	end   time.Time
}

func classifyReportMutation(kind MutationKind) reportMutationClass {
	switch kind {
	case MutationOperatingPoint:
		return reportMutationNormal
	case MutationSafetyRollback, MutationOverheatRecovery:
		return reportMutationSafety
	default:
		return reportMutationIgnored
	}
}

func mergedReportSeconds(intervals []reportInterval) float64 {
	if len(intervals) == 0 {
		return 0
	}
	sort.Slice(intervals, func(left, right int) bool { return intervals[left].start.Before(intervals[right].start) })
	merged := 0.0
	start, end := intervals[0].start, intervals[0].end
	for _, interval := range intervals[1:] {
		if !interval.start.After(end) {
			if interval.end.After(end) {
				end = interval.end
			}
			continue
		}
		merged += end.Sub(start).Seconds()
		start, end = interval.start, interval.end
	}
	return merged + end.Sub(start).Seconds()
}

func validateReportWindow(window ReportWindow) error {
	if window.Start.IsZero() || window.End.IsZero() || !window.End.After(window.Start) ||
		window.Start.UTC().Truncate(time.Hour) != window.Start.UTC() ||
		window.End.UTC().Truncate(time.Hour) != window.End.UTC() {
		return fmt.Errorf("range must be positive UTC hours")
	}
	if window.End.Sub(window.Start) > LongTermRetentionHours*time.Hour {
		return fmt.Errorf("range exceeds retained history")
	}
	return nil
}

func finiteReportValue(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
