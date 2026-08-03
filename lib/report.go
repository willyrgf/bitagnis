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
	// ReportPracticalUplift is the predeclared practical work-improvement target.
	ReportPracticalUplift = 0.02
)

// ReportWindow is a UTC half-open reporting interval. Partial boundary hours
// are conservatively classified as unknown because hourly rows do not retain
// sub-hour state history.
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
// calculations. PreArmSettledHashRate and the complete boundary tuple are
// frozen at the arm boundary.
type ReportMinerInput struct {
	MacAddr                       string
	Hostname                      string
	PreArmSettledHashRate         float64
	PassStartedAt                 time.Time
	PassReferenceHash             float64
	SettledAt                     time.Time
	BoundaryPoint                 OperatingPoint
	BoundarySettledAt             time.Time
	PointStable                   bool
	PointRecords                  []OperatingPointRecord
	NormalRestartBaselineRequests int
	NormalRestartBaselineObserved bool
	Hourly                        []HourlyAggregate
	MutationAttempts              []MutationAttempt
}

// ReportMinerMetrics contains the full-wall-duration economic summary for one
// miner in one report arm.
type ReportMinerMetrics struct {
	Hostname                           string
	BoundaryPoint                      OperatingPoint
	BoundarySettledAt                  time.Time
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
	PostSettlementCoverageValid        bool
	NormalRestartBaselineRequests      int
	NormalRestartBaselineObserved      bool
	NormalRestartReduction             float64
	NormalRestartReductionValid        bool
	NormalExposureValid                bool
	ConvergedBy48Hours                 bool
	DuplicateEnteredTargets            int
	TimeCreatedEligibility             int
	Frontier24Audited                  bool
	Frontier24Valid                    bool
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
	ControlStable   bool
	UpliftValid     bool
	Valid           bool
	Accepted        bool
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
	Accepted        bool
	PracticalTarget bool
}

// SummarizeReportMiner computes metrics over the complete wall interval.
// Unknown time lowers coverage and contributes zero work; it is never
// discarded by observed-time normalization.
func SummarizeReportMiner(input ReportMinerInput, window ReportWindow) (ReportMinerMetrics, error) {
	if err := validateReportWindow(window); err != nil {
		return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: %w", err)
	}
	if !finiteReportValue(input.PreArmSettledHashRate) || input.PreArmSettledHashRate < 0 {
		return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: pre-arm rate is invalid")
	}
	if input.NormalRestartBaselineRequests < 0 {
		return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: normal restart baseline is invalid")
	}
	result := ReportMinerMetrics{
		Hostname:                      input.Hostname,
		BoundaryPoint:                 input.BoundaryPoint,
		BoundarySettledAt:             input.BoundarySettledAt,
		PreArmSettledHashRate:         input.PreArmSettledHashRate,
		SettledAt:                     input.SettledAt,
		NormalRestartBaselineRequests: input.NormalRestartBaselineRequests,
		NormalRestartBaselineObserved: input.NormalRestartBaselineObserved,
		Frontier24Audited:             len(input.PointRecords) > 0,
	}
	result.DuplicateEnteredTargets, result.TimeCreatedEligibility = auditFrontier24(
		input.PointRecords, window, input.PassStartedAt,
	)
	result.Frontier24Valid = result.Frontier24Audited &&
		result.DuplicateEnteredTargets == 0 && result.TimeCreatedEligibility == 0
	hourlyByStart := make(map[int64]HourlyAggregate, len(input.Hourly))
	expectedMAC := ""
	for _, aggregate := range input.Hourly {
		if aggregate.MacAddr == "" {
			return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: hourly row is outside the requested scope")
		}
		if input.MacAddr != "" && aggregate.MacAddr != input.MacAddr {
			return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: hourly row belongs to another miner")
		}
		if expectedMAC == "" {
			expectedMAC = aggregate.MacAddr
		} else if aggregate.MacAddr != expectedMAC {
			return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: hourly rows contain multiple miners")
		}
		if aggregate.HourStartedAt.Location() != time.UTC || !aggregate.HourStartedAt.Equal(aggregate.HourStartedAt.Truncate(time.Hour)) {
			return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: hourly row is not UTC")
		}
		hourStartedAt := aggregate.HourStartedAt.Unix()
		if _, seen := hourlyByStart[hourStartedAt]; seen {
			return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: duplicate hourly bucket")
		}
		if err := validateHourlyAggregate(aggregate); err != nil {
			return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: %w", err)
		}
		hourEnd := aggregate.HourStartedAt.Add(time.Hour)
		segmentStart := aggregate.HourStartedAt
		if segmentStart.Before(window.Start) {
			segmentStart = window.Start
		}
		segmentEnd := hourEnd
		if segmentEnd.After(window.End) {
			segmentEnd = window.End
		}
		if !segmentEnd.After(segmentStart) {
			return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: hourly row is outside the requested scope")
		}
		hourlyByStart[hourStartedAt] = aggregate
	}
	for cursor := window.Start.Truncate(time.Hour); cursor.Before(window.End); cursor = cursor.Add(time.Hour) {
		hourEnd := cursor.Add(time.Hour)
		segmentStart := cursor
		if segmentStart.Before(window.Start) {
			segmentStart = window.Start
		}
		segmentEnd := hourEnd
		if segmentEnd.After(window.End) {
			segmentEnd = window.End
		}
		if !segmentEnd.After(segmentStart) {
			continue
		}
		aggregate, found := hourlyByStart[cursor.Unix()]
		if !found || !segmentStart.Equal(cursor) || !segmentEnd.Equal(hourEnd) {
			result.UnknownGapSeconds += segmentEnd.Sub(segmentStart).Seconds()
			continue
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
	for _, value := range []float64{
		result.ObservedSeconds, result.UnknownGapSeconds, result.ActualHashSeconds,
		result.TrialActualHashSeconds, result.IncumbentCounterfactualHashSeconds,
		result.SettledSeconds, result.TrialSeconds,
	} {
		if !finiteReportValue(value) {
			return ReportMinerMetrics{}, fmt.Errorf("summarize report miner: hourly totals are non-finite")
		}
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

// EvaluateArm compares treatment and control over exactly 168 hours. Validity
// is the economic coverage rule; Accepted includes convergence, restart,
// frontier, settlement, and control-stability gates.
func EvaluateArm(input ReportArmInput) (ArmReport, error) {
	if input.Start.IsZero() || input.Start.Location() != time.UTC {
		return ArmReport{}, fmt.Errorf("evaluate arm: start must be a UTC timestamp")
	}
	if input.Treatment.Hostname == "" || input.Control.Hostname == "" ||
		input.Treatment.Hostname == input.Control.Hostname {
		return ArmReport{}, fmt.Errorf("evaluate arm: treatment and control must be distinct miners")
	}
	treatmentMAC := reportInputMAC(input.Treatment)
	controlMAC := reportInputMAC(input.Control)
	if treatmentMAC != "" && treatmentMAC == controlMAC {
		return ArmReport{}, fmt.Errorf("evaluate arm: treatment and control MACs must be distinct")
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
	result.Valid = result.CoverageValid
	if !input.Treatment.NormalRestartBaselineObserved {
		treatment.NormalRestartReductionValid = false
	} else if treatment.NormalRestartBaselineRequests == 0 {
		treatment.NormalRestartReduction = 1
		treatment.NormalRestartReductionValid = treatment.Restart.NormalRequests == 0
	} else {
		treatment.NormalRestartReduction = 1 - float64(treatment.Restart.NormalRequests)/float64(treatment.NormalRestartBaselineRequests)
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
			if treatment.SettledSeconds <= postSettlementSeconds+1e-9 {
				treatment.PostSettlementCoverage = treatment.SettledSeconds / postSettlementSeconds
				treatment.PostSettlementCoverageValid = true
			}
		}
	}
	result.Treatment = treatment
	if treatment.PreArmSettledHashRate > 0 && control.PreArmSettledHashRate > 0 {
		result.Uplift = treatment.NormalizedWork - control.NormalizedWork
	}
	treatmentBoundaryFrozen := validReportBoundary(input.Treatment, window.Start) &&
		input.Treatment.PassStartedAt.Equal(window.Start) && input.Treatment.PassReferenceHash > 0 &&
		input.Treatment.PassReferenceHash == treatment.PreArmSettledHashRate
	controlBoundarySettled := validReportBoundary(input.Control, window.Start)
	result.ControlStable = input.Control.PointStable && controlBoundarySettled
	result.UpliftValid = treatmentBoundaryFrozen && result.Uplift >= 0 &&
		result.ControlStable
	result.Accepted = result.Valid && result.UpliftValid &&
		treatment.ConvergedBy48Hours && treatment.NormalRestartBaselineObserved &&
		treatment.NormalRestartReductionValid && treatment.NormalExposureValid &&
		treatment.PostSettlementCoverageValid && treatment.PostSettlementCoverage >= ReportPostSettlementCoverage &&
		treatment.Frontier24Audited && treatment.Frontier24Valid
	result.Treatment = treatment
	result.PracticalTarget = result.Accepted && result.Uplift >= ReportPracticalUplift
	return result, nil
}

func validReportBoundary(input ReportMinerInput, armStart time.Time) bool {
	return input.PreArmSettledHashRate > 0 && finiteReportValue(input.PreArmSettledHashRate) &&
		IsCanonicalOperatingPoint(input.BoundaryPoint) &&
		input.BoundaryPoint.Frequency != 50 &&
		!input.BoundarySettledAt.IsZero() &&
		input.BoundarySettledAt.Location() == time.UTC &&
		input.BoundarySettledAt.UnixNano() > 0 &&
		!input.BoundarySettledAt.After(armStart)
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
	if abTreatmentMAC, baControlMAC := reportInputMAC(input.AB.Treatment), reportInputMAC(input.BA.Control); abTreatmentMAC != "" && baControlMAC != "" && abTreatmentMAC != baControlMAC {
		return CrossoverReport{}, fmt.Errorf("evaluate crossover: treatment MAC roles are not symmetric")
	}
	if abControlMAC, baTreatmentMAC := reportInputMAC(input.AB.Control), reportInputMAC(input.BA.Treatment); abControlMAC != "" && baTreatmentMAC != "" && abControlMAC != baTreatmentMAC {
		return CrossoverReport{}, fmt.Errorf("evaluate crossover: control MAC roles are not symmetric")
	}
	if ba.Window.Start.Before(ab.Window.Start) {
		return CrossoverReport{}, fmt.Errorf("evaluate crossover: BA arm precedes AB arm")
	}
	if ba.Window.Start.Before(ab.Window.End) {
		return CrossoverReport{}, fmt.Errorf("evaluate crossover: arm windows overlap")
	}
	result := CrossoverReport{AB: ab, BA: ba}
	if !ab.Valid || !ba.Valid || !ab.UpliftValid || !ba.UpliftValid {
		return result, nil
	}
	result.CrossoverUplift = (ab.Uplift + ba.Uplift) / 2
	result.Valid = result.CrossoverUplift >= 0
	result.Accepted = result.Valid && ab.Accepted && ba.Accepted
	result.PracticalTarget = result.Accepted && result.CrossoverUplift >= ReportPracticalUplift
	return result, nil
}

func reportInputMAC(input ReportMinerInput) string {
	if input.MacAddr != "" {
		return input.MacAddr
	}
	for _, aggregate := range input.Hourly {
		if aggregate.MacAddr != "" {
			return aggregate.MacAddr
		}
	}
	return ""
}

// auditFrontier24 checks the durable point-entry ledger for the first 24
// hours of a treatment pass. A candidate may have one entry and must be tied
// to its mutation attempt; the sole unbound entry is the pass baseline.
func auditFrontier24(
	records []OperatingPointRecord,
	window ReportWindow,
	passStartedAt time.Time,
) (duplicateTargets, timeCreatedEligibility int) {
	if len(records) == 0 {
		return 0, 0
	}
	auditEnd := window.Start.Add(24 * time.Hour)
	entered := make(map[OperatingPoint]int)
	unboundAtPassStart := 0
	for _, record := range records {
		if record.EnteredAt.Before(window.Start) || !record.EnteredAt.Before(auditEnd) {
			continue
		}
		entered[record.Point()]++
		if record.EntryAttemptID != 0 {
			continue
		}
		if passStartedAt.IsZero() || !record.EnteredAt.Equal(passStartedAt) {
			timeCreatedEligibility++
		} else {
			unboundAtPassStart++
		}
	}
	for _, count := range entered {
		if count > 1 {
			duplicateTargets += count - 1
		}
	}
	if unboundAtPassStart == 0 {
		timeCreatedEligibility++
	} else if unboundAtPassStart > 1 {
		timeCreatedEligibility += unboundAtPassStart - 1
	}
	return duplicateTargets, timeCreatedEligibility
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
		window.Start.Location() != time.UTC || window.End.Location() != time.UTC {
		return fmt.Errorf("range must be positive UTC timestamps")
	}
	if window.End.Sub(window.Start) > LongTermRetentionHours*time.Hour {
		return fmt.Errorf("range exceeds retained history")
	}
	return nil
}

func finiteReportValue(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
