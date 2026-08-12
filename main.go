package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/signal"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/willyrgf/bitagnis/lib"
)

const (
	colorReset       = "\033[0m"
	colorRed         = "\033[31m"
	colorGreen       = "\033[32m"
	colorYellow      = "\033[33m"
	pollWorkerLimit  = 16
	minerColumnCount = 10
)

var minerTableHeader = [minerColumnCount]string{
	"Hostname",
	"Freq",
	"VCore",
	"State",
	"Window",
	"Temp",
	"VRTemp",
	"HRate/Expected",
	"Watts",
	"Fan",
}

type deviceAPI interface {
	GetSystemInfo(context.Context, string) (lib.Info, error)
	GetASICSettings(context.Context, string) (lib.ASICSettings, error)
	PatchOperatingPoint(context.Context, lib.OperatingPoint, string) error
	PatchFirmwareRecovery(context.Context, lib.OperatingPoint, string) error
	PatchMiningConfiguration(
		context.Context,
		lib.MiningSettings,
		string,
		string,
		string,
	) error
	Restart(context.Context, string) error
}

type optimizerStateStore interface {
	LoadMiner(string) (lib.MinerState, error)
	ListPoints(string) ([]lib.OperatingPointRecord, error)
	Apply(lib.Transition, time.Time) (lib.TransitionResult, error)
	StartMutationAttempt(*lib.MutationAttempt) (int64, error)
	AdvanceMutationAttempt(int64, lib.MutationMilestone, time.Time) error
	RecordConfiguredVerification(int64, time.Time, int) error
	RecordFirstPositive(int64, time.Time) error
	ListMutationAttempts(string) ([]lib.MutationAttempt, error)
	PendingMutationResume(string) (lib.MutationAttempt, bool, error)
	UnfinishedMutationAttempt(string) (lib.MutationAttempt, bool, error)
	CompareAndSetHourly(string, time.Time, time.Time, []lib.HourlyAggregate, time.Time) error
	ListHourly(string, time.Time, time.Time) ([]lib.HourlyAggregate, error)
	OpenEvidenceEpochFor(string) (lib.EvidenceEpoch, bool, error)
	EvidenceEpochByID(int64) (lib.EvidenceEpoch, error)
	MonitorReference(int64) (lib.WindowAggregate, error)
}

type controller struct {
	devices  deviceAPI
	states   optimizerStateStore
	settings lib.SettingsFile
	logger   *log.Logger
	output   io.Writer

	runtimeMu sync.Mutex
	runtimes  map[string]*minerRuntime

	mutations *mutationCoordinator

	attributionMu     sync.Mutex
	attributionSince  time.Time
	attributionWindow []pollCycleAttribution
}

// pollCycleAttribution is one poll cycle's wall time split across the four segments pollMiners
// executes serially, in call order. It exists to attribute tick loss: main.go's select loop
// invokes pollMiners synchronously and time.Ticker drops its one buffered tick when the receiver
// is slow, so identifying which segment dominates a slow cycle is a prerequisite to fixing it.
type pollCycleAttribution struct {
	httpFanOut        time.Duration
	hourlyAccounting  time.Duration
	safetyAndControl  time.Duration
	mutationAndRender time.Duration
}

func (attribution pollCycleAttribution) total() time.Duration {
	return attribution.httpFanOut + attribution.hourlyAccounting +
		attribution.safetyAndControl + attribution.mutationAndRender
}

type pollCycleAttributionSummary struct {
	count                        int
	totalP50, totalP95           time.Duration
	httpP50, httpP95             time.Duration
	accountingP50, accountingP95 time.Duration
	safetyP50, safetyP95         time.Duration
	mutationP50, mutationP95     time.Duration
}

func summarizePollCycleAttribution(samples []pollCycleAttribution) pollCycleAttributionSummary {
	total := make([]float64, len(samples))
	http := make([]float64, len(samples))
	accounting := make([]float64, len(samples))
	safety := make([]float64, len(samples))
	mutation := make([]float64, len(samples))
	for index, sample := range samples {
		total[index] = float64(sample.total())
		http[index] = float64(sample.httpFanOut)
		accounting[index] = float64(sample.hourlyAccounting)
		safety[index] = float64(sample.safetyAndControl)
		mutation[index] = float64(sample.mutationAndRender)
	}
	return pollCycleAttributionSummary{
		count:         len(samples),
		totalP50:      time.Duration(percentile(total, .5)),
		totalP95:      time.Duration(percentile(total, .95)),
		httpP50:       time.Duration(percentile(http, .5)),
		httpP95:       time.Duration(percentile(http, .95)),
		accountingP50: time.Duration(percentile(accounting, .5)),
		accountingP95: time.Duration(percentile(accounting, .95)),
		safetyP50:     time.Duration(percentile(safety, .5)),
		safetyP95:     time.Duration(percentile(safety, .95)),
		mutationP50:   time.Duration(percentile(mutation, .5)),
		mutationP95:   time.Duration(percentile(mutation, .95)),
	}
}

// recordPollCycleAttribution logs one line per cycle and, once an hour of cycles has
// accumulated, a percentile summary — the data commit 2's constants are calibrated from, not from
// the 0.673 clean-interval rate measured against the defect this instruments. Credential-free:
// only durations and a sample count ever appear in the log line.
func (minerController *controller) recordPollCycleAttribution(now time.Time, attribution pollCycleAttribution) {
	minerController.logf(
		"poll cycle attribution: total=%s http=%s accounting=%s safety=%s mutation=%s",
		attribution.total().Round(time.Millisecond),
		attribution.httpFanOut.Round(time.Millisecond),
		attribution.hourlyAccounting.Round(time.Millisecond),
		attribution.safetyAndControl.Round(time.Millisecond),
		attribution.mutationAndRender.Round(time.Millisecond),
	)

	minerController.attributionMu.Lock()
	defer minerController.attributionMu.Unlock()
	if minerController.attributionSince.IsZero() {
		minerController.attributionSince = now
	}
	minerController.attributionWindow = append(minerController.attributionWindow, attribution)
	if now.Sub(minerController.attributionSince) < time.Hour {
		return
	}
	summary := summarizePollCycleAttribution(minerController.attributionWindow)
	minerController.attributionWindow = nil
	minerController.attributionSince = now
	minerController.logf(
		"poll cycle attribution (hourly, n=%d): total p50=%s p95=%s http p50=%s p95=%s "+
			"accounting p50=%s p95=%s safety p50=%s p95=%s mutation p50=%s p95=%s",
		summary.count,
		summary.totalP50.Round(time.Millisecond), summary.totalP95.Round(time.Millisecond),
		summary.httpP50.Round(time.Millisecond), summary.httpP95.Round(time.Millisecond),
		summary.accountingP50.Round(time.Millisecond), summary.accountingP95.Round(time.Millisecond),
		summary.safetyP50.Round(time.Millisecond), summary.safetyP95.Round(time.Millisecond),
		summary.mutationP50.Round(time.Millisecond), summary.mutationP95.Round(time.Millisecond),
	)
}

type pollJob struct {
	index int
	miner lib.DiscoveredMiner
}

type minerPollResult struct {
	columns           [minerColumnCount]string
	hashRate          float64
	hashRateAvailable bool
	observation       *minerObservation
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, arguments []string) error {
	options, err := parseArguments(arguments)
	if err != nil {
		return err
	}
	var store *lib.OptimizerStore
	if options.reportMode != "" {
		store, err = lib.OpenOptimizerStoreReadOnly("optimizer.db")
	} else {
		store, err = lib.OpenOptimizerStore("optimizer.db")
	}
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			log.Printf("Optimizer database shutdown failed: %s", closeErr)
		}
	}()
	if options.reportMode != "" {
		return runLongTermReport(store, options, os.Stdout)
	}
	settingsFile, err := lib.LoadSettings("settings.yaml")
	if err != nil {
		return err
	}
	if err := validateReapplyHostnames(options.reapply, settingsFile); err != nil {
		return err
	}
	defaultSettings, err := settingsFile.ForHost("")
	if err != nil {
		return err
	}

	scanClient := lib.NewBitaxeClient(3 * time.Second)
	client := lib.NewBitaxeClient(5 * time.Second)
	miners, err := lib.ScanNetwork(ctx, options.hostnames, settingsFile, scanClient)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	if err := validateNamedDiscovery(options.hostnames, miners); err != nil {
		return err
	}

	minerController := &controller{
		devices:  client,
		states:   store,
		settings: settingsFile,
		logger:   log.Default(),
		output:   os.Stdout,
		runtimes: make(map[string]*minerRuntime),
	}
	rediscover := func(
		discoveryContext context.Context,
		macAddr string,
	) (lib.DiscoveredMiner, error) {
		discovered, discoveryErr := lib.ScanNetwork(
			discoveryContext,
			map[string]bool{"all": true},
			settingsFile,
			scanClient,
		)
		if discoveryErr != nil {
			return lib.DiscoveredMiner{}, discoveryErr
		}
		for _, miner := range discovered {
			if miner.Info.MacAddr == macAddr {
				return miner, nil
			}
		}
		return lib.DiscoveredMiner{}, errMinerNotFound
	}
	minerController.mutations = newMutationCoordinator(
		client,
		store,
		settingsFile,
		miners,
		options.reapply,
		options.retune,
		rediscover,
		func(settings lib.MiningSettings) (string, string, error) {
			return lib.LoadMiningPasswords(".env", settings)
		},
		log.Default(),
		minerController.resetRuntime,
	)
	if !options.hostnames["all"] {
		expected := make(map[string]string, len(miners))
		for _, miner := range miners {
			expected[miner.Info.MacAddr] = miner.Info.Hostname
		}
		minerController.mutations.RequireHostnames(expected)
	}
	if options.retune != "" {
		minerController.mutations.RecordRetuneDiscovery(time.Now().UTC())
	}
	metricsPoll := time.NewTicker(defaultSettings.MetricsTime)
	defer metricsPoll.Stop()
	networkPoll := time.NewTicker(20 * time.Minute)
	defer networkPoll.Stop()

	log.Printf("Metrics interval: %s", defaultSettings.MetricsTime)
	log.Printf(
		"Operating-point evaluation: %s after %s ramp",
		defaultSettings.EvaluationWindowTime,
		defaultSettings.RampUpTime,
	)
	minerController.pollMiners(ctx, minerController.mutations.Routes(), time.Now())

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-networkPoll.C:
			discovered, scanErr := lib.ScanNetwork(
				ctx,
				options.hostnames,
				settingsFile,
				scanClient,
			)
			if scanErr != nil {
				if !errors.Is(scanErr, context.Canceled) {
					log.Printf(
						"Network rescan failed; retaining %d known miners: %s",
						len(minerController.mutations.Routes()),
						scanErr,
					)
				}
				continue
			}
			if discoveryErr := validateNamedDiscovery(
				options.hostnames,
				discovered,
			); discoveryErr != nil {
				log.Printf("Network rescan rejected: %s", discoveryErr)
				continue
			}
			minerController.mutations.UpdateDiscovery(discovered)
		case now := <-metricsPoll.C:
			minerController.pollMiners(
				ctx,
				minerController.mutations.Routes(),
				now,
			)
		}
	}
}

type commandOptions struct {
	hostnames    map[string]bool
	reapply      map[string]bool
	retune       string
	reportMode   string
	reportHosts  []string
	reportStarts []time.Time
}

func parseArguments(arguments []string) (commandOptions, error) {
	options := commandOptions{
		hostnames: make(map[string]bool),
		reapply:   make(map[string]bool),
	}
	reapply := false
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		switch {
		case argument == "--reapply-mining":
			if reapply {
				return commandOptions{}, fmt.Errorf("--reapply-mining was specified more than once")
			}
			reapply = true
		case argument == "--retune":
			if options.retune != "" {
				return commandOptions{}, fmt.Errorf("--retune was specified more than once")
			}
			if index+1 >= len(arguments) || arguments[index+1] == "" || arguments[index+1][0] == '-' {
				return commandOptions{}, fmt.Errorf("--retune requires exactly one hostname")
			}
			if arguments[index+1] == "all" {
				return commandOptions{}, fmt.Errorf("--retune requires a specific hostname, not all")
			}
			options.retune = arguments[index+1]
			index++
		case argument == "--report":
			if options.reportMode != "" {
				return commandOptions{}, fmt.Errorf("--report was specified more than once")
			}
			mode, hosts, starts, consumed, parseErr := parseReportArguments(arguments[index+1:])
			if parseErr != nil {
				return commandOptions{}, parseErr
			}
			options.reportMode = mode
			options.reportHosts = hosts
			options.reportStarts = starts
			index += consumed
		case argument == "":
			return commandOptions{}, fmt.Errorf("hostname arguments cannot be empty")
		case argument[0] == '-':
			return commandOptions{}, fmt.Errorf("unknown flag %q", argument)
		default:
			options.hostnames[argument] = true
		}
	}
	if options.retune != "" && reapply {
		return commandOptions{}, fmt.Errorf("--retune and --reapply-mining are mutually exclusive")
	}
	if options.retune != "" && len(options.hostnames) != 0 {
		return commandOptions{}, fmt.Errorf("--retune requires exactly one hostname")
	}
	if options.reportMode != "" {
		if reapply || options.retune != "" || len(options.hostnames) != 0 {
			return commandOptions{}, fmt.Errorf("--report cannot be combined with mutation or hostname arguments")
		}
		return options, nil
	}
	if options.retune != "" {
		options.hostnames[options.retune] = true
	}
	if reapply {
		if len(options.hostnames) == 0 {
			return commandOptions{}, fmt.Errorf("--reapply-mining requires at least one hostname")
		}
		for hostname := range options.hostnames {
			options.reapply[hostname] = true
		}
	}
	if len(options.hostnames) == 0 {
		options.hostnames["all"] = true
	}
	return options, nil
}

func parseReportArguments(arguments []string) (string, []string, []time.Time, int, error) {
	if len(arguments) == 0 {
		return "", nil, nil, 0, fmt.Errorf("--report requires one-arm or ab-ba arguments")
	}
	mode := arguments[0]
	var hostCount int
	switch mode {
	case "one-arm":
		hostCount = 2
	case "ab-ba":
		hostCount = 2
	default:
		return "", nil, nil, 0, fmt.Errorf("--report mode %q is invalid", mode)
	}
	startCount := 1
	if mode == "ab-ba" {
		startCount = 2
	}
	consumed := 1 + hostCount + startCount
	if len(arguments) < consumed {
		return "", nil, nil, 0, fmt.Errorf("--report %s requires %d arguments", mode, hostCount+startCount+1)
	}
	hosts := append([]string(nil), arguments[1:1+hostCount]...)
	for _, host := range hosts {
		if host == "" || host == "all" || strings.HasPrefix(host, "-") {
			return "", nil, nil, 0, fmt.Errorf("--report requires specific hostnames")
		}
	}
	starts := make([]time.Time, startCount)
	for index := range starts {
		value, err := time.Parse(time.RFC3339, arguments[1+hostCount+index])
		if err != nil || value.Location() != time.UTC {
			return "", nil, nil, 0, fmt.Errorf("--report start must be a UTC RFC3339 timestamp")
		}
		starts[index] = value.UTC()
	}
	return mode, hosts, starts, consumed, nil
}

func validateReapplyHostnames(
	hostnames map[string]bool,
	settingsFile lib.SettingsFile,
) error {
	for hostname := range hostnames {
		settings, err := settingsFile.ForHost(hostname)
		if err != nil {
			return err
		}
		if !settings.Mining.Enabled {
			return fmt.Errorf(
				"--reapply-mining requires mining to be enabled for %q",
				hostname,
			)
		}
	}
	return nil
}

func validateNamedDiscovery(
	hostnames map[string]bool,
	miners []lib.DiscoveredMiner,
) error {
	if hostnames["all"] {
		return nil
	}
	counts := make(map[string]int)
	for _, miner := range miners {
		counts[miner.Info.Hostname]++
	}
	for hostname := range hostnames {
		if counts[hostname] != 1 {
			return fmt.Errorf(
				"selected hostname %q must map to exactly one MAC; found %d",
				hostname,
				counts[hostname],
			)
		}
	}
	return nil
}

func runLongTermReport(store *lib.OptimizerStore, options commandOptions, output io.Writer) error {
	if store == nil || options.reportMode == "" {
		return fmt.Errorf("long-term report: invalid request")
	}
	switch options.reportMode {
	case "one-arm":
		input, err := loadReportArmInput(store, options.reportHosts[0], options.reportHosts[1], options.reportStarts[0])
		if err != nil {
			return err
		}
		report, err := lib.EvaluateArm(input)
		if err != nil {
			return err
		}
		formatArmReport(output, report)
		return nil
	case "ab-ba":
		ab, err := loadReportArmInput(store, options.reportHosts[0], options.reportHosts[1], options.reportStarts[0])
		if err != nil {
			return err
		}
		ba, err := loadReportArmInput(store, options.reportHosts[1], options.reportHosts[0], options.reportStarts[1])
		if err != nil {
			return err
		}
		report, err := lib.EvaluateCrossover(lib.CrossoverInput{AB: ab, BA: ba})
		if err != nil {
			return err
		}
		formatCrossoverReport(output, report)
		return nil
	default:
		return fmt.Errorf("long-term report: unsupported mode %q", options.reportMode)
	}
}

func loadReportArmInput(
	store *lib.OptimizerStore,
	treatmentHostname string,
	controlHostname string,
	start time.Time,
) (lib.ReportArmInput, error) {
	window := lib.ReportWindow{Start: start.UTC(), End: start.UTC().Add(lib.ReportArmDuration)}
	treatment, err := loadReportMinerInput(store, treatmentHostname, window, true)
	if err != nil {
		return lib.ReportArmInput{}, err
	}
	control, err := loadReportMinerInput(store, controlHostname, window, false)
	if err != nil {
		return lib.ReportArmInput{}, err
	}
	if treatment.MacAddr == "" || control.MacAddr == "" || treatment.MacAddr == control.MacAddr {
		return lib.ReportArmInput{}, fmt.Errorf("long-term report: treatment and control must be distinct MACs")
	}
	return lib.ReportArmInput{Start: window.Start, Treatment: treatment, Control: control}, nil
}

func loadReportMinerInput(
	store *lib.OptimizerStore,
	hostname string,
	window lib.ReportWindow,
	treatment bool,
) (lib.ReportMinerInput, error) {
	state, err := store.LoadMinerByHostname(hostname)
	if err != nil {
		return lib.ReportMinerInput{}, err
	}
	records, err := store.ListPoints(state.MacAddr)
	if err != nil {
		return lib.ReportMinerInput{}, fmt.Errorf("long-term report %s: selected point: %w", hostname, err)
	}
	hourly, err := store.ListHourly(state.MacAddr, window.Start, window.End)
	if err != nil {
		return lib.ReportMinerInput{}, fmt.Errorf("long-term report %s: hourly data: %w", hostname, err)
	}
	attempts, err := store.ListMutationAttempts(state.MacAddr)
	if err != nil {
		return lib.ReportMinerInput{}, fmt.Errorf("long-term report %s: mutation history: %w", hostname, err)
	}
	baselineRequests := 0
	baselineObserved := false
	if treatment {
		baselineWindow := lib.ReportWindow{Start: window.Start.Add(-lib.ReportArmDuration), End: window.Start}
		baselineHourly, baselineErr := store.ListHourly(state.MacAddr, baselineWindow.Start, baselineWindow.End)
		if baselineErr != nil {
			return lib.ReportMinerInput{}, fmt.Errorf("long-term report %s: restart baseline hourly data: %w", hostname, baselineErr)
		}
		baselineMetrics, baselineErr := lib.SummarizeReportMiner(lib.ReportMinerInput{
			MacAddr: state.MacAddr,
			Hourly:  baselineHourly,
		}, baselineWindow)
		if baselineErr != nil {
			return lib.ReportMinerInput{}, fmt.Errorf("long-term report %s: restart baseline: %w", hostname, baselineErr)
		}
		baselineObserved = baselineMetrics.Coverage >= lib.ReportMinimumCoverage
		baselineExposure, baselineErr := lib.SummarizeRestartExposure(attempts, baselineWindow)
		if baselineErr != nil {
			return lib.ReportMinerInput{}, fmt.Errorf("long-term report %s: restart baseline mutations: %w", hostname, baselineErr)
		}
		baselineRequests = baselineExposure.NormalRequests
	}
	input := lib.ReportMinerInput{
		MacAddr:                       state.MacAddr,
		Hostname:                      hostname,
		PassStartedAt:                 state.PassStartedAt,
		PassReferenceHash:             state.PassReferenceHash,
		PointRecords:                  records,
		NormalRestartBaselineRequests: baselineRequests,
		NormalRestartBaselineObserved: baselineObserved,
		Hourly:                        hourly,
		MutationAttempts:              attempts,
	}
	mutationStable := reportPointStable(attempts, window)
	if treatment {
		if state.PassTrigger != lib.PassOperator || !state.PassStartedAt.Equal(window.Start) || state.PassReferenceHash <= 0 {
			return lib.ReportMinerInput{}, fmt.Errorf("long-term report %s: treatment pass boundary is not durably frozen at the arm start", hostname)
		}
		input.PreArmSettledHashRate = state.PassReferenceHash
		input.BoundaryPoint = lib.OperatingPoint{
			Frequency:   state.PassReferenceFrequency,
			CoreVoltage: state.PassReferenceCoreVoltage,
		}
		input.BoundarySettledAt = state.PassReferenceSettledAt
		selected, found := findRecord(records, state.CurrentPoint())
		if found && selected.Status == lib.PointValidated && finite(selected.MedianHash) && selected.MedianHash > 0 {
			input.SettledAt = state.SettledAt
		}
		return input, nil
	}

	if state.PassStartedAt.Equal(window.End) && state.PassTrigger == lib.PassOperator {
		// The second retune starts at the half-open AB arm end. Its atomic
		// pass-reference snapshot is the only durable source for B's historical
		// AB boundary; the new BA point history is deliberately not consulted.
		input.PreArmSettledHashRate = state.PassReferenceHash
		input.BoundaryPoint = lib.OperatingPoint{
			Frequency:   state.PassReferenceFrequency,
			CoreVoltage: state.PassReferenceCoreVoltage,
		}
		input.BoundarySettledAt = state.PassReferenceSettledAt
		input.PointStable = mutationStable
		return input, nil
	}
	if state.PassStartedAt.After(window.Start) {
		// A reset inside the arm or after an inter-arm gap means the current
		// state cannot prove the historical control boundary.
		return input, nil
	}

	selected, found := findRecord(records, state.CurrentPoint())
	if !found || selected.Status != lib.PointValidated || !finite(selected.MedianHash) || selected.MedianHash <= 0 {
		return input, nil
	}
	input.PreArmSettledHashRate = selected.MedianHash
	input.BoundaryPoint = selected.Point()
	input.BoundarySettledAt = state.SettledAt
	input.SettledAt = state.SettledAt
	input.PointStable = state.Phase == lib.PhaseMonitor && state.MonitorReason == lib.MonitorSelected &&
		!state.SettledAt.IsZero() && state.PendingKind == "" && !state.MiningPending && mutationStable
	return input, nil
}

func reportPointStable(attempts []lib.MutationAttempt, window lib.ReportWindow) bool {
	for _, attempt := range attempts {
		if attempt.Kind != lib.MutationOperatingPoint && attempt.Kind != lib.MutationSafetyRollback && attempt.Kind != lib.MutationFirmwareRecovery {
			continue
		}
		if mutationOverlapsReportWindow(attempt, window) {
			return false
		}
	}
	return true
}

func mutationOverlapsReportWindow(attempt lib.MutationAttempt, window lib.ReportWindow) bool {
	if !attempt.StartedAt.Before(window.End) {
		return false
	}
	end := window.End
	switch {
	case !attempt.MiningResumedAt.IsZero():
		end = attempt.MiningResumedAt
	case !attempt.FailedAt.IsZero():
		end = attempt.FailedAt
	}
	return end.After(window.Start)
}

func formatArmReport(output io.Writer, report lib.ArmReport) {
	fmt.Fprintf(output, "Arm report: %s -> %s (%s to %s UTC)\n", report.TreatmentHost, report.ControlHost, report.Window.Start.Format(time.RFC3339), report.Window.End.Format(time.RFC3339))
	formatReportMiner(output, "treatment", report.Treatment)
	formatReportMiner(output, "control", report.Control)
	fmt.Fprintf(output, "  uplift: %.4f\n", report.Uplift)
	fmt.Fprintf(output, "  gates: coverage=%t convergence_48h=%t baseline_observed=%t restart_reduction=%t normal_exposure=%t post_settlement=%t frontier_24h=%t control_stable=%t\n",
		report.CoverageValid, report.Treatment.ConvergedBy48Hours,
		report.Treatment.NormalRestartBaselineObserved, report.Treatment.NormalRestartReductionValid,
		report.Treatment.NormalExposureValid,
		report.Treatment.PostSettlementCoverageValid && report.Treatment.PostSettlementCoverage >= lib.ReportPostSettlementCoverage,
		report.Treatment.Frontier24Valid, report.ControlStable)
	fmt.Fprintf(output, "  practical target (>=%.0f%% uplift): %t\n", lib.ReportPracticalUplift*100, report.PracticalTarget)
	fmt.Fprintf(output, "  result: %s (coverage-valid=%t)\n", reportStatus(report.Accepted, report.Treatment.PreArmSettledHashRate > 0 && report.Control.PreArmSettledHashRate > 0), report.Valid)
}

func formatCrossoverReport(output io.Writer, report lib.CrossoverReport) {
	fmt.Fprintln(output, "AB arm")
	formatArmReport(output, report.AB)
	fmt.Fprintln(output, "BA arm")
	formatArmReport(output, report.BA)
	fmt.Fprintf(output, "Crossover uplift: %.4f\n", report.CrossoverUplift)
	fmt.Fprintf(output, "Crossover result: %s (economic-valid=%t)\n", reportStatus(report.Accepted, report.AB.Treatment.PreArmSettledHashRate > 0 && report.BA.Treatment.PreArmSettledHashRate > 0), report.Valid)
}

func formatReportMiner(output io.Writer, role string, report lib.ReportMinerMetrics) {
	fmt.Fprintf(output, "  %s: coverage %.1f%% (observed %.0fs, unknown %.0fs), actual %.0f H/s·s, trial actual %.0f, incumbent counterfactual %.0f, normalized %.4f, settled %.0fs, trial %.0fs\n", role, report.Coverage*100, report.ObservedSeconds, report.UnknownGapSeconds, report.ActualHashSeconds, report.TrialActualHashSeconds, report.IncumbentCounterfactualHashSeconds, report.NormalizedWork, report.SettledSeconds, report.TrialSeconds)
	if report.BoundaryPoint != (lib.OperatingPoint{}) && !report.BoundarySettledAt.IsZero() {
		fmt.Fprintf(output, "    boundary %d/%d, settled %s\n", report.BoundaryPoint.Frequency, report.BoundaryPoint.CoreVoltage, report.BoundarySettledAt.UTC().Format(time.RFC3339))
	} else {
		fmt.Fprintln(output, "    boundary unavailable")
	}
	fmt.Fprintf(output, "    post-settlement coverage %.1f%% (evidence %t), prior normal requests %d (observed %t), reduction %.1f%%\n", report.PostSettlementCoverage*100, report.PostSettlementCoverageValid, report.NormalRestartBaselineRequests, report.NormalRestartBaselineObserved, report.NormalRestartReduction*100)
	fmt.Fprintf(output, "    frontier first 24h: audited %t, duplicates %d, time-created eligibility %d, valid %t\n", report.Frontier24Audited, report.DuplicateEnteredTargets, report.TimeCreatedEligibility, report.Frontier24Valid)
	fmt.Fprintf(output, "    restarts normal %d/%.0fs, safety %d/%.0fs, unresolved %d\n", report.Restart.NormalRequests, report.Restart.NormalExposureSeconds, report.Restart.SafetyRequests, report.Restart.SafetyExposureSeconds, report.Restart.UnresolvedAttempts)
}

func reportStatus(valid, evaluated bool) string {
	if !evaluated {
		return "NOT EVALUATED"
	}
	if valid {
		return "VALID"
	}
	return "INVALID"
}

func (minerController *controller) pollMiners(
	ctx context.Context,
	miners []lib.DiscoveredMiner,
	now time.Time,
) {
	var attribution pollCycleAttribution
	output := minerController.outputWriter()
	results := make([]minerPollResult, len(miners))
	jobs := make(chan pollJob, len(miners))
	for index, miner := range miners {
		jobs <- pollJob{index: index, miner: miner}
	}
	close(jobs)

	fanOutStart := time.Now()
	workerCount := min(pollWorkerLimit, len(miners))
	var workers sync.WaitGroup
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				results[job.index] = minerController.pollMinerSafely(
					ctx,
					job.miner,
					now,
				)
			}
		}()
	}
	workers.Wait()
	attribution.httpFanOut = time.Since(fanOutStart)

	observations := make(map[string]*minerObservation, len(results))
	handled := make(map[string]bool, len(results))
	allowOptimization := minerController.mutations == nil
	for index := range results {
		observation := results[index].observation
		if observation == nil {
			if index < len(miners) {
				if state, err := minerController.states.LoadMiner(miners[index].Info.MacAddr); err == nil {
					accountingStart := time.Now()
					err := minerController.accountHourly(miners[index].Info.MacAddr, nil, &state, lib.Info{}, now)
					attribution.hourlyAccounting += time.Since(accountingStart)
					if err != nil && !errors.Is(err, context.Canceled) {
						minerController.logf("Hourly accounting failed for %s: %s", miners[index].Info.Hostname, err)
					}
				}
			}
			continue
		}
		observations[observation.state.MacAddr] = observation
		accountingStart := time.Now()
		accountingErr := minerController.accountHourly(observation.state.MacAddr, observation, &observation.state, observation.info, now)
		attribution.hourlyAccounting += time.Since(accountingStart)
		if accountingErr != nil && !errors.Is(accountingErr, context.Canceled) {
			minerController.logf("Hourly accounting failed for %s: %s", observation.info.Hostname, accountingErr)
		}
		safetyStart := time.Now()
		wasHandled, err := minerController.enforceMinerSafety(
			ctx,
			&observation.state,
			observation.info,
			observation.asic,
			observation.settings,
			now,
		)
		attribution.safetyAndControl += time.Since(safetyStart)
		handled[observation.state.MacAddr] = wasHandled || err != nil
		if err != nil && !errors.Is(err, context.Canceled) {
			minerController.logf(
				"Safety control failed for %s: %s",
				observation.info.Hostname,
				err,
			)
		}
	}
	if minerController.mutations != nil {
		mutationStart := time.Now()
		var err error
		allowOptimization, err = minerController.mutations.Advance(ctx, observations, now)
		attribution.mutationAndRender += time.Since(mutationStart)
		if err != nil {
			minerController.logf("Mutation coordination failed: %s", err)
		}
	}
	for index := range results {
		observation := results[index].observation
		if observation == nil || handled[observation.state.MacAddr] {
			continue
		}
		controlStart := time.Now()
		var err error
		if poll, ok := newReadablePoll(observation.info, observation.asic); ok {
			err = minerController.controlMinerAfterSafety(
				ctx,
				&observation.state,
				poll,
				observation.settings,
				now,
				allowOptimization,
			)
		}
		attribution.safetyAndControl += time.Since(controlStart)
		if err != nil && !errors.Is(err, context.Canceled) {
			minerController.logf(
				"Optimizer control failed for %s: %s",
				observation.info.Hostname,
				err,
			)
		}
	}
	if minerController.mutations != nil && allowOptimization {
		mutationStart := time.Now()
		_, err := minerController.mutations.Advance(ctx, observations, now)
		attribution.mutationAndRender += time.Since(mutationStart)
		if err != nil {
			minerController.logf("Mutation coordination failed: %s", err)
		}
	}
	renderStart := time.Now()
	for index := range results {
		observation := results[index].observation
		if observation == nil {
			continue
		}
		results[index].columns = minerController.formatMinerColumns(
			observation.state,
			observation.info,
			observation.settings,
			now,
		)
		results[index].hashRate = observation.info.HashRate
		results[index].hashRateAvailable = validHashRate(observation.info.HashRate)
	}

	sort.Slice(results, func(left int, right int) bool {
		return results[left].columns[0] < results[right].columns[0]
	})
	totalHashRate := 0.0
	hashRateAvailable := false
	for _, result := range results {
		if result.hashRateAvailable {
			totalHashRate += result.hashRate
			hashRateAvailable = true
		}
	}
	writeMinerTable(output, results)
	fmt.Fprintf(output, "\nTotal: %s\n\n", formatAggregateHashRate(totalHashRate, hashRateAvailable))
	attribution.mutationAndRender += time.Since(renderStart)
	minerController.recordPollCycleAttribution(now, attribution)
}

func (minerController *controller) accountHourly(
	macAddr string,
	observation *minerObservation,
	state *lib.MinerState,
	info lib.Info,
	now time.Time,
) error {
	if state == nil || state.AccountedThroughAt.IsZero() || now.IsZero() {
		return fmt.Errorf("hourly accounting: missing durable cursor")
	}
	settings, err := minerController.settings.ForHost(state.Hostname)
	if err != nil {
		return err
	}
	runtime := minerController.runtimeFor(macAddr)
	if !now.After(state.AccountedThroughAt) {
		runtime.accounting = nil
		return nil
	}
	current := accountingSample{
		at: now, point: state.CurrentPoint(), phase: state.Phase,
		hashRate: info.HashRate, validHash: observation != nil && validHashRate(info.HashRate), state: *state,
	}
	if observation != nil {
		current.settled = minerController.verifiedSettledObservation(*state, info, observation.asic, settings, now)
	}
	if current.phase == lib.PhaseUndervolt || current.phase == lib.PhaseFrequencyTest || current.phase == lib.PhaseVoltageTest {
		if current.point != state.FallbackPoint() {
			records, listErr := minerController.states.ListPoints(macAddr)
			if listErr != nil {
				return listErr
			}
			entered, found := findRecord(records, current.point)
			if !found || entered.EntryAttemptID <= 0 || entered.ReferenceHash <= 0 || !finite(entered.ReferenceHash) {
				return fmt.Errorf("hourly accounting: live trial has no frozen reference hash")
			}
			current.referenceHash = entered.ReferenceHash
		}
	}
	_, hasOpenEpoch, epochErr := minerController.states.OpenEvidenceEpochFor(macAddr)
	if epochErr != nil {
		return epochErr
	}
	current.classification = classifyAccountingState(*state, current.referenceHash, current.settled, hasOpenEpoch)
	validCurrent := current.validHash
	previous := runtime.accounting
	compatible := accountingSamplesCompatible(previous, current, state.AccountedThroughAt, settings.MetricsTime)
	fragments := hourlyFragments(macAddr, state.AccountedThroughAt, now, current, compatible)
	if err := minerController.states.CompareAndSetHourly(macAddr, state.AccountedThroughAt, now, fragments, now); err != nil {
		runtime.accounting = nil
		if errors.Is(err, lib.ErrAccountingCursorChanged) {
			fresh, loadErr := minerController.states.LoadMiner(macAddr)
			if loadErr == nil {
				*state = fresh
			}
		}
		return err
	}
	state.AccountedThroughAt = now.UTC()
	if validCurrent {
		runtime.accounting = &current
	} else {
		runtime.accounting = nil
	}
	return nil
}

func hourlyFragments(
	macAddr string,
	start time.Time,
	end time.Time,
	current accountingSample,
	actual bool,
) []lib.HourlyAggregate {
	if !end.After(start) {
		return nil
	}
	retainedStart := end.UTC().Add(-384 * time.Hour)
	if start.Before(retainedStart) {
		start = retainedStart
	}
	if !end.After(start) {
		return nil
	}
	fragments := make([]lib.HourlyAggregate, 0, 2)
	for cursor := start.UTC(); cursor.Before(end.UTC()); {
		hour := cursor.Truncate(time.Hour)
		segmentEnd := hour.Add(time.Hour)
		if segmentEnd.After(end.UTC()) {
			segmentEnd = end.UTC()
		}
		duration := segmentEnd.Sub(cursor)
		seconds := duration.Seconds()
		fragment := lib.HourlyAggregate{MacAddr: macAddr, HourStartedAt: hour}
		if actual {
			fragment.ObservedDuration = duration
			fragment.ActualHashSeconds = current.hashRate * seconds
			if current.phase == lib.PhaseUndervolt || current.phase == lib.PhaseFrequencyTest || current.phase == lib.PhaseVoltageTest {
				if current.state.CurrentPoint() != current.state.FallbackPoint() {
					fragment.TrialDuration = duration
					fragment.TrialActualHashSeconds = fragment.ActualHashSeconds
					fragment.IncumbentCounterfactualHashSeconds = current.referenceHash * seconds
				}
			}
			if current.settled {
				fragment.SettledDuration = duration
			}
		} else {
			fragment.UnknownGapDuration = duration
		}
		fragments = append(fragments, fragment)
		cursor = segmentEnd
	}
	return fragments
}

func (minerController *controller) verifiedSettledObservation(
	state lib.MinerState,
	info lib.Info,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
) bool {
	return qualifiesSettledObservation(minerController.states, state, info, asic, settings, now, false)
}

func qualifiesSettledObservation(
	states optimizerStateStore,
	state lib.MinerState,
	info lib.Info,
	asic lib.ASICSettings,
	settings lib.Settings,
	now time.Time,
	allowManual bool,
) bool {
	switch state.MonitorReason {
	case lib.MonitorSelected:
		if state.SettledAt.IsZero() || state.MonitorReferenceEpochID <= 0 || now.Before(state.SettledAt) {
			return false
		}
	case lib.MonitorManual, lib.MonitorRejected, lib.MonitorStarved:
		if !allowManual {
			return false
		}
	default:
		return false
	}
	if states == nil || state.Phase != lib.PhaseMonitor ||
		state.PendingKind != "" || state.MiningPending ||
		info.MacAddr != state.MacAddr || operatingPointFromInfo(info) != state.CurrentPoint() ||
		canonicalASICGrid(asic) != nil || !operatingPointAdvertised(asic, state.CurrentPoint()) ||
		!completeSafetyTelemetry(info) || hasPowerFault(info) {
		return false
	}
	epoch, open, err := states.OpenEvidenceEpochFor(state.MacAddr)
	if err != nil || !open || epoch.Purpose != lib.EpochMonitor || epoch.Point != state.CurrentPoint() ||
		epoch.RequiredWindows != 2 {
		return false
	}
	minimum, err := minimumAdvertisedPoint(asic)
	if err != nil || assessInstantaneousSafety(info, settings, state.CurrentPoint(), minimum).action != safetyNormal {
		return false
	}
	attempts, err := states.ListMutationAttempts(state.MacAddr)
	if err != nil {
		return false
	}
	for _, attempt := range attempts {
		if attempt.FailedAt.IsZero() && attempt.MiningResumedAt.IsZero() {
			return false
		}
	}
	switch state.MonitorReason {
	case lib.MonitorManual, lib.MonitorRejected, lib.MonitorStarved:
		return state.SafetyReason == ""
	case lib.MonitorSelected:
		if state.SafetyReason != "" {
			return false
		}
		records, err := states.ListPoints(state.MacAddr)
		if err != nil {
			return false
		}
		selected, selectedOK := selectFinalPoint(records, asic, settings)
		best, bestOK := selectBestPoint(records, asic, settings)
		return selectedOK && bestOK && selected.Point() == state.CurrentPoint() &&
			selected.Status == lib.PointValidated && best.MedianHash == state.BestHashRate &&
			best.Point() == state.BestPoint()
	default:
		return false
	}
}

func (minerController *controller) pollMinerSafely(
	ctx context.Context,
	miner lib.DiscoveredMiner,
	now time.Time,
) (result minerPollResult) {
	defer func() {
		if recovered := recover(); recovered != nil {
			minerController.logf(
				"Recovered panic while polling %s\n%s",
				miner.IP,
				debug.Stack(),
			)
			result = minerPollResult{}
		}
	}()

	result, err := minerController.pollMiner(ctx, miner, now)
	if err != nil && !errors.Is(err, context.Canceled) {
		minerController.logf("Poll failed for %s: %s", miner.IP, err)
	}
	return result
}

func (minerController *controller) pollMiner(
	ctx context.Context,
	miner lib.DiscoveredMiner,
	now time.Time,
) (minerPollResult, error) {
	info, err := minerController.devices.GetSystemInfo(ctx, miner.IP)
	if err != nil {
		return minerPollResult{}, err
	}
	if info.MacAddr != miner.Info.MacAddr {
		return minerPollResult{}, fmt.Errorf(
			"wrong device at %s: expected MAC %s, found %s",
			miner.IP,
			miner.Info.MacAddr,
			info.MacAddr,
		)
	}
	settings, err := minerController.settings.ForHost(info.Hostname)
	if err != nil {
		return minerPollResult{}, err
	}
	if settings.Skip {
		return minerPollResult{}, nil
	}
	asic, err := minerController.devices.GetASICSettings(ctx, miner.IP)
	if err != nil {
		return minerPollResult{}, err
	}
	if asic.ASICModel != info.ASICModel {
		return minerPollResult{}, fmt.Errorf(
			"%s reported conflicting ASIC models %q and %q",
			info.Hostname,
			info.ASICModel,
			asic.ASICModel,
		)
	}
	pairAdvertised := operatingPointAdvertised(asic, operatingPointFromInfo(info)) &&
		canonicalASICGrid(asic) == nil
	result, err := minerController.states.Apply(lib.Bootstrap{
		Info:           info,
		IP:             miner.IP,
		PairAdvertised: pairAdvertised,
	}, now)
	if err != nil {
		return minerPollResult{}, err
	}
	state, created := result.State, result.Created
	if created {
		minerController.logf(
			"Bootstrapping %s from live operating point %d MHz/%d mV",
			info.Hostname,
			info.Frequency,
			info.CoreVoltage,
		)
	}

	return minerPollResult{
		observation: &minerObservation{
			miner:    miner,
			info:     info,
			asic:     asic,
			settings: settings,
			state:    state,
			created:  created,
		},
	}, nil
}

func writeMinerTable(output io.Writer, results []minerPollResult) {
	rows := make([][minerColumnCount]string, 1, len(results)+1)
	rows[0] = minerTableHeader
	for _, result := range results {
		if result.columns[0] != "" {
			rows = append(rows, result.columns)
		}
	}

	var widths [minerColumnCount]int
	for _, row := range rows {
		for index, value := range row {
			widths[index] = max(widths[index], terminalTextWidth(value))
		}
	}
	for _, row := range rows {
		for index, value := range row {
			fmt.Fprint(output, value)
			if index < minerColumnCount-1 {
				padding := widths[index] - terminalTextWidth(value) + 2
				fmt.Fprint(output, strings.Repeat(" ", padding))
			}
		}
		fmt.Fprintln(output)
	}
}

func terminalTextWidth(value string) int {
	for _, color := range [...]string{colorReset, colorRed, colorGreen, colorYellow} {
		value = strings.ReplaceAll(value, color, "")
	}
	return utf8.RuneCountInString(value)
}

func (minerController *controller) formatMinerColumns(
	state lib.MinerState,
	info lib.Info,
	settings lib.Settings,
	now time.Time,
) [minerColumnCount]string {
	return [minerColumnCount]string{
		info.Hostname,
		fmt.Sprintf("%d", info.Frequency),
		formatCoreVoltage(info),
		formatState(state, info, now),
		minerController.formatWindow(state, settings, now),
		formatTemp(info, settings),
		formatVRTemp(info, settings),
		formatHashRate(info),
		formatPower(info, settings),
		formatFan(info),
	}
}

func formatCoreVoltage(info lib.Info) string {
	if info.CoreVoltageActual > 0 {
		return fmt.Sprintf("%d/%.0f", info.CoreVoltage, info.CoreVoltageActual)
	}
	return fmt.Sprintf("%d", info.CoreVoltage)
}

func colorize(color string, value string) string {
	return color + value + colorReset
}

func validHashRate(hashRate float64) bool {
	return hashRate >= 0 && !math.IsNaN(hashRate) && !math.IsInf(hashRate, 0)
}

func formatHashRate(info lib.Info) string {
	if !validHashRate(info.HashRate) {
		return colorize(colorYellow, "N/A")
	}
	formatted := fmt.Sprintf("%.0f", info.HashRate)
	if info.ExpectedHashRate > 0 {
		formatted = fmt.Sprintf(
			"%.0f/%.0f %.0f%%",
			info.HashRate,
			info.ExpectedHashRate,
			info.HashRate/info.ExpectedHashRate*100,
		)
	}
	switch {
	case info.OverHeatMode != 0:
		return colorize(colorRed, formatted)
	case info.HashRate == 0:
		return colorize(colorYellow, formatted)
	default:
		return colorize(colorGreen, formatted)
	}
}

func formatAggregateHashRate(hashRate float64, available bool) string {
	if !available || !validHashRate(hashRate) {
		return "N/A"
	}
	if hashRate >= 1000 {
		return fmt.Sprintf("%.2f Th/s", hashRate/1000)
	}
	return fmt.Sprintf("%.2f Gh/s", hashRate)
}

func formatState(state lib.MinerState, info lib.Info, now time.Time) string {
	switch {
	case info.OverHeatMode != 0 ||
		(state.Phase == lib.PhaseEmergency && state.SafetyReason == lib.SafetyReasonFirmwareOverheat):
		return colorize(colorRed, "AXEOS")
	case state.Phase == lib.PhaseEmergency && state.SafetyReason == lib.SafetyReasonTelemetryUnavailable:
		return colorize(colorRed, "VERIFY")
	case state.Phase == lib.PhaseEmergency:
		return colorize(colorRed, "CONTAIN")
	case state.PendingKind == lib.MutationSafetyRollback:
		return colorize(colorRed, "BACKOFF")
	case state.PendingKind != "" || state.MiningPending:
		return colorize(colorYellow, "PENDING")
	case state.Phase == lib.PhaseCooldown:
		return colorize(colorYellow, "RECOVERY")
	case state.Phase == lib.PhaseMonitor:
		return colorize(colorGreen, string(state.Phase))
	default:
		return colorize(colorYellow, string(state.Phase))
	}
}

func formatTemp(info lib.Info, settings lib.Settings) string {
	if info.Temp <= 0 {
		return colorize(colorYellow, "N/A")
	}
	switch {
	case info.Temp >= settings.TempCutoff:
		return colorize(colorRed, fmt.Sprintf("%.0f", info.Temp))
	case info.Temp > settings.TempLimit:
		return colorize(colorRed, fmt.Sprintf("%.0f", info.Temp))
	case info.Temp >= settings.TargetTemp:
		return colorize(colorYellow, fmt.Sprintf("%.0f", info.Temp))
	default:
		return colorize(colorGreen, fmt.Sprintf("%.0f", info.Temp))
	}
}

func formatVRTemp(info lib.Info, settings lib.Settings) string {
	if info.VRTemp <= 0 {
		return colorize(colorYellow, "N/A")
	}
	formatted := fmt.Sprintf("%.0f", info.VRTemp)
	if info.VRTemp >= settings.VRTempHigh {
		return colorize(colorRed, formatted)
	}
	return colorize(colorGreen, formatted)
}

func formatPower(info lib.Info, settings lib.Settings) string {
	if info.Power <= 0 {
		return colorize(colorYellow, "N/A")
	}
	formatted := fmt.Sprintf("%.0f", info.Power)
	if info.Power >= settings.MaxPower {
		return colorize(colorRed, formatted)
	}
	return colorize(colorGreen, formatted)
}

func formatFan(info lib.Info) string {
	switch {
	case info.FanSpeed <= 0 && info.FanRPM <= 0:
		return colorize(colorYellow, "N/A")
	case info.FanRPM > 0:
		return fmt.Sprintf("%.0f%%/%d", info.FanSpeed, info.FanRPM)
	default:
		return fmt.Sprintf("%.0f%%", info.FanSpeed)
	}
}

func (minerController *controller) logf(format string, arguments ...any) {
	if minerController != nil && minerController.logger != nil {
		minerController.logger.Printf(format, arguments...)
		return
	}
	log.Printf(format, arguments...)
}

func (minerController *controller) outputWriter() io.Writer {
	if minerController != nil && minerController.output != nil {
		return minerController.output
	}
	return os.Stdout
}
