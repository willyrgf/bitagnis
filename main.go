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
	PatchOverheatRecovery(context.Context, lib.OperatingPoint, string) error
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
	BootstrapMiner(lib.Info, string, time.Time, time.Duration, time.Duration, bool) (lib.MinerState, bool, error)
	LoadMiner(string) (lib.MinerState, error)
	SaveMiner(*lib.MinerState) error
	ListPoints(string) ([]lib.OperatingPointRecord, error)
	ResetOptimizationPass(string, lib.OperatingPoint, time.Time, time.Time, time.Time) error
	AdmitTrial(*lib.MinerState, lib.OperatingPoint, lib.OperatingPoint, lib.OptimizerPhase, float64, time.Time, time.Time) (int64, error)
	FinalizeTrial(*lib.MinerState, lib.OperatingPointRecord, lib.TrialDecision, time.Time, time.Time, time.Time) error
	FinalizeBaseline(*lib.MinerState, lib.OperatingPointRecord, bool, time.Time) error
	AdoptManualPoint(*lib.MinerState, lib.OperatingPoint, time.Time, time.Time, time.Time) error
	AdoptExternalPoint(*lib.MinerState, lib.OperatingPoint, int64, time.Time, time.Time, time.Time) error
	StartMutationAttempt(*lib.MutationAttempt) (int64, error)
	AdvanceMutationAttempt(int64, lib.MutationMilestone, time.Time) error
	RecordConfiguredVerification(int64, time.Time, int) error
	RecordFirstPositive(int64, time.Time) error
	CompleteMiningResume(*lib.MinerState, int64, time.Time, time.Time, time.Time) error
	FailMutationAndSave(*lib.MinerState, int64, lib.MutationFailureStage, time.Time) error
	FailMutationAndFinalizeTrial(*lib.MinerState, lib.OperatingPointRecord, lib.TrialDecision, int64, lib.MutationFailureStage, time.Time, time.Time, time.Time) error
	QuarantineMutation(*lib.MinerState, int64, lib.MutationFailureStage, time.Time) error
	SupersedeMutation(*lib.MinerState, *lib.MinerState, int64, time.Time) error
	PersistSafetyTransition(*lib.MinerState, *lib.MinerState, *lib.OperatingPointRecord, time.Time) error
	CompleteMutationAttempt(*lib.MinerState, int64, time.Time) error
	ListMutationAttempts(string) ([]lib.MutationAttempt, error)
	PendingMutationResume(string) (lib.MutationAttempt, bool, error)
	UnfinishedMutationAttempt(string) (lib.MutationAttempt, bool, error)
	CompareAndSetHourly(string, time.Time, time.Time, []lib.HourlyAggregate, time.Time) error
	ListHourly(string, time.Time, time.Time) ([]lib.HourlyAggregate, error)
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
	if !treatment && state.PassTrigger == lib.PassOperator && state.PassReferenceHash > 0 &&
		!state.PassStartedAt.Before(window.End) {
		boundaryRate := 0.0
		boundaryReference := 0.0
		if state.PassStartedAt.Equal(window.End) {
			// The schema-v5 snapshot is persisted, but this report-reader phase
			// still defers consuming its point and settlement fields. Keep the
			// boundary unavailable rather than inferring it from current state.
			boundaryRate = state.PassReferenceHash
			boundaryReference = state.PassReferenceHash
		}
		return lib.ReportMinerInput{
			MacAddr:               state.MacAddr,
			Hostname:              hostname,
			PreArmSettledHashRate: boundaryRate,
			PassStartedAt:         state.PassStartedAt,
			PassReferenceHash:     boundaryReference,
			BoundarySettled:       false,
			PointStable:           false,
			PointRecords:          records,
			Hourly:                hourly,
			MutationAttempts:      attempts,
		}, nil
	}
	selected, found := findRecord(records, state.CurrentPoint())
	if !found || selected.Status != lib.PointValidated || !finite(selected.MedianHash) || selected.MedianHash <= 0 {
		return lib.ReportMinerInput{}, fmt.Errorf("long-term report %s: no positive validated selected point", hostname)
	}
	pointStable := state.Phase == lib.PhaseHold && state.HoldReason == lib.HoldOptimized &&
		!state.SettledAt.IsZero() && state.PendingKind == "" && !state.MiningPending
	for _, attempt := range attempts {
		if attempt.Kind != lib.MutationOperatingPoint && attempt.Kind != lib.MutationSafetyRollback && attempt.Kind != lib.MutationOverheatRecovery {
			continue
		}
		if mutationOverlapsReportWindow(attempt, window) {
			pointStable = false
		}
	}
	preArmRate := selected.MedianHash
	if state.PassStartedAt.Equal(window.Start) && state.PassReferenceHash > 0 {
		preArmRate = state.PassReferenceHash
	}
	if treatment && (state.PassTrigger != lib.PassOperator || !state.PassStartedAt.Equal(window.Start) || state.PassReferenceHash <= 0) {
		return lib.ReportMinerInput{}, fmt.Errorf("long-term report %s: treatment pass boundary is not durably frozen at the arm start", hostname)
	}
	return lib.ReportMinerInput{
		MacAddr:  state.MacAddr,
		Hostname: hostname, PreArmSettledHashRate: preArmRate,
		PassStartedAt: state.PassStartedAt, PassReferenceHash: state.PassReferenceHash,
		SettledAt:       state.SettledAt,
		BoundarySettled: !state.SettledAt.IsZero() && !state.SettledAt.After(window.Start),
		PointStable:     pointStable, PointRecords: records,
		NormalRestartBaselineRequests: baselineRequests, NormalRestartBaselineObserved: baselineObserved,
		Hourly: hourly, MutationAttempts: attempts,
	}, nil
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
	output := minerController.outputWriter()
	results := make([]minerPollResult, len(miners))
	jobs := make(chan pollJob, len(miners))
	for index, miner := range miners {
		jobs <- pollJob{index: index, miner: miner}
	}
	close(jobs)

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

	observations := make(map[string]*minerObservation, len(results))
	handled := make(map[string]bool, len(results))
	allowOptimization := minerController.mutations == nil
	for index := range results {
		observation := results[index].observation
		if observation == nil {
			if index < len(miners) {
				if state, err := minerController.states.LoadMiner(miners[index].Info.MacAddr); err == nil {
					if err := minerController.accountHourly(miners[index].Info.MacAddr, nil, &state, lib.Info{}, now); err != nil && !errors.Is(err, context.Canceled) {
						minerController.logf("Hourly accounting failed for %s: %s", miners[index].Info.Hostname, err)
					}
				}
			}
			continue
		}
		observations[observation.state.MacAddr] = observation
		if err := minerController.accountHourly(observation.state.MacAddr, observation, &observation.state, observation.info, now); err != nil && !errors.Is(err, context.Canceled) {
			minerController.logf("Hourly accounting failed for %s: %s", observation.info.Hostname, err)
		}
		wasHandled, err := minerController.enforceMinerSafety(
			ctx,
			&observation.state,
			observation.info,
			observation.asic,
			observation.settings,
			now,
		)
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
		var err error
		allowOptimization, err = minerController.mutations.Advance(ctx, observations, now)
		if err != nil {
			minerController.logf("Mutation coordination failed: %s", err)
		}
	}
	for index := range results {
		observation := results[index].observation
		if observation == nil || handled[observation.state.MacAddr] {
			continue
		}
		if err := minerController.controlMinerAfterSafety(
			ctx,
			&observation.state,
			observation.info,
			observation.asic,
			observation.settings,
			now,
			allowOptimization,
		); err != nil && !errors.Is(err, context.Canceled) {
			minerController.logf(
				"Optimizer control failed for %s: %s",
				observation.info.Hostname,
				err,
			)
		}
	}
	if minerController.mutations != nil && allowOptimization {
		if _, err := minerController.mutations.Advance(ctx, observations, now); err != nil {
			minerController.logf("Mutation coordination failed: %s", err)
		}
	}
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
	current.classification = classifyAccountingState(*state, current.referenceHash, current.settled)
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
		seconds := segmentEnd.Sub(cursor).Seconds()
		fragment := lib.HourlyAggregate{MacAddr: macAddr, HourStartedAt: hour}
		if actual {
			fragment.ObservedSeconds = seconds
			fragment.ActualHashSeconds = current.hashRate * seconds
			if current.phase == lib.PhaseUndervolt || current.phase == lib.PhaseFrequencyTest || current.phase == lib.PhaseVoltageTest {
				if current.state.CurrentPoint() != current.state.FallbackPoint() {
					fragment.TrialSeconds = seconds
					fragment.TrialActualHashSeconds = fragment.ActualHashSeconds
					fragment.IncumbentCounterfactualHashSeconds = current.referenceHash * seconds
				}
			}
			if current.settled {
				fragment.SettledSeconds = seconds
			}
		} else {
			fragment.UnknownGapSeconds = seconds
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
	if (state.HoldReason != lib.HoldOptimized && state.HoldReason != lib.HoldSafety) ||
		state.Phase != lib.PhaseHold || state.SettledAt.IsZero() || !state.EvidenceDeadlineAt.IsZero() ||
		state.PendingKind != "" || state.MiningPending || now.Before(state.RampUntil) ||
		info.MacAddr != state.MacAddr || operatingPointFromInfo(info) != state.CurrentPoint() ||
		canonicalASICGrid(asic) != nil || !operatingPointAdvertised(asic, state.CurrentPoint()) ||
		!completeSafetyTelemetry(info) || hasPowerFault(info) {
		return false
	}
	minimum, err := minimumAdvertisedPoint(asic)
	if err != nil || assessInstantaneousSafety(info, settings, state.CurrentPoint(), minimum).action != safetyNormal {
		return false
	}
	attempts, err := minerController.states.ListMutationAttempts(state.MacAddr)
	if err != nil {
		return false
	}
	for _, attempt := range attempts {
		if attempt.FailedAt.IsZero() && attempt.MiningResumedAt.IsZero() {
			return false
		}
	}
	if state.HoldReason == lib.HoldSafety {
		return state.SafetyReason != ""
	}
	if state.SafetyReason != "" {
		return false
	}
	records, err := minerController.states.ListPoints(state.MacAddr)
	if err != nil {
		return false
	}
	selected, ok := selectFinalPoint(records, asic, settings)
	best, bestOK := selectBestPoint(records, asic, settings)
	return ok && bestOK && selected.Point() == state.CurrentPoint() && selected.Status == lib.PointValidated &&
		best.MedianHash == state.BestHashRate && best.Point() == state.BestPoint()
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
	state, created, err := minerController.states.BootstrapMiner(
		info,
		miner.IP,
		now,
		settings.RampUpTime,
		settings.EvaluationWindowTime,
		pairAdvertised,
	)
	if err != nil {
		return minerPollResult{}, err
	}
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
	case info.OverHeatMode != 0 || state.Phase == lib.PhaseOverheat:
		return colorize(colorRed, string(lib.PhaseOverheat))
	case state.PendingKind == lib.MutationSafetyRollback:
		return colorize(colorRed, "ROLLBACK")
	case state.PendingKind != "" || state.MiningPending:
		return colorize(colorYellow, "PENDING")
	case now.Before(state.CooldownUntil):
		return colorize(colorYellow, string(lib.PhaseCooldown))
	case state.Phase == lib.PhaseHold:
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
