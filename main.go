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
	"sync"
	"syscall"
	"time"

	"github.com/willyrgf/bitagnis/lib"
)

const (
	colorReset      = "\033[0m"
	colorRed        = "\033[31m"
	colorGreen      = "\033[32m"
	colorYellow     = "\033[33m"
	pollWorkerLimit = 16
)

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
	LoadOrCreate(lib.Info, string, time.Time) (lib.MinerState, bool, error)
	LoadMiner(string) (lib.MinerState, error)
	SaveMiner(*lib.MinerState) error
	SavePoint(*lib.OperatingPointRecord) error
	ListPoints(string) ([]lib.OperatingPointRecord, error)
}

type controller struct {
	devices  deviceAPI
	states   optimizerStateStore
	settings lib.SettingsFile
	logger   *log.Logger
	output   io.Writer

	runtimeMu sync.Mutex
	runtimes  map[string]*minerRuntime

	asicMu    sync.Mutex
	asicCache map[string]lib.ASICSettings

	mutations *mutationCoordinator
}

type pollJob struct {
	index int
	miner lib.DiscoveredMiner
}

type minerPollResult struct {
	line              string
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
	store, err := lib.OpenOptimizerStore("optimizer.db")
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			log.Printf("Optimizer database shutdown failed: %s", closeErr)
		}
	}()

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
		devices:   client,
		states:    store,
		settings:  settingsFile,
		logger:    log.Default(),
		output:    os.Stdout,
		runtimes:  make(map[string]*minerRuntime),
		asicCache: make(map[string]lib.ASICSettings),
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
		rediscover,
		os.LookupEnv,
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
	hostnames map[string]bool
	reapply   map[string]bool
}

func parseArguments(arguments []string) (commandOptions, error) {
	options := commandOptions{
		hostnames: make(map[string]bool),
		reapply:   make(map[string]bool),
	}
	reapply := false
	for _, argument := range arguments {
		switch {
		case argument == "--reapply-mining":
			if reapply {
				return commandOptions{}, fmt.Errorf("--reapply-mining was specified more than once")
			}
			reapply = true
		case argument == "":
			return commandOptions{}, fmt.Errorf("hostname arguments cannot be empty")
		case argument[0] == '-':
			return commandOptions{}, fmt.Errorf("unknown flag %q", argument)
		default:
			options.hostnames[argument] = true
		}
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

func (minerController *controller) pollMiners(
	ctx context.Context,
	miners []lib.DiscoveredMiner,
	now time.Time,
) {
	output := minerController.outputWriter()
	fmt.Fprintf(
		output,
		"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
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
	)

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
	allowOptimization := minerController.mutations == nil ||
		minerController.mutations.GateOpen()
	for index := range results {
		observation := results[index].observation
		if observation == nil {
			continue
		}
		observations[observation.state.MacAddr] = observation
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
		if err := minerController.mutations.Advance(ctx, observations, now); err != nil {
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
		if err := minerController.mutations.Advance(ctx, observations, now); err != nil {
			minerController.logf("Mutation coordination failed: %s", err)
		}
	}
	for index := range results {
		observation := results[index].observation
		if observation == nil {
			continue
		}
		results[index].line = minerController.formatMinerLine(
			observation.state,
			observation.info,
			observation.settings,
			now,
		)
		results[index].hashRate = observation.info.HashRate
		results[index].hashRateAvailable = validHashRate(observation.info.HashRate)
	}

	sort.Slice(results, func(left int, right int) bool {
		return results[left].line < results[right].line
	})
	totalHashRate := 0.0
	hashRateAvailable := false
	for _, result := range results {
		if result.line != "" {
			fmt.Fprint(output, result.line)
		}
		if result.hashRateAvailable {
			totalHashRate += result.hashRate
			hashRateAvailable = true
		}
	}
	fmt.Fprintf(output, "\nTotal: %s\n\n", formatAggregateHashRate(totalHashRate, hashRateAvailable))
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
	asic, err := minerController.cachedASICSettings(ctx, info.MacAddr, miner.IP)
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
	state, created, err := minerController.states.LoadOrCreate(info, miner.IP, now)
	if err != nil {
		return minerPollResult{}, err
	}
	if created {
		state.RampUntil = now.Add(settings.RampUpTime)
		if err := minerController.states.SaveMiner(&state); err != nil {
			return minerPollResult{}, err
		}
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

func (minerController *controller) cachedASICSettings(
	ctx context.Context,
	macAddr string,
	ip string,
) (lib.ASICSettings, error) {
	minerController.asicMu.Lock()
	settings, ok := minerController.asicCache[macAddr]
	minerController.asicMu.Unlock()
	if ok {
		return settings, nil
	}

	settings, err := minerController.devices.GetASICSettings(ctx, ip)
	if err != nil {
		return lib.ASICSettings{}, err
	}
	minerController.asicMu.Lock()
	minerController.asicCache[macAddr] = settings
	minerController.asicMu.Unlock()
	return settings, nil
}

func (minerController *controller) formatMinerLine(
	state lib.MinerState,
	info lib.Info,
	settings lib.Settings,
	now time.Time,
) string {
	return fmt.Sprintf(
		"%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
		info.Hostname,
		info.Frequency,
		formatCoreVoltage(info),
		formatState(state, info, now),
		minerController.formatWindow(state, settings, now),
		formatTemp(info, settings),
		formatVRTemp(info, settings),
		formatHashRate(info),
		formatPower(info, settings),
		formatFan(info),
	)
}

func formatCoreVoltage(info lib.Info) string {
	if info.CoreVoltageActual > 0 {
		return fmt.Sprintf("%d/%.0f", info.CoreVoltage, info.CoreVoltageActual)
	}
	return fmt.Sprintf("%d", info.CoreVoltage)
}

func validHashRate(hashRate float64) bool {
	return hashRate >= 0 && !math.IsNaN(hashRate) && !math.IsInf(hashRate, 0)
}

func formatHashRate(info lib.Info) string {
	if !validHashRate(info.HashRate) {
		return colorYellow + "N/A" + colorReset
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
		return colorRed + formatted + colorReset
	case info.HashRate == 0:
		return colorYellow + formatted + colorReset
	default:
		return colorGreen + formatted + colorReset
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
		return colorRed + string(lib.PhaseOverheat) + colorReset
	case state.PendingKind != "" || state.MiningPending:
		return colorYellow + "APPLYING" + colorReset
	case now.Before(state.CooldownUntil):
		return colorYellow + string(lib.PhaseCooldown) + colorReset
	case state.Phase == lib.PhaseHold:
		return colorGreen + string(state.Phase) + colorReset
	default:
		return colorYellow + string(state.Phase) + colorReset
	}
}

func formatTemp(info lib.Info, settings lib.Settings) string {
	if info.Temp <= 0 {
		return colorYellow + "N/A" + colorReset
	}
	switch {
	case info.Temp >= settings.TempCutoff:
		return colorRed + fmt.Sprintf("%.0f", info.Temp) + colorReset
	case info.Temp > settings.TempLimit:
		return colorRed + fmt.Sprintf("%.0f", info.Temp) + colorReset
	case info.Temp >= settings.TargetTemp:
		return colorYellow + fmt.Sprintf("%.0f", info.Temp) + colorReset
	default:
		return colorGreen + fmt.Sprintf("%.0f", info.Temp) + colorReset
	}
}

func formatVRTemp(info lib.Info, settings lib.Settings) string {
	if info.VRTemp <= 0 {
		return colorYellow + "N/A" + colorReset
	}
	formatted := fmt.Sprintf("%.0f", info.VRTemp)
	if info.VRTemp >= settings.VRTempHigh {
		return colorRed + formatted + colorReset
	}
	return colorGreen + formatted + colorReset
}

func formatPower(info lib.Info, settings lib.Settings) string {
	if info.Power <= 0 {
		return colorYellow + "N/A" + colorReset
	}
	formatted := fmt.Sprintf("%.0f", info.Power)
	if info.Power >= settings.MaxPower {
		return colorRed + formatted + colorReset
	}
	return colorGreen + formatted + colorReset
}

func formatFan(info lib.Info) string {
	switch {
	case info.FanSpeed <= 0 && info.FanRPM <= 0:
		return colorYellow + "N/A" + colorReset
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
