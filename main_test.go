package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/willyrgf/bitagnis/lib"
)

type operatingRequest struct {
	point    lib.OperatingPoint
	ip       string
	recovery bool
}

type miningRequest struct {
	settings         lib.MiningSettings
	primaryPassword  string
	fallbackPassword string
	ip               string
}

type fakeDeviceAPI struct {
	mu sync.Mutex

	getInfo      func(context.Context, string) (lib.Info, error)
	asicSettings lib.ASICSettings
	asicError    error
	setError     error
	recoverError error
	miningError  error
	restartError error
	requests     []operatingRequest
	mining       []miningRequest
	restarts     []string
	patchHook    func(lib.MutationKind, lib.OperatingPoint)
	asicHook     func()
}

func (fake *fakeDeviceAPI) GetSystemInfo(
	ctx context.Context,
	ip string,
) (lib.Info, error) {
	if fake.getInfo == nil {
		return lib.Info{}, errors.New("no fake system info configured")
	}
	return fake.getInfo(ctx, ip)
}

func (fake *fakeDeviceAPI) GetASICSettings(
	context.Context,
	string,
) (lib.ASICSettings, error) {
	if fake.asicHook != nil {
		fake.asicHook()
	}
	return fake.asicSettings, fake.asicError
}

func (fake *fakeDeviceAPI) PatchOperatingPoint(
	_ context.Context,
	point lib.OperatingPoint,
	ip string,
) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.requests = append(fake.requests, operatingRequest{point: point, ip: ip})
	if fake.patchHook != nil {
		fake.patchHook(lib.MutationOperatingPoint, point)
	}
	return fake.setError
}

func (fake *fakeDeviceAPI) PatchOverheatRecovery(
	_ context.Context,
	point lib.OperatingPoint,
	ip string,
) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.requests = append(fake.requests, operatingRequest{
		point:    point,
		ip:       ip,
		recovery: true,
	})
	if fake.patchHook != nil {
		fake.patchHook(lib.MutationOverheatRecovery, point)
	}
	return fake.recoverError
}

func (fake *fakeDeviceAPI) PatchMiningConfiguration(
	_ context.Context,
	settings lib.MiningSettings,
	primaryPassword string,
	fallbackPassword string,
	ip string,
) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.mining = append(fake.mining, miningRequest{
		settings:         settings,
		primaryPassword:  primaryPassword,
		fallbackPassword: fallbackPassword,
		ip:               ip,
	})
	if fake.patchHook != nil {
		fake.patchHook(lib.MutationMiningConfiguration, lib.OperatingPoint{})
	}
	return fake.miningError
}

func (fake *fakeDeviceAPI) Restart(_ context.Context, ip string) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.restarts = append(fake.restarts, ip)
	return fake.restartError
}

func (fake *fakeDeviceAPI) operatingRequests() []operatingRequest {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]operatingRequest(nil), fake.requests...)
}

func (fake *fakeDeviceAPI) miningRequests() []miningRequest {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]miningRequest(nil), fake.mining...)
}

func (fake *fakeDeviceAPI) restartRequests() []string {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]string(nil), fake.restarts...)
}

type memoryOptimizerStore struct {
	mu      sync.Mutex
	states  map[string]lib.MinerState
	records map[string]lib.OperatingPointRecord
	saveErr error
}

func newMemoryOptimizerStore() *memoryOptimizerStore {
	return &memoryOptimizerStore{
		states:  make(map[string]lib.MinerState),
		records: make(map[string]lib.OperatingPointRecord),
	}
}

func (store *memoryOptimizerStore) LoadOrCreate(
	info lib.Info,
	ip string,
	now time.Time,
) (lib.MinerState, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if state, ok := store.states[info.MacAddr]; ok {
		state.IP = ip
		state.Hostname = info.Hostname
		store.states[info.MacAddr] = state
		return state, false, nil
	}
	state := lib.MinerState{
		MacAddr:        info.MacAddr,
		Hostname:       info.Hostname,
		IP:             ip,
		Phase:          lib.PhaseBaseline,
		PhaseStartedAt: now,
	}
	point := operatingPointFromInfo(info)
	if info.OverHeatMode == 0 && validLivePoint(point) {
		state.SetCurrentPoint(point)
	} else if info.OverHeatMode != 0 {
		state.Phase = lib.PhaseOverheat
		state.OverheatCount = 1
	}
	store.states[state.MacAddr] = state
	return state, true, nil
}

func (store *memoryOptimizerStore) SaveMiner(state *lib.MinerState) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.saveErr != nil {
		return store.saveErr
	}
	store.states[state.MacAddr] = *state
	return nil
}

func (store *memoryOptimizerStore) LoadMiner(macAddr string) (lib.MinerState, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	state, ok := store.states[macAddr]
	if !ok {
		return lib.MinerState{}, fmt.Errorf("missing state %s", macAddr)
	}
	return state, nil
}

func pointRecordKey(mac string, point lib.OperatingPoint) string {
	return fmt.Sprintf("%s/%d/%d", mac, point.Frequency, point.CoreVoltage)
}

func (store *memoryOptimizerStore) SavePoint(record *lib.OperatingPointRecord) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.saveErr != nil {
		return store.saveErr
	}
	store.records[pointRecordKey(record.MacAddr, record.Point())] = *record
	return nil
}

func (store *memoryOptimizerStore) ListPoints(
	macAddr string,
) ([]lib.OperatingPointRecord, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var records []lib.OperatingPointRecord
	for _, record := range store.records {
		if record.MacAddr == macAddr {
			records = append(records, record)
		}
	}
	return records, nil
}

func (store *memoryOptimizerStore) putState(state lib.MinerState) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.states[state.MacAddr] = state
}

func (store *memoryOptimizerStore) getState(macAddr string) lib.MinerState {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.states[macAddr]
}

func (store *memoryOptimizerStore) putRecord(record lib.OperatingPointRecord) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.records[pointRecordKey(record.MacAddr, record.Point())] = record
}

func optimizerSettings() lib.Settings {
	return lib.Settings{
		RecoveryTemp:            61,
		TargetTemp:              65,
		TempLimit:               66,
		TempCutoff:              70,
		MaxPower:                24,
		VRTempHigh:              97,
		MaxErrorPercentage:      5,
		MetricsInterval:         10,
		RampUpSeconds:           2,
		EvaluationWindowMinutes: 1,
		OverheatCooldownMins:    120,
		MetricsTime:             10 * time.Second,
		RampUpTime:              2 * time.Second,
		EvaluationWindowTime:    time.Minute,
	}
}

func gammaASIC() lib.ASICSettings {
	return lib.ASICSettings{
		ASICModel:        "BM1370",
		DefaultFrequency: 525,
		DefaultVoltage:   1150,
		FrequencyOptions: []int{400, 490, 525, 550, 600, 625},
		VoltageOptions:   []int{1000, 1060, 1100, 1150, 1200, 1250},
	}
}

func healthyInfo() lib.Info {
	return lib.Info{
		Version:           supportedAxeOSVersion,
		ASICModel:         supportedASICModel,
		BoardVersion:      supportedBoardVersion,
		Hostname:          "bitaxe-alpha",
		MacAddr:           "aa:bb:cc:dd:ee:ff",
		Frequency:         400,
		CoreVoltage:       1100,
		CoreVoltageActual: 1085,
		HashRate:          800,
		ExpectedHashRate:  816,
		Power:             14.8,
		Temp:              62,
		VRTemp:            49,
		FanSpeed:          100,
		FanRPM:            8450,
		UpTimeSeconds:     120,
		SharesAccepted:    100,
	}
}

func discovered(info lib.Info, ip string) lib.DiscoveredMiner {
	return lib.DiscoveredMiner{IP: ip, Info: info}
}

func optimizerState(now time.Time) lib.MinerState {
	state := lib.MinerState{
		MacAddr:        "aa:bb:cc:dd:ee:ff",
		Hostname:       "bitaxe-alpha",
		IP:             "192.0.2.10",
		Phase:          lib.PhaseHold,
		PhaseStartedAt: now,
	}
	state.SetCurrentPoint(lib.OperatingPoint{Frequency: 400, CoreVoltage: 1100})
	state.SetBestPoint(lib.OperatingPoint{Frequency: 400, CoreVoltage: 1100})
	state.BestHashRate = 800
	return state
}

func testController(
	devices deviceAPI,
	states optimizerStateStore,
	loggerOutput io.Writer,
) *controller {
	if loggerOutput == nil {
		loggerOutput = io.Discard
	}
	return &controller{
		devices: devices,
		states:  states,
		settings: lib.SettingsFile{
			Defaults: optimizerSettings(),
		},
		logger:   log.New(loggerOutput, "", 0),
		output:   io.Discard,
		runtimes: make(map[string]*minerRuntime),
		asicCache: map[string]lib.ASICSettings{
			"aa:bb:cc:dd:ee:ff": gammaASIC(),
		},
	}
}

func healthySummary(hash float64, expected float64) windowSummary {
	return windowSummary{
		MedianHash:   hash,
		ExpectedHash: expected,
		Attainment:   hash / expected,
		P95Temp:      62,
		P95VRTemp:    49,
		P95Power:     15,
	}
}

func TestParseArgumentsRejectsUnknownFlagsAndScopesReapply(t *testing.T) {
	options, err := parseArguments([]string{"--reapply-mining", "bitaxe-alpha", "bitaxe-beta"})
	if err != nil {
		t.Fatalf("parse reapply arguments: %v", err)
	}
	if len(options.hostnames) != 2 ||
		!options.reapply["bitaxe-alpha"] ||
		!options.reapply["bitaxe-beta"] {
		t.Fatalf("parsed options = %+v", options)
	}
	if _, err := parseArguments([]string{"--reapply-mining"}); err == nil {
		t.Fatal("reapply without hostnames was accepted")
	}
	if _, err := parseArguments([]string{"--unknown"}); err == nil {
		t.Fatal("unknown flag was accepted")
	}
	options, err = parseArguments(nil)
	if err != nil || !options.hostnames["all"] || len(options.reapply) != 0 {
		t.Fatalf("default options = %+v, error = %v", options, err)
	}
}

func TestNamedDiscoveryRequiresExactlyOneMACPerHostname(t *testing.T) {
	info := healthyInfo()
	missing := map[string]bool{"missing": true}
	if err := validateNamedDiscovery(missing, nil); err == nil {
		t.Fatal("missing selected hostname was accepted")
	}
	first := discovered(info, "192.0.2.10")
	secondInfo := info
	secondInfo.MacAddr = "00:11:22:33:44:55"
	second := discovered(secondInfo, "192.0.2.11")
	if err := validateNamedDiscovery(
		map[string]bool{info.Hostname: true},
		[]lib.DiscoveredMiner{first, second},
	); err == nil {
		t.Fatal("duplicate hostname mapped to different MACs was accepted")
	}
	if err := validateNamedDiscovery(
		map[string]bool{info.Hostname: true},
		[]lib.DiscoveredMiner{first},
	); err != nil {
		t.Fatalf("unique named discovery rejected: %v", err)
	}
}

func TestSummarizeWindowUsesMedianP95AndShareDeltas(t *testing.T) {
	errorValue := 2.0
	samples := []telemetrySample{
		{hashRate: 700, expectedHash: 816, temp: 60, vrTemp: 47, power: 14, errorPercent: &errorValue, acceptedShare: 100, rejectedShare: 2},
		{hashRate: 900, expectedHash: 816, temp: 65, vrTemp: 52, power: 17, errorPercent: &errorValue, acceptedShare: 120, rejectedShare: 3},
		{hashRate: 800, expectedHash: 816, temp: 62, vrTemp: 49, power: 15, errorPercent: &errorValue, acceptedShare: 130, rejectedShare: 3},
	}
	summary := summarizeWindow(samples)
	if summary.MedianHash != 800 ||
		summary.MeanTemp != (60.0+65.0+62.0)/3 ||
		summary.P95Temp != 65 ||
		summary.P95Power != 17 ||
		summary.AcceptedDelta != 30 ||
		summary.RejectedDelta != 1 ||
		summary.ErrorPercent == nil ||
		*summary.ErrorPercent != 2 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestBootstrapWaitsForCompleteWindowThenUndervolts(t *testing.T) {
	info := healthyInfo()
	devices := &fakeDeviceAPI{
		getInfo: func(context.Context, string) (lib.Info, error) {
			return info, nil
		},
		asicSettings: gammaASIC(),
	}
	states := newMemoryOptimizerStore()
	minerController := testController(devices, states, nil)
	now := time.Now().Round(time.Second)

	miner := discovered(info, "192.0.2.10")
	minerController.pollMiners(context.Background(), []lib.DiscoveredMiner{miner}, now)
	if len(devices.operatingRequests()) != 0 {
		t.Fatal("bootstrap changed settings immediately")
	}

	for sample := 0; sample < 6; sample++ {
		at := now.Add(2*time.Second + time.Duration(sample)*10*time.Second)
		minerController.pollMiners(context.Background(), []lib.DiscoveredMiner{miner}, at)
	}
	got := states.getState(info.MacAddr)
	if got.PendingKind != lib.MutationOperatingPoint ||
		got.PendingPoint() != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}) {
		t.Fatalf("pending mutation = %+v, want first undervolt 400/1060", got)
	}
	if len(devices.operatingRequests()) != 0 {
		t.Fatal("optimizer bypassed the mutation coordinator")
	}
}

func TestPendingPairIsNotConfirmedFromConfiguredReadback(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	state.SetPendingMutation(
		lib.MutationOperatingPoint,
		lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060},
		now,
	)
	state.Phase = lib.PhaseUndervolt
	states := newMemoryOptimizerStore()
	states.putState(state)
	minerController := testController(&fakeDeviceAPI{}, states, nil)
	info := healthyInfo()
	info.CoreVoltage = 1060

	if err := minerController.controlMiner(
		context.Background(),
		&state,
		info,
		gammaASIC(),
		optimizerSettings(),
		now.Add(time.Second),
	); err != nil {
		t.Fatalf("controlMiner returned an error: %v", err)
	}
	got := states.getState(state.MacAddr)
	if got.PendingKind != lib.MutationOperatingPoint ||
		got.CurrentPoint() == (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}) {
		t.Fatalf("NVS-only readback confirmed pending state: %+v", got)
	}
}

func TestFailedUndervoltRollsBackBothSettings(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	state.SetCurrentPoint(lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060})
	state.SetFallbackPoint(lib.OperatingPoint{Frequency: 400, CoreVoltage: 1100})
	state.Phase = lib.PhaseUndervolt
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{}
	minerController := testController(devices, states, nil)

	summary := healthySummary(650, 816)
	if err := minerController.evaluateTrial(
		context.Background(),
		&state,
		summary,
		gammaASIC(),
		optimizerSettings(),
		now,
	); err != nil {
		t.Fatalf("evaluateTrial returned an error: %v", err)
	}
	if state.PendingKind != lib.MutationOperatingPoint ||
		state.PendingPoint() != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1100}) {
		t.Fatalf("rollback intent = %+v", state)
	}
}

func TestUnderperformingFrequencyTestsOneHigherVoltage(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	state.SetCurrentPoint(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1060})
	state.SetFallbackPoint(lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060})
	state.SetBestPoint(lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060})
	state.Phase = lib.PhaseFrequencyTest
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{}
	minerController := testController(devices, states, nil)

	summary := healthySummary(800, 999.6)
	if err := minerController.evaluateTrial(
		context.Background(),
		&state,
		summary,
		gammaASIC(),
		optimizerSettings(),
		now,
	); err != nil {
		t.Fatalf("evaluateTrial returned an error: %v", err)
	}
	if state.PendingKind != lib.MutationOperatingPoint ||
		state.PendingPoint() != (lib.OperatingPoint{Frequency: 490, CoreVoltage: 1100}) {
		t.Fatalf("voltage trial intent = %+v", state)
	}
	if got := states.getState(state.MacAddr).Phase; got != lib.PhaseVoltageTest {
		t.Fatalf("phase = %s, want VOLT_TEST", got)
	}
}

func TestVoltageWithoutMaterialResponseRollsBack(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	state.SetCurrentPoint(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1100})
	state.SetFallbackPoint(lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060})
	state.SetBestPoint(lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060})
	state.Phase = lib.PhaseVoltageTest
	states := newMemoryOptimizerStore()
	states.putState(state)
	states.putRecord(lib.OperatingPointRecord{
		MacAddr:      state.MacAddr,
		Frequency:    490,
		CoreVoltage:  1060,
		Status:       lib.PointUnstable,
		MedianHash:   800,
		ExpectedHash: 999.6,
		Attainment:   0.80,
		P95Temp:      62,
		P95VRTemp:    49,
		P95Power:     15,
	})
	devices := &fakeDeviceAPI{}
	minerController := testController(devices, states, nil)

	summary := healthySummary(810, 999.6)
	if err := minerController.evaluateTrial(
		context.Background(),
		&state,
		summary,
		gammaASIC(),
		optimizerSettings(),
		now,
	); err != nil {
		t.Fatalf("evaluateTrial returned an error: %v", err)
	}
	if state.PendingKind != lib.MutationOperatingPoint ||
		state.PendingPoint() != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}) {
		t.Fatalf("rollback intent = %+v", state)
	}
}

func TestActualHashGainWinsDespiteLowExpectedAttainment(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	state.SetCurrentPoint(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1060})
	state.SetFallbackPoint(lib.OperatingPoint{Frequency: 400, CoreVoltage: 1100})
	state.BestHashRate = 718.8
	state.Phase = lib.PhaseVoltageTest
	states := newMemoryOptimizerStore()
	states.putState(state)
	states.putRecord(lib.OperatingPointRecord{
		MacAddr:      state.MacAddr,
		Frequency:    490,
		CoreVoltage:  1000,
		Status:       lib.PointNoGain,
		MedianHash:   532.8,
		ExpectedHash: 999.6,
		Attainment:   0.533,
		MeanTemp:     54,
		P95Temp:      55.3,
		P95VRTemp:    45,
		P95Power:     13.6,
		RetryAfter:   now.Add(blockedPointRetry),
	})
	devices := &fakeDeviceAPI{}
	minerController := testController(devices, states, nil)
	summary := healthySummary(826.3, 999.6)
	summary.Attainment = 0.827
	summary.MeanTemp = 60.2
	summary.P95Temp = 61.3

	if err := minerController.evaluateTrial(
		context.Background(),
		&state,
		summary,
		gammaASIC(),
		optimizerSettings(),
		now,
	); err != nil {
		t.Fatalf("evaluateTrial returned an error: %v", err)
	}
	saved := states.getState(state.MacAddr)
	if saved.BestPoint() != (lib.OperatingPoint{Frequency: 490, CoreVoltage: 1060}) ||
		saved.BestHashRate != 826.3 {
		t.Fatalf("actual-hash winner was not accepted: %+v", saved)
	}
	record := states.records[pointRecordKey(
		state.MacAddr,
		lib.OperatingPoint{Frequency: 490, CoreVoltage: 1060},
	)]
	if record.Status != lib.PointValidated || record.MeanTemp != 60.2 {
		t.Fatalf("accepted point record = %+v", record)
	}
	if state.PendingKind != lib.MutationOperatingPoint ||
		state.PendingPoint() != (lib.OperatingPoint{Frequency: 490, CoreVoltage: 1100}) {
		t.Fatalf("next voltage intent = %+v", state)
	}
}

func TestHigherFrequencySweepStartsAtMinimumVoltage(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	states := newMemoryOptimizerStore()
	states.putState(state)
	for _, point := range []lib.OperatingPoint{
		{Frequency: 400, CoreVoltage: 1060},
		{Frequency: 400, CoreVoltage: 1150},
	} {
		states.putRecord(lib.OperatingPointRecord{
			MacAddr:      state.MacAddr,
			Frequency:    point.Frequency,
			CoreVoltage:  point.CoreVoltage,
			Status:       lib.PointNoGain,
			MedianHash:   650,
			ExpectedHash: 816,
			MeanTemp:     58,
			P95Temp:      60,
			P95VRTemp:    48,
			P95Power:     15,
			RetryAfter:   now.Add(blockedPointRetry),
		})
	}
	devices := &fakeDeviceAPI{}
	minerController := testController(devices, states, nil)

	if err := minerController.startNextCandidate(
		context.Background(),
		&state,
		healthySummary(800, 816),
		gammaASIC(),
		now,
	); err != nil {
		t.Fatalf("startNextCandidate returned an error: %v", err)
	}
	if state.PendingKind != lib.MutationOperatingPoint ||
		state.PendingPoint() != (lib.OperatingPoint{Frequency: 490, CoreVoltage: 1000}) {
		t.Fatalf("frequency sweep intent = %+v", state)
	}
}

func TestThermalVoltageRollsBackToActualHashBest(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	state.SetCurrentPoint(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1100})
	state.SetBestPoint(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1060})
	state.BestHashRate = 826.3
	state.SetFallbackPoint(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1060})
	state.Phase = lib.PhaseVoltageTest
	states := newMemoryOptimizerStore()
	states.putState(state)
	states.putRecord(lib.OperatingPointRecord{
		MacAddr:      state.MacAddr,
		Frequency:    490,
		CoreVoltage:  1060,
		Status:       lib.PointValidated,
		MedianHash:   826.3,
		ExpectedHash: 999.6,
		Attainment:   0.827,
		MeanTemp:     60.2,
		P95Temp:      61.3,
		P95VRTemp:    49,
		P95Power:     15.3,
	})
	devices := &fakeDeviceAPI{}
	minerController := testController(devices, states, nil)
	summary := healthySummary(768.3, 999.6)
	summary.MeanTemp = 65.4
	summary.P95Temp = 66.3
	summary.P95VRTemp = 51
	summary.P95Power = 16.7

	if err := minerController.evaluateWindow(
		context.Background(),
		&state,
		summary,
		gammaASIC(),
		optimizerSettings(),
		now,
	); err != nil {
		t.Fatalf("evaluateWindow returned an error: %v", err)
	}
	if state.PendingKind != lib.MutationSafetyRollback ||
		state.PendingPoint() != (lib.OperatingPoint{Frequency: 490, CoreVoltage: 1060}) {
		t.Fatalf("thermal rollback intent = %+v", state)
	}
}

func TestTargetTemperatureStopsExplorationBeforeHardLimit(t *testing.T) {
	settings := optimizerSettings()
	summary := healthySummary(820, 1000)
	summary.P95Temp = 64.9
	if !hasExplorationHeadroom(summary, settings) {
		t.Fatal("point below target temperature should retain exploration headroom")
	}
	summary.P95Temp = 65.2
	if hasExplorationHeadroom(summary, settings) {
		t.Fatal("point above target temperature should stop exploration")
	}
	if _, failed := windowSafetyFailure(summary, settings); failed {
		t.Fatal("point between target temperature and hard limit should remain valid")
	}
	summary.P95Temp = 66.1
	if _, failed := windowSafetyFailure(summary, settings); !failed {
		t.Fatal("point above hard limit should fail safety")
	}
}

func TestSafetyRollbackChangesFrequencyAndVoltageTogether(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	state.SetCurrentPoint(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1150})
	state.SetBestPoint(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1150})
	states := newMemoryOptimizerStore()
	states.putState(state)
	states.putRecord(lib.OperatingPointRecord{
		MacAddr:      state.MacAddr,
		Frequency:    400,
		CoreVoltage:  1060,
		Status:       lib.PointValidated,
		MedianHash:   790,
		ExpectedHash: 816,
		Attainment:   0.97,
		P95Temp:      62,
		P95VRTemp:    49,
		P95Power:     14,
	})
	devices := &fakeDeviceAPI{}
	minerController := testController(devices, states, nil)
	info := healthyInfo()
	info.Frequency = 490
	info.CoreVoltage = 1150
	info.HashRate = -1
	info.Temp = 67

	if err := minerController.controlMiner(
		context.Background(),
		&state,
		info,
		gammaASIC(),
		optimizerSettings(),
		now,
	); err != nil {
		t.Fatalf("controlMiner returned an error: %v", err)
	}
	if state.PendingKind != lib.MutationSafetyRollback ||
		state.PendingPoint() != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}) {
		t.Fatalf("safety rollback intent = %+v", state)
	}
}

func TestSafetyWithoutHistoryUsesMinimumAdvertisedPair(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	state.SetCurrentPoint(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1150})
	state.SetBestPoint(lib.OperatingPoint{})
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{}
	minerController := testController(devices, states, nil)
	info := healthyInfo()
	info.Frequency = 490
	info.CoreVoltage = 1150
	info.Power = 24

	if err := minerController.controlMiner(
		context.Background(),
		&state,
		info,
		gammaASIC(),
		optimizerSettings(),
		now,
	); err != nil {
		t.Fatalf("controlMiner returned an error: %v", err)
	}
	if state.PendingKind != lib.MutationSafetyRollback ||
		state.PendingPoint() != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}) {
		t.Fatalf("minimum safety intent = %+v", state)
	}
}

func TestInstantaneousSafetyBoundaries(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	oldPhaseStarted := now.Add(-time.Hour)
	tests := []struct {
		name          string
		mutate        func(*lib.Info)
		handled       bool
		phase         lib.OptimizerPhase
		kind          lib.MutationKind
		overheatCount int
	}{
		{
			name:    "exactly temperature limit",
			mutate:  func(info *lib.Info) { info.Temp = 66 },
			handled: false,
			phase:   lib.PhaseHold,
		},
		{
			name:    "incomplete otherwise normal telemetry",
			mutate:  func(info *lib.Info) { info.Power = 0 },
			handled: true,
			phase:   lib.PhaseHold,
		},
		{
			name:    "immediately above temperature limit",
			mutate:  func(info *lib.Info) { info.Temp = 66.001 },
			handled: true,
			phase:   lib.PhaseCooldown,
			kind:    lib.MutationSafetyRollback,
		},
		{
			name:    "immediately below host cutoff",
			mutate:  func(info *lib.Info) { info.Temp = 69.999 },
			handled: true,
			phase:   lib.PhaseCooldown,
			kind:    lib.MutationSafetyRollback,
		},
		{
			name:          "exactly host cutoff",
			mutate:        func(info *lib.Info) { info.Temp = 70 },
			handled:       true,
			phase:         lib.PhaseOverheat,
			kind:          lib.MutationSafetyRollback,
			overheatCount: 1,
		},
		{
			name:          "exactly AxeOS ASIC trip boundary",
			mutate:        func(info *lib.Info) { info.Temp = 75 },
			handled:       true,
			phase:         lib.PhaseOverheat,
			kind:          lib.MutationSafetyRollback,
			overheatCount: 1,
		},
		{
			name:          "above AxeOS ASIC trip boundary",
			mutate:        func(info *lib.Info) { info.Temp = 75.001 },
			handled:       true,
			phase:         lib.PhaseOverheat,
			overheatCount: 1,
		},
		{
			name:    "power immediately below limit",
			mutate:  func(info *lib.Info) { info.Power = 23.999 },
			handled: false,
			phase:   lib.PhaseHold,
		},
		{
			name:    "power exactly at limit",
			mutate:  func(info *lib.Info) { info.Power = 24 },
			handled: true,
			phase:   lib.PhaseCooldown,
			kind:    lib.MutationSafetyRollback,
		},
		{
			name:    "VR immediately below limit",
			mutate:  func(info *lib.Info) { info.VRTemp = 96.999 },
			handled: false,
			phase:   lib.PhaseHold,
		},
		{
			name:    "VR exactly at limit",
			mutate:  func(info *lib.Info) { info.VRTemp = 97 },
			handled: true,
			phase:   lib.PhaseCooldown,
			kind:    lib.MutationSafetyRollback,
		},
		{
			name:    "exactly AxeOS VR trip boundary",
			mutate:  func(info *lib.Info) { info.VRTemp = 105 },
			handled: true,
			phase:   lib.PhaseCooldown,
			kind:    lib.MutationSafetyRollback,
		},
		{
			name:          "above AxeOS VR trip boundary",
			mutate:        func(info *lib.Info) { info.VRTemp = 105.001 },
			handled:       true,
			phase:         lib.PhaseOverheat,
			overheatCount: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := optimizerState(oldPhaseStarted)
			state.SetCurrentPoint(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1100})
			state.PhaseStartedAt = oldPhaseStarted
			states := newMemoryOptimizerStore()
			states.putState(state)
			minerController := testController(&fakeDeviceAPI{}, states, nil)
			info := healthyInfo()
			info.Frequency = 490
			info.CoreVoltage = 1100
			test.mutate(&info)

			handled, err := minerController.enforceMinerSafety(
				context.Background(),
				&state,
				info,
				gammaASIC(),
				optimizerSettings(),
				now,
			)
			if err != nil {
				t.Fatalf("enforce safety: %v", err)
			}
			if handled != test.handled ||
				state.Phase != test.phase ||
				state.PendingKind != test.kind ||
				state.OverheatCount != test.overheatCount {
				t.Fatalf("safety state = handled %v, %+v", handled, state)
			}
			if test.kind != "" {
				if state.PendingPoint() != (lib.OperatingPoint{
					Frequency:   400,
					CoreVoltage: 1000,
				}) || !state.PendingSince.Equal(now) {
					t.Fatalf("pending safety intent = %+v", state)
				}
			} else if !state.PendingSince.IsZero() {
				t.Fatalf("unexpected pending age = %s", state.PendingSince)
			}
			if test.overheatCount != 0 {
				if !state.PhaseStartedAt.Equal(now) ||
					!state.CooldownUntil.Equal(now.Add(2*time.Hour)) {
					t.Fatalf("emergency episode age/cooldown = %+v", state)
				}
			} else if test.kind == "" && !state.PhaseStartedAt.Equal(oldPhaseStarted) {
				t.Fatalf("normal observation refreshed phase age: %+v", state)
			}
		})
	}
}

func TestRecoveryBoundariesRequireEveryPredicate(t *testing.T) {
	settings := optimizerSettings()
	base := healthyInfo()
	base.Temp = settings.RecoveryTemp
	base.Power = settings.MaxPower - powerHeadroom
	base.VRTemp = settings.VRTempHigh * vrExplorationFactor
	tests := []struct {
		name   string
		mutate func(*lib.Info)
		safe   bool
	}{
		{name: "all exact boundaries", safe: true, mutate: func(*lib.Info) {}},
		{
			name:   "temperature immediately above",
			mutate: func(info *lib.Info) { info.Temp += 0.001 },
		},
		{
			name:   "power immediately above",
			mutate: func(info *lib.Info) { info.Power += 0.001 },
		},
		{
			name:   "VR temperature immediately above",
			mutate: func(info *lib.Info) { info.VRTemp += 0.001 },
		},
		{
			name: "power fault",
			mutate: func(info *lib.Info) {
				fault := "synthetic fault"
				info.PowerFault = &fault
			},
		},
		{
			name:   "unsupported identity",
			mutate: func(info *lib.Info) { info.Version = "unsupported" },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info := base
			test.mutate(&info)
			if got := safeToRecover(info, settings); got != test.safe {
				t.Fatalf("safeToRecover = %v, want %v", got, test.safe)
			}
		})
	}
}

func TestHostEmergencyAtMinimumHoldsThenRecoversWithoutMutation(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	state := optimizerState(now.Add(-time.Hour))
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	state.SetCurrentPoint(minimum)
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{}
	minerController := testController(devices, states, nil)
	info := healthyInfo()
	info.Frequency = minimum.Frequency
	info.CoreVoltage = minimum.CoreVoltage
	info.Temp = optimizerSettings().TempCutoff

	handled, err := minerController.enforceMinerSafety(
		context.Background(),
		&state,
		info,
		gammaASIC(),
		optimizerSettings(),
		now,
	)
	if err != nil || !handled {
		t.Fatalf("enter minimum hold = %v, %v", handled, err)
	}
	episodeStarted := state.PhaseStartedAt
	cooldownUntil := state.CooldownUntil
	if state.Phase != lib.PhaseOverheat ||
		state.PendingKind != "" ||
		state.OverheatCount != 1 {
		t.Fatalf("minimum emergency hold = %+v", state)
	}

	recoveredAt := now.Add(15 * time.Minute)
	info.Temp = optimizerSettings().RecoveryTemp
	info.Power = optimizerSettings().MaxPower - powerHeadroom
	info.VRTemp = optimizerSettings().VRTempHigh * vrExplorationFactor
	handled, err = minerController.enforceMinerSafety(
		context.Background(),
		&state,
		info,
		gammaASIC(),
		optimizerSettings(),
		recoveredAt,
	)
	if err != nil || !handled {
		t.Fatalf("recover minimum hold = %v, %v", handled, err)
	}
	if state.Phase != lib.PhaseCooldown ||
		state.PendingKind != "" ||
		state.OverheatCount != 1 ||
		!state.CooldownUntil.Equal(cooldownUntil) ||
		state.PhaseStartedAt.Equal(episodeStarted) ||
		len(devices.operatingRequests()) != 0 ||
		len(devices.restartRequests()) != 0 {
		t.Fatalf("host recovery without mutation = %+v", state)
	}
}

func TestContainmentReadbackBeforeRebootProofDoesNotClearIntent(t *testing.T) {
	now := time.Now()
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	state := optimizerState(now.Add(-time.Hour))
	state.SetCurrentPoint(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1100})
	state.Phase = lib.PhaseOverheat
	state.PhaseStartedAt = now.Add(-30 * time.Minute)
	state.SetPendingMutation(
		lib.MutationSafetyRollback,
		minimum,
		now.Add(-20*time.Minute),
	)
	states := newMemoryOptimizerStore()
	states.putState(state)
	minerController := testController(&fakeDeviceAPI{}, states, nil)
	info := healthyInfo()
	info.Frequency = minimum.Frequency
	info.CoreVoltage = minimum.CoreVoltage
	info.Temp = optimizerSettings().RecoveryTemp
	info.Power = optimizerSettings().MaxPower - powerHeadroom
	info.VRTemp = optimizerSettings().VRTempHigh * vrExplorationFactor

	if _, err := minerController.enforceMinerSafety(
		context.Background(),
		&state,
		info,
		gammaASIC(),
		optimizerSettings(),
		now,
	); err != nil {
		t.Fatalf("enforce pending containment: %v", err)
	}
	if state.PendingKind != lib.MutationSafetyRollback ||
		state.PendingPoint() != minimum ||
		state.Phase != lib.PhaseOverheat ||
		state.CurrentPoint() == minimum {
		t.Fatalf("pre-proof readback cleared or completed containment: %+v", state)
	}
}

func TestRepeatedSafetyObservationPreservesPendingAndEpisodeAge(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	state := optimizerState(now.Add(-time.Hour))
	state.SetCurrentPoint(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1100})
	states := newMemoryOptimizerStore()
	states.putState(state)
	minerController := testController(&fakeDeviceAPI{}, states, nil)
	info := healthyInfo()
	info.Frequency = 490
	info.CoreVoltage = 1100
	info.Temp = optimizerSettings().TempCutoff

	if _, err := minerController.enforceMinerSafety(
		context.Background(),
		&state,
		info,
		gammaASIC(),
		optimizerSettings(),
		now,
	); err != nil {
		t.Fatalf("first emergency observation: %v", err)
	}
	pendingSince := state.PendingSince
	phaseStarted := state.PhaseStartedAt
	cooldownUntil := state.CooldownUntil
	if _, err := minerController.enforceMinerSafety(
		context.Background(),
		&state,
		info,
		gammaASIC(),
		optimizerSettings(),
		now.Add(time.Minute),
	); err != nil {
		t.Fatalf("repeated emergency observation: %v", err)
	}
	if !state.PendingSince.Equal(pendingSince) ||
		!state.PhaseStartedAt.Equal(phaseStarted) ||
		!state.CooldownUntil.Equal(cooldownUntil) ||
		state.OverheatCount != 1 {
		t.Fatalf("repeated observation refreshed emergency state: %+v", state)
	}

	rollbackState := optimizerState(now.Add(-time.Hour))
	rollbackState.SetCurrentPoint(
		lib.OperatingPoint{Frequency: 490, CoreVoltage: 1100},
	)
	rollbackStates := newMemoryOptimizerStore()
	rollbackStates.putState(rollbackState)
	rollbackController := testController(
		&fakeDeviceAPI{},
		rollbackStates,
		nil,
	)
	info.Temp = optimizerSettings().TempLimit + 0.1
	if _, err := rollbackController.enforceMinerSafety(
		context.Background(),
		&rollbackState,
		info,
		gammaASIC(),
		optimizerSettings(),
		now,
	); err != nil {
		t.Fatalf("first rollback observation: %v", err)
	}
	rollbackPendingSince := rollbackState.PendingSince
	rollbackPhaseStarted := rollbackState.PhaseStartedAt
	if _, err := rollbackController.enforceMinerSafety(
		context.Background(),
		&rollbackState,
		info,
		gammaASIC(),
		optimizerSettings(),
		now.Add(time.Minute),
	); err != nil {
		t.Fatalf("repeated rollback observation: %v", err)
	}
	if !rollbackState.PendingSince.Equal(rollbackPendingSince) ||
		!rollbackState.PhaseStartedAt.Equal(rollbackPhaseStarted) {
		t.Fatalf("repeated rollback refreshed durable age: %+v", rollbackState)
	}
}

func TestRollbackSelectionRequiresCompleteStrictlyDeescalatingEvidence(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	live := lib.OperatingPoint{Frequency: 600, CoreVoltage: 1150}
	state.SetCurrentPoint(live)
	states := newMemoryOptimizerStore()
	states.putState(state)
	record := func(
		point lib.OperatingPoint,
		hash float64,
	) lib.OperatingPointRecord {
		return lib.OperatingPointRecord{
			MacAddr:     state.MacAddr,
			Frequency:   point.Frequency,
			CoreVoltage: point.CoreVoltage,
			Status:      lib.PointValidated,
			MedianHash:  hash,
			P95Temp:     62,
			P95VRTemp:   50,
			P95Power:    20,
		}
	}
	for _, candidate := range []lib.OperatingPointRecord{
		record(lib.OperatingPoint{Frequency: 550, CoreVoltage: 1100}, 800),
		record(lib.OperatingPoint{Frequency: 525, CoreVoltage: 1060}, 900),
		record(lib.OperatingPoint{Frequency: 625, CoreVoltage: 1000}, 2000),
		record(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1200}, 1900),
		record(lib.OperatingPoint{Frequency: 575, CoreVoltage: 1100}, 1800),
	} {
		states.putRecord(candidate)
	}
	missingVR := record(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1100}, 1700)
	missingVR.P95VRTemp = 0
	states.putRecord(missingVR)
	missingPower := record(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1060}, 1600)
	missingPower.P95Power = 0
	states.putRecord(missingPower)
	missingTemp := record(lib.OperatingPoint{Frequency: 400, CoreVoltage: 1100}, 1500)
	missingTemp.P95Temp = 0
	states.putRecord(missingTemp)

	minerController := testController(&fakeDeviceAPI{}, states, nil)
	got, err := minerController.bestRollbackPoint(
		&state,
		live,
		gammaASIC(),
		optimizerSettings(),
	)
	if err != nil {
		t.Fatalf("select rollback: %v", err)
	}
	want := lib.OperatingPoint{Frequency: 525, CoreVoltage: 1060}
	if got != want {
		t.Fatalf("rollback target = %+v, want %+v", got, want)
	}
}

func TestOverheatRecoveryIgnoresEmergencySentinel(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{}
	minerController := testController(devices, states, nil)
	info := healthyInfo()
	info.Frequency = 50
	info.CoreVoltage = 1000
	info.HashRate = 0
	info.ExpectedHashRate = 0
	info.Temp = 60
	info.VRTemp = 49
	info.OverHeatMode = 1

	handled, err := minerController.enforceMinerSafety(
		context.Background(),
		&state,
		info,
		gammaASIC(),
		optimizerSettings(),
		now,
	)
	if err != nil || !handled {
		t.Fatalf("enforceMinerSafety = %v, %v", handled, err)
	}
	if state.PendingKind != lib.MutationOverheatRecovery ||
		state.PendingPoint() != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}) {
		t.Fatalf("overheat recovery intent = %+v", state)
	}
	saved := states.getState(state.MacAddr)
	if saved.CurrentPoint() == (lib.OperatingPoint{Frequency: 50, CoreVoltage: 1000}) {
		t.Fatalf("emergency sentinel entered optimizer state: %+v", saved)
	}
}

func TestOverheatRecoveryWaitsUntilCool(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{}
	minerController := testController(devices, states, nil)
	info := healthyInfo()
	info.OverHeatMode = 1
	info.Temp = 65

	handled, err := minerController.enforceMinerSafety(
		context.Background(),
		&state,
		info,
		gammaASIC(),
		optimizerSettings(),
		now,
	)
	if err != nil || !handled {
		t.Fatalf("enforceMinerSafety = %v, %v", handled, err)
	}
	if len(devices.operatingRequests()) != 0 {
		t.Fatal("optimizer touched the device while recording recovery")
	}
	if state.PendingKind != lib.MutationOverheatRecovery {
		t.Fatal("hot recovery was not durably queued for later safe actuation")
	}
}

func TestManualPointIsAdoptedAfterTwoPolls(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	states := newMemoryOptimizerStore()
	states.putState(state)
	minerController := testController(&fakeDeviceAPI{}, states, nil)
	manual := lib.OperatingPoint{Frequency: 490, CoreVoltage: 1100}

	if err := minerController.observeExternalPoint(
		&state,
		manual,
		optimizerSettings(),
		now,
	); err != nil {
		t.Fatalf("first observation: %v", err)
	}
	if got := states.getState(state.MacAddr).CurrentPoint(); got == manual {
		t.Fatal("manual point was adopted after only one poll")
	}
	state = states.getState(state.MacAddr)
	if err := minerController.observeExternalPoint(
		&state,
		manual,
		optimizerSettings(),
		now.Add(10*time.Second),
	); err != nil {
		t.Fatalf("second observation: %v", err)
	}
	if got := states.getState(state.MacAddr).CurrentPoint(); got != manual {
		t.Fatalf("current point = %+v, want adopted manual point", got)
	}
}

func TestQualityUsesActualHashAndOptionalErrors(t *testing.T) {
	summary := healthySummary(720, 10_000)
	settings := optimizerSettings()
	if !qualityHealthy(summary, settings) {
		t.Fatal("positive actual hash should not be rejected by expected hash")
	}
	summary.ExpectedHash = 0
	summary.Attainment = 0
	if !qualityHealthy(summary, settings) {
		t.Fatal("missing expected hash should remain diagnostic")
	}
	errorValue := 6.0
	summary.ErrorPercent = &errorValue
	if qualityHealthy(summary, settings) {
		t.Fatal("ASIC error percentage above the limit unexpectedly passed")
	}
}

func TestMissingExpectedHashStillExploresByActualHash(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{}
	minerController := testController(devices, states, nil)
	summary := healthySummary(800, 0)
	summary.Attainment = 0

	if err := minerController.evaluateWindow(
		context.Background(),
		&state,
		summary,
		gammaASIC(),
		optimizerSettings(),
		now,
	); err != nil {
		t.Fatalf("evaluateWindow returned an error: %v", err)
	}
	if state.PendingKind != lib.MutationOperatingPoint ||
		state.PendingPoint() != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}) {
		t.Fatalf("intent = %+v, want actual-hash exploration", state)
	}
}

func TestAutomatedRequestRejectsOffGridOperatingPoint(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	states := newMemoryOptimizerStore()
	devices := &fakeDeviceAPI{}
	minerController := testController(devices, states, nil)

	err := minerController.requestOperatingPoint(
		context.Background(),
		&state,
		lib.OperatingPoint{Frequency: 425, CoreVoltage: 1090},
		lib.PhaseFrequencyTest,
		lib.MutationOperatingPoint,
		now,
	)
	if err == nil || !strings.Contains(err.Error(), "not advertised") {
		t.Fatalf("error = %v, want advertised-grid rejection", err)
	}
	if len(devices.operatingRequests()) != 0 {
		t.Fatal("off-grid operating point reached the device")
	}
}

func TestPollMinerContainsDependencyPanic(t *testing.T) {
	var logs bytes.Buffer
	devices := &fakeDeviceAPI{
		getInfo: func(context.Context, string) (lib.Info, error) {
			panic("unexpected dependency panic")
		},
		asicSettings: gammaASIC(),
	}
	minerController := testController(devices, newMemoryOptimizerStore(), &logs)

	info := healthyInfo()
	result := minerController.pollMinerSafely(
		context.Background(),
		discovered(info, "192.0.2.10"),
		time.Now(),
	)
	if result != (minerPollResult{}) {
		t.Fatalf("result = %+v, want empty after panic", result)
	}
	if !strings.Contains(logs.String(), "Recovered panic") {
		t.Fatalf("panic was not recorded: %s", logs.String())
	}
	if strings.Contains(logs.String(), "unexpected dependency panic") {
		t.Fatalf("panic value escaped into logs: %s", logs.String())
	}
}

func TestPollMinersReportsOperatingPointsAndAggregateHash(t *testing.T) {
	devices := &fakeDeviceAPI{
		getInfo: func(_ context.Context, ip string) (lib.Info, error) {
			info := healthyInfo()
			switch ip {
			case "192.0.2.10":
				info.Hostname = "bitaxe-alpha"
				info.MacAddr = "aa:bb:cc:dd:ee:10"
				info.HashRate = 800
				info.ExpectedHashRate = 816
			case "192.0.2.11":
				info.Hostname = "bitaxe-beta"
				info.MacAddr = "aa:bb:cc:dd:ee:11"
				info.Frequency = 600
				info.CoreVoltage = 1100
				info.CoreVoltageActual = 1080
				info.HashRate = 1175
				info.ExpectedHashRate = 1224
			default:
				return lib.Info{}, errors.New("unexpected IP")
			}
			info.StratumUser = "synthetic-user-must-not-appear"
			info.FallbackStratumUser = "synthetic-fallback-user-must-not-appear"
			info.UpTimeSeconds = 1
			return info, nil
		},
		asicSettings: gammaASIC(),
	}
	var output bytes.Buffer
	minerController := testController(devices, newMemoryOptimizerStore(), nil)
	minerController.output = &output

	minerController.pollMiners(
		context.Background(),
		[]lib.DiscoveredMiner{
			discovered(lib.Info{MacAddr: "aa:bb:cc:dd:ee:10"}, "192.0.2.10"),
			discovered(lib.Info{MacAddr: "aa:bb:cc:dd:ee:11"}, "192.0.2.11"),
		},
		time.Now(),
	)

	got := output.String()
	plain := strings.NewReplacer(
		colorReset, "",
		colorRed, "",
		colorGreen, "",
		colorYellow, "",
	).Replace(got)
	for _, expected := range []string{
		"Hostname      Freq  VCore",
		"bitaxe-alpha  400   1100/1085",
		"800/816 98%",
		"bitaxe-beta   600   1100/1080",
		"1175/1224 96%",
		"100%/8450",
		"Total: 1.98 Th/s",
	} {
		if !strings.Contains(plain, expected) {
			t.Fatalf("output does not contain %q:\n%s", expected, got)
		}
	}
	lines := strings.Split(plain, "\n")
	if len(lines) < 3 {
		t.Fatalf("terminal output has fewer than three lines:\n%s", got)
	}
	for _, check := range []struct {
		header string
		first  string
		second string
	}{
		{header: "Freq", first: "400", second: "600"},
		{header: "VCore", first: "1100/1085", second: "1100/1080"},
		{header: "State", first: "BASELINE", second: "BASELINE"},
		{header: "Window", first: "ramp 2s", second: "ramp 2s"},
		{header: "Temp", first: "62", second: "62"},
		{header: "VRTemp", first: "49", second: "49"},
		{header: "HRate/Expected", first: "800/816 98%", second: "1175/1224 96%"},
		{header: "Watts", first: "15", second: "15"},
		{header: "Fan", first: "100%/8450", second: "100%/8450"},
	} {
		want := strings.Index(lines[0], check.header)
		first := strings.Index(lines[1], check.first)
		second := strings.Index(lines[2], check.second)
		if want < 0 || first != want || second != want {
			t.Fatalf(
				"%s column starts at header/rows %d/%d/%d:\n%s",
				check.header,
				want,
				first,
				second,
				plain,
			)
		}
	}
	if strings.Contains(got, "synthetic-user") ||
		strings.Contains(got, "synthetic-fallback-user") {
		t.Fatalf("terminal output exposed a pool user:\n%s", got)
	}
}

func TestPollMinersRendersTableAfterPollLogs(t *testing.T) {
	info := healthyInfo()
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return info, nil },
		asicSettings: gammaASIC(),
	}
	var output bytes.Buffer
	minerController := testController(devices, newMemoryOptimizerStore(), &output)
	minerController.output = &output

	minerController.pollMiners(
		context.Background(),
		[]lib.DiscoveredMiner{discovered(info, "192.0.2.10")},
		time.Now(),
	)

	got := output.String()
	logPosition := strings.Index(got, "Bootstrapping bitaxe-alpha")
	headerPosition := strings.Index(got, "Hostname")
	rowPosition := strings.Index(got, "\nbitaxe-alpha")
	if rowPosition >= 0 {
		rowPosition++
	}
	if logPosition < 0 ||
		headerPosition <= logPosition ||
		rowPosition <= headerPosition {
		t.Fatalf(
			"log/header/row positions = %d/%d/%d:\n%s",
			logPosition,
			headerPosition,
			rowPosition,
			got,
		)
	}
}

func TestPollMinersUsesBoundedWorkerPool(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	devices := &fakeDeviceAPI{
		getInfo: func(_ context.Context, ip string) (lib.Info, error) {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				observed := maximum.Load()
				if current <= observed || maximum.CompareAndSwap(observed, current) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			info := healthyInfo()
			info.Hostname = "miner-" + ip
			var host int
			_, _ = fmt.Sscanf(ip, "192.0.2.%d", &host)
			info.MacAddr = fmt.Sprintf("02:00:00:00:00:%02d", host)
			info.UpTimeSeconds = 1
			return info, nil
		},
		asicSettings: gammaASIC(),
	}
	minerController := testController(devices, newMemoryOptimizerStore(), nil)
	miners := make([]lib.DiscoveredMiner, 40)
	for index := range miners {
		ip := fmt.Sprintf("192.0.2.%d", index+1)
		macAddr := fmt.Sprintf("02:00:00:00:00:%02d", index+1)
		miners[index] = discovered(lib.Info{MacAddr: macAddr}, ip)
	}

	minerController.pollMiners(context.Background(), miners, time.Now())

	if got := maximum.Load(); got > pollWorkerLimit {
		t.Fatalf("maximum concurrent polls = %d, worker limit = %d", got, pollWorkerLimit)
	}
	if got := maximum.Load(); got < 2 {
		t.Fatalf("maximum concurrent polls = %d, want bounded concurrency evidence", got)
	}
}

func TestOperatingPointRequestReturnsPersistenceFailureWithoutTouchingDevice(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	states := newMemoryOptimizerStore()
	states.saveErr = errors.New("disk full")
	devices := &fakeDeviceAPI{}
	minerController := testController(devices, states, nil)

	err := minerController.requestOperatingPoint(
		context.Background(),
		&state,
		lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060},
		lib.PhaseUndervolt,
		lib.MutationOperatingPoint,
		now,
	)
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("error = %v, want persistence failure", err)
	}
	if len(devices.operatingRequests()) != 0 {
		t.Fatal("device was changed before request persistence succeeded")
	}
}

func TestRepeatedOverheatsExtendAndCapCooldown(t *testing.T) {
	settings := optimizerSettings()
	if got := overheatCooldown(settings, 1); got != 2*time.Hour {
		t.Fatalf("first cooldown = %s, want 2h", got)
	}
	if got := overheatCooldown(settings, 3); got != 6*time.Hour {
		t.Fatalf("third cooldown = %s, want 6h", got)
	}
	if got := overheatCooldown(settings, 1_000_000); got != 24*time.Hour {
		t.Fatalf("capped cooldown = %s, want 24h", got)
	}
}
