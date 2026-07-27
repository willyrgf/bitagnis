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

type fakeDeviceAPI struct {
	mu sync.Mutex

	getInfo      func(context.Context, string) (lib.Info, error)
	asicSettings lib.ASICSettings
	asicError    error
	setError     error
	recoverError error
	requests     []operatingRequest
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
	return fake.asicSettings, fake.asicError
}

func (fake *fakeDeviceAPI) SetOperatingPoint(
	_ context.Context,
	point lib.OperatingPoint,
	ip string,
) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.requests = append(fake.requests, operatingRequest{point: point, ip: ip})
	return fake.setError
}

func (fake *fakeDeviceAPI) RecoverOperatingPoint(
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
	return fake.recoverError
}

func (fake *fakeDeviceAPI) operatingRequests() []operatingRequest {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]operatingRequest(nil), fake.requests...)
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
		state.OverheatPending = true
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
		Hostname:          "mineira",
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

func optimizerState(now time.Time) lib.MinerState {
	state := lib.MinerState{
		MacAddr:        "aa:bb:cc:dd:ee:ff",
		Hostname:       "mineira",
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

	if _, err := minerController.pollMiner(context.Background(), "192.0.2.10", now); err != nil {
		t.Fatalf("bootstrap poll: %v", err)
	}
	if len(devices.operatingRequests()) != 0 {
		t.Fatal("bootstrap changed settings immediately")
	}

	for sample := 0; sample < 6; sample++ {
		at := now.Add(2*time.Second + time.Duration(sample)*10*time.Second)
		if _, err := minerController.pollMiner(context.Background(), "192.0.2.10", at); err != nil {
			t.Fatalf("sample %d: %v", sample, err)
		}
	}
	requests := devices.operatingRequests()
	if len(requests) != 1 ||
		requests[0].point != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}) ||
		requests[0].recovery {
		t.Fatalf("requests = %+v, want first undervolt 400/1060", requests)
	}
}

func TestPendingPairMustBeConfirmedBeforeEvaluation(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	state.SetPendingPoint(lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060})
	state.PendingSince = now
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
	if got.PendingFrequency != 0 ||
		got.CurrentPoint() != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}) ||
		!got.RampUntil.After(now) {
		t.Fatalf("confirmed state = %+v", got)
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
	requests := devices.operatingRequests()
	if len(requests) != 1 ||
		requests[0].point != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1100}) {
		t.Fatalf("rollback requests = %+v", requests)
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
	requests := devices.operatingRequests()
	if len(requests) != 1 ||
		requests[0].point != (lib.OperatingPoint{Frequency: 490, CoreVoltage: 1100}) {
		t.Fatalf("voltage trial requests = %+v", requests)
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
	requests := devices.operatingRequests()
	if len(requests) != 1 ||
		requests[0].point != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}) {
		t.Fatalf("rollback requests = %+v", requests)
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
	requests := devices.operatingRequests()
	if len(requests) != 1 ||
		requests[0].point != (lib.OperatingPoint{Frequency: 490, CoreVoltage: 1100}) {
		t.Fatalf("next voltage request = %+v", requests)
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
	requests := devices.operatingRequests()
	if len(requests) != 1 ||
		requests[0].point != (lib.OperatingPoint{Frequency: 490, CoreVoltage: 1000}) {
		t.Fatalf("frequency sweep did not start at minimum voltage: %+v", requests)
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
	requests := devices.operatingRequests()
	if len(requests) != 1 ||
		requests[0].point != (lib.OperatingPoint{Frequency: 490, CoreVoltage: 1060}) {
		t.Fatalf("thermal rollback requests = %+v", requests)
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
	requests := devices.operatingRequests()
	if len(requests) != 1 ||
		requests[0].point != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}) {
		t.Fatalf("safety requests = %+v", requests)
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
	requests := devices.operatingRequests()
	if len(requests) != 1 ||
		requests[0].point != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}) {
		t.Fatalf("minimum safety requests = %+v", requests)
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
	info.Frequency = 75
	info.CoreVoltage = 4870
	info.HashRate = 0
	info.ExpectedHashRate = 0
	info.Temp = 60
	info.VRTemp = 49
	info.OverHeatMode = 1

	if err := minerController.handleOverheat(
		context.Background(),
		&state,
		info,
		gammaASIC(),
		optimizerSettings(),
		now,
	); err != nil {
		t.Fatalf("handleOverheat returned an error: %v", err)
	}
	requests := devices.operatingRequests()
	if len(requests) != 1 ||
		!requests[0].recovery ||
		requests[0].point != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}) {
		t.Fatalf("overheat recovery = %+v", requests)
	}
	saved := states.getState(state.MacAddr)
	if saved.PendingPoint().CoreVoltage == 4870 {
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

	if err := minerController.handleOverheat(
		context.Background(),
		&state,
		info,
		gammaASIC(),
		optimizerSettings(),
		now,
	); err != nil {
		t.Fatalf("handleOverheat returned an error: %v", err)
	}
	if len(devices.operatingRequests()) != 0 {
		t.Fatal("recovery was requested while the ASIC was hot")
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
	requests := devices.operatingRequests()
	if len(requests) != 1 ||
		requests[0].point != (lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}) {
		t.Fatalf("requests = %+v, want actual-hash exploration", requests)
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
		false,
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

	result := minerController.pollMinerSafely(context.Background(), "192.0.2.10", time.Now())
	if result != (minerPollResult{}) {
		t.Fatalf("result = %+v, want empty after panic", result)
	}
	if !strings.Contains(logs.String(), "Recovered panic") ||
		!strings.Contains(logs.String(), "unexpected dependency panic") {
		t.Fatalf("panic was not recorded: %s", logs.String())
	}
}

func TestPollMinersReportsOperatingPointsAndAggregateHash(t *testing.T) {
	devices := &fakeDeviceAPI{
		getInfo: func(_ context.Context, ip string) (lib.Info, error) {
			info := healthyInfo()
			switch ip {
			case "192.0.2.10":
				info.Hostname = "mineira"
				info.MacAddr = "aa:bb:cc:dd:ee:10"
				info.HashRate = 800
				info.ExpectedHashRate = 816
			case "192.0.2.11":
				info.Hostname = "mineiro"
				info.MacAddr = "aa:bb:cc:dd:ee:11"
				info.Frequency = 600
				info.CoreVoltage = 1100
				info.CoreVoltageActual = 1080
				info.HashRate = 1175
				info.ExpectedHashRate = 1224
			default:
				return lib.Info{}, errors.New("unexpected IP")
			}
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
		[]string{"192.0.2.10", "192.0.2.11"},
		time.Now(),
	)

	got := output.String()
	for _, expected := range []string{
		"Hostname\tFreq\tVCore\tState\tWindow",
		"mineira\t400\t1100/1085",
		"800/816 98%",
		"mineiro\t600\t1100/1080",
		"1175/1224 96%",
		"100%/8450",
		"Total: 1.98 Th/s",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("output does not contain %q:\n%s", expected, got)
		}
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
			info.MacAddr = "mac-" + ip
			info.UpTimeSeconds = 1
			return info, nil
		},
		asicSettings: gammaASIC(),
	}
	minerController := testController(devices, newMemoryOptimizerStore(), nil)
	ips := make([]string, 40)
	for index := range ips {
		ips[index] = fmt.Sprintf("192.0.2.%d", index+1)
	}

	minerController.pollMiners(context.Background(), ips, time.Now())

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
		false,
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
