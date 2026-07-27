package lib

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func openTestOptimizerStore(t *testing.T) *OptimizerStore {
	t.Helper()
	store, err := OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatalf("open test optimizer store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close test optimizer store: %v", err)
		}
	})
	return store
}

func normalInfo() Info {
	return Info{
		Hostname:    "mineira",
		MacAddr:     "aa:bb:cc:dd:ee:ff",
		Frequency:   400,
		CoreVoltage: 1100,
		HashRate:    800,
		Temp:        62,
	}
}

func TestOptimizerStoreBootstrapsFromLiveOperatingPoint(t *testing.T) {
	store := openTestOptimizerStore(t)
	now := time.Now().Round(time.Second)
	state, created, err := store.LoadOrCreate(normalInfo(), "192.0.2.10", now)
	if err != nil {
		t.Fatalf("LoadOrCreate returned an error: %v", err)
	}
	if !created {
		t.Fatal("new miner was not reported as created")
	}
	if state.CurrentPoint() != (OperatingPoint{Frequency: 400, CoreVoltage: 1100}) {
		t.Fatalf("current point = %+v", state.CurrentPoint())
	}
	if state.Phase != PhaseBaseline || !state.PhaseStartedAt.Equal(now) {
		t.Fatalf("initial optimizer state = %+v", state)
	}

	reloaded, created, err := store.LoadOrCreate(normalInfo(), "192.0.2.11", now.Add(time.Minute))
	if err != nil {
		t.Fatalf("reload state: %v", err)
	}
	if created || reloaded.IP != "192.0.2.11" {
		t.Fatalf("reloaded state = %+v, created = %v", reloaded, created)
	}
}

func TestOptimizerStoreNeverTrustsEmergencyOperatingPoint(t *testing.T) {
	store := openTestOptimizerStore(t)
	info := normalInfo()
	info.Frequency = 75
	info.CoreVoltage = 4870
	info.OverHeatMode = 1

	state, created, err := store.LoadOrCreate(info, "192.0.2.10", time.Now())
	if err != nil {
		t.Fatalf("LoadOrCreate returned an error: %v", err)
	}
	if !created || state.CurrentPoint() != (OperatingPoint{}) {
		t.Fatalf("emergency point was trusted: %+v", state)
	}
	if !state.OverheatPending || state.Phase != PhaseOverheat || state.OverheatCount != 1 {
		t.Fatalf("overheat bootstrap state = %+v", state)
	}
}

func TestOptimizerStoreCreatesSchema(t *testing.T) {
	store := openTestOptimizerStore(t)
	if !store.db.Migrator().HasTable("optimizer_miners") ||
		!store.db.Migrator().HasTable("operating_points") {
		t.Fatal("optimizer schema is incomplete")
	}
}

func TestOptimizerStorePersistsEvaluatedPoints(t *testing.T) {
	store := openTestOptimizerStore(t)
	record := OperatingPointRecord{
		MacAddr:      "aa:bb",
		Frequency:    400,
		CoreVoltage:  1060,
		Status:       PointValidated,
		MedianHash:   800,
		ExpectedHash: 816,
		Attainment:   800.0 / 816,
		MeanTemp:     60.5,
		P95Temp:      62,
		P95VRTemp:    49,
		P95Power:     14.8,
		MeasuredAt:   time.Now(),
	}
	if err := store.SavePoint(&record); err != nil {
		t.Fatalf("SavePoint returned an error: %v", err)
	}
	records, err := store.ListPoints(record.MacAddr)
	if err != nil {
		t.Fatalf("ListPoints returned an error: %v", err)
	}
	if len(records) != 1 || records[0].Point() != record.Point() ||
		records[0].Status != PointValidated ||
		records[0].MeanTemp != 60.5 {
		t.Fatalf("records = %+v", records)
	}
}

func TestOptimizerStorePersistsPendingPairAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	now := time.Now().Round(time.Second)
	state, _, err := store.LoadOrCreate(normalInfo(), "192.0.2.10", now)
	if err != nil {
		t.Fatalf("create state: %v", err)
	}
	state.SetPendingPoint(OperatingPoint{Frequency: 400, CoreVoltage: 1060})
	state.PendingSince = now
	state.Phase = PhaseUndervolt
	if err := store.SaveMiner(&state); err != nil {
		t.Fatalf("save pending state: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got, created, err := reopened.LoadOrCreate(normalInfo(), "192.0.2.10", now)
	if err != nil {
		t.Fatalf("load reopened state: %v", err)
	}
	if created || got.PendingPoint() != (OperatingPoint{Frequency: 400, CoreVoltage: 1060}) {
		t.Fatalf("reopened state = %+v", got)
	}
}

func TestOptimizerStoreSerializesConcurrentWrites(t *testing.T) {
	store := openTestOptimizerStore(t)
	var workers sync.WaitGroup
	errorsChannel := make(chan error, 32)
	for index := range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			state := MinerState{
				MacAddr:            fmt.Sprintf("aa:bb:cc:dd:ee:%02d", index),
				Hostname:           fmt.Sprintf("miner-%02d", index),
				IP:                 fmt.Sprintf("192.0.2.%d", index+1),
				Phase:              PhaseBaseline,
				CurrentFrequency:   400,
				CurrentCoreVoltage: 1100,
			}
			if err := store.SaveMiner(&state); err != nil {
				errorsChannel <- err
			}
		}()
	}
	workers.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		t.Errorf("concurrent SaveMiner returned an error: %v", err)
	}
}

func TestOptimizerStoreRejectsInvalidStateAndPoint(t *testing.T) {
	store := openTestOptimizerStore(t)
	err := store.SaveMiner(&MinerState{
		MacAddr:            "aa:bb",
		Hostname:           "mineira",
		IP:                 "192.0.2.10",
		Phase:              PhaseBaseline,
		CurrentFrequency:   400,
		CurrentCoreVoltage: 4870,
	})
	if err == nil {
		t.Fatal("SaveMiner returned nil, want invalid voltage error")
	}

	err = store.SavePoint(&OperatingPointRecord{
		MacAddr:     "aa:bb",
		Frequency:   400,
		CoreVoltage: 4870,
		Status:      PointUnstable,
	})
	if err == nil {
		t.Fatal("SavePoint returned nil, want invalid voltage error")
	}
}
