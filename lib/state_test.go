package lib

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
		Hostname:    "bitaxe-alpha",
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

func TestOptimizerStoreCreatesOnlyCurrentSchema(t *testing.T) {
	store := openTestOptimizerStore(t)
	version, err := schemaVersion(t.Context(), store.conn)
	if err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	tables, err := applicationTables(t.Context(), store.conn)
	if err != nil {
		t.Fatalf("read schema tables: %v", err)
	}
	if version != optimizerSchemaVersion ||
		fmt.Sprint(tables) != "[operating_points optimizer_miners]" {
		t.Fatalf("schema version/tables = %d/%v", version, tables)
	}
}

func TestOptimizerStoreOpensRelativeRuntimePath(t *testing.T) {
	t.Chdir(t.TempDir())
	store, err := OpenOptimizerStore("optimizer.db")
	if err != nil {
		t.Fatalf("open relative optimizer store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close relative optimizer store: %v", err)
	}
	if _, err := os.Stat("optimizer.db"); err != nil {
		t.Fatalf("stat relative optimizer database: %v", err)
	}
}

func TestOptimizerStoreExcludesSecondProcess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	first, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })

	started := time.Now()
	second, err := OpenOptimizerStore(path)
	if second != nil {
		_ = second.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "already in use") {
		t.Fatalf("second open error = %v", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatalf("second open was not bounded: %s", time.Since(started))
	}
}

func TestOptimizerStorePersistsHistoryAndPendingObligationsAcrossReopen(t *testing.T) {
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
	state.SetPendingMutation(
		MutationOverheatRecovery,
		OperatingPoint{Frequency: 400, CoreVoltage: 1060},
		now,
	)
	state.MiningPending = true
	state.OverheatCount = 3
	state.CooldownUntil = now.Add(6 * time.Hour)
	if err := store.SaveMiner(&state); err != nil {
		t.Fatalf("save pending state: %v", err)
	}
	record := OperatingPointRecord{
		MacAddr:      state.MacAddr,
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
		MeasuredAt:   now,
	}
	if err := store.SavePoint(&record); err != nil {
		t.Fatalf("save point: %v", err)
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
	if created ||
		got.PendingKind != MutationOverheatRecovery ||
		got.PendingPoint() != (OperatingPoint{Frequency: 400, CoreVoltage: 1060}) ||
		!got.MiningPending ||
		got.OverheatCount != 3 ||
		!got.CooldownUntil.Equal(state.CooldownUntil) {
		t.Fatalf("reopened state = %+v", got)
	}
	records, err := reopened.ListPoints(state.MacAddr)
	if err != nil {
		t.Fatalf("list reopened points: %v", err)
	}
	if len(records) != 1 || records[0].Point() != record.Point() {
		t.Fatalf("reopened records = %+v", records)
	}
}

func TestOptimizerStoreRejectsIncompatibleSchemaWithoutModification(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{
			name:   "old unversioned",
			schema: `CREATE TABLE optimizer_miners (mac_addr TEXT PRIMARY KEY, pending_recovery INTEGER);`,
		},
		{
			name:   "unknown version",
			schema: `PRAGMA user_version = 99; CREATE TABLE marker (value TEXT);`,
		},
		{
			name: "partial current",
			schema: `PRAGMA user_version = 1;
				CREATE TABLE optimizer_miners (mac_addr TEXT NOT NULL PRIMARY KEY);`,
		},
		{
			name: "unexpected current table",
			schema: `PRAGMA user_version = 1;
				CREATE TABLE unexpected (value TEXT);`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "optimizer.db")
			createRawDatabase(t, path, test.schema)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read database before open: %v", err)
			}

			store, err := OpenOptimizerStore(path)
			if store != nil {
				_ = store.Close()
			}
			if err == nil ||
				!strings.Contains(err.Error(), "move aside or remove") {
				t.Fatalf("open error = %v", err)
			}
			after, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatalf("read database after rejection: %v", readErr)
			}
			if string(after) != string(before) {
				t.Fatal("incompatible database bytes were modified")
			}
		})
	}
}

func TestOptimizerStoreRejectsInvalidCurrentDataBeforeUse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, _, err := store.LoadOrCreate(
		normalInfo(),
		"192.0.2.10",
		time.Now(),
	); err != nil {
		_ = store.Close()
		t.Fatalf("create state: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	database, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open raw database: %v", err)
	}
	if _, err := database.Exec(
		"UPDATE optimizer_miners SET pending_kind = 'unknown'",
	); err != nil {
		_ = database.Close()
		t.Fatalf("corrupt current data: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close raw database: %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read database before validation: %v", err)
	}
	reopened, err := OpenOptimizerStore(path)
	if reopened != nil {
		_ = reopened.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "pending mutation kind") {
		t.Fatalf("reopen error = %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read database after validation: %v", err)
	}
	if string(after) != string(before) {
		t.Fatal("invalid current database was modified during rejection")
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
				MacAddr:            fmt.Sprintf("aa:bb:cc:dd:ee:%02x", index),
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

func TestOptimizerStoreRejectsInvalidPendingStateAndPoint(t *testing.T) {
	store := openTestOptimizerStore(t)
	state := MinerState{
		MacAddr:            "aa:bb:cc:dd:ee:ff",
		Hostname:           "bitaxe-alpha",
		IP:                 "192.0.2.10",
		Phase:              PhaseBaseline,
		CurrentFrequency:   400,
		CurrentCoreVoltage: 1100,
		PendingKind:        MutationOperatingPoint,
	}
	if err := store.SaveMiner(&state); err == nil {
		t.Fatal("SaveMiner returned nil for incomplete pending mutation")
	}

	err := store.SavePoint(&OperatingPointRecord{
		MacAddr:     "aa:bb",
		Frequency:   400,
		CoreVoltage: 4870,
		Status:      PointUnstable,
	})
	if err == nil {
		t.Fatal("SavePoint returned nil, want invalid voltage error")
	}
}

func createRawDatabase(t *testing.T, path string, schema string) {
	t.Helper()
	database, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open raw database: %v", err)
	}
	if _, err := database.Exec(schema); err != nil {
		_ = database.Close()
		t.Fatalf("create raw schema: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close raw database: %v", err)
	}
}
