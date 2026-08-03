package lib

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	testMAC      = "aa:bb:cc:dd:ee:01"
	testHostname = "bitaxe-test"
	testIP       = "192.0.2.11"
)

func testInfo() Info {
	return Info{
		Version: "v2.8.1", ASICModel: "BM1370", BoardVersion: "601",
		Hostname: testHostname, MacAddr: testMAC, Frequency: 525, CoreVoltage: 1150,
		CoreVoltageActual: 1150, HashRate: 100, ExpectedHashRate: 100,
		Temp: 55, VRTemp: 70, Power: 18, UpTimeSeconds: 100,
	}
}

func testASIC() ASICSettings {
	return ASICSettings{
		ASICModel: "BM1370", DefaultFrequency: 525, DefaultVoltage: 1150,
		FrequencyOptions: []int{400, 490, 525, 550, 600, 625},
		VoltageOptions:   []int{1000, 1060, 1100, 1150, 1200, 1250},
	}
}

func TestOpenOptimizerStoreReadOnlyDoesNotCreateOrWrite(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.db")
	if _, err := OpenOptimizerStoreReadOnly(missing); err == nil {
		t.Fatal("read-only open created a missing database")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("read-only open changed missing database state: %v", err)
	}

	path := filepath.Join(t.TempDir(), "optimizer.db")
	writable, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writable.Close(); err != nil {
		t.Fatal(err)
	}
	readOnly, err := OpenOptimizerStoreReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	if _, err := readOnly.conn.ExecContext(context.Background(), "CREATE TABLE report_write_probe (value INTEGER)"); err == nil {
		t.Fatal("read-only optimizer store accepted a write")
	}

	invalidPath := filepath.Join(t.TempDir(), "optimizer.db")
	invalid, err := OpenOptimizerStore(invalidPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := invalid.conn.ExecContext(context.Background(), "CREATE VIEW report_schema_probe AS SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if err := invalid.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenOptimizerStoreReadOnly(invalidPath); err == nil {
		t.Fatal("read-only optimizer store accepted an unexpected schema object")
	}
}

func openTestStore(t *testing.T) *OptimizerStore {
	t.Helper()
	store, err := OpenOptimizerStore(filepath.Join(t.TempDir(), "optimizer.db"))
	if err != nil {
		t.Fatalf("open optimizer store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func bootstrapTestMiner(t *testing.T, store *OptimizerStore) (MinerState, time.Time) {
	t.Helper()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	state, created, err := store.BootstrapMiner(testInfo(), testIP, now, time.Minute, 5*time.Minute, true)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if !created {
		t.Fatal("bootstrap did not create miner")
	}
	return state, now
}

func TestSchemaV5BootstrapAndReopen(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	if state.PassTrigger != PassInitial || state.PassStartedAt != now || state.AccountedThroughAt != now {
		t.Fatalf("unexpected bootstrap authority: %+v", state)
	}
	points, err := store.ListPoints(testMAC)
	if err != nil {
		t.Fatalf("list bootstrap point: %v", err)
	}
	if len(points) != 1 || points[0].Status != PointEntered || points[0].EntryAttemptID != 0 {
		t.Fatalf("unexpected baseline row: %+v", points)
	}
	version, err := schemaVersion(context.Background(), store.conn)
	if err != nil || version != 5 {
		t.Fatalf("schema version = %d, %v", version, err)
	}
	path := storePath(t, store)
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	loaded, err := reopened.LoadMiner(testMAC)
	if err != nil || loaded.CurrentPoint() != state.CurrentPoint() {
		t.Fatalf("reopened state = %+v, %v", loaded, err)
	}
}

func TestBootstrapOffGridPairIsBlockedWithoutFrontierRow(t *testing.T) {
	store := openTestStore(t)
	offGrid := testInfo()
	offGrid.Frequency = 500
	offGrid.CoreVoltage = 1000
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	state, created, err := store.BootstrapMiner(offGrid, testIP, now, time.Minute, 5*time.Minute, true)
	if err != nil || !created {
		t.Fatalf("off-grid bootstrap = %+v, %t, %v", state, created, err)
	}
	if state.Phase != PhaseHold || state.HoldReason != HoldBlocked ||
		state.CurrentPoint() != (OperatingPoint{Frequency: offGrid.Frequency, CoreVoltage: offGrid.CoreVoltage}) {
		t.Fatalf("off-grid bootstrap state = %+v", state)
	}
	points, err := store.ListPoints(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 0 {
		t.Fatalf("off-grid bootstrap created frontier rows: %+v", points)
	}
}

func TestAutomationPersistenceRejectsOffGridAuthority(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	offGrid := OperatingPoint{Frequency: 500, CoreVoltage: 1000}
	if err := validatePointRecord(OperatingPointRecord{
		MacAddr: testMAC, Frequency: offGrid.Frequency, CoreVoltage: offGrid.CoreVoltage,
		Status: PointEntered, EnteredAt: now,
	}); err == nil {
		t.Fatal("off-grid point record was accepted")
	}
	for _, mutate := range []func(*MinerState){
		func(value *MinerState) { value.SetBestPoint(offGrid) },
		func(value *MinerState) { value.SetFallbackPoint(offGrid) },
		func(value *MinerState) { value.SetPendingMutation(MutationOperatingPoint, offGrid, now) },
	} {
		candidate := state
		mutate(&candidate)
		if err := store.SaveMiner(&candidate); err == nil {
			t.Fatalf("off-grid durable authority was accepted: %+v", candidate)
		}
	}
	if err := validateMinerState(func() MinerState {
		current := state
		current.SetCurrentPoint(offGrid)
		return current
	}()); err != nil {
		t.Fatalf("off-grid current observation was rejected: %v", err)
	}
}

func TestReopenRejectsOffGridOperatingPointRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatal(err)
	}
	_, now := bootstrapTestMiner(t, store)
	if _, err := store.conn.ExecContext(context.Background(), `INSERT INTO operating_points (
		mac_addr, frequency, core_voltage, status, median_hash, expected_hash,
		attainment, mean_temp, p95_temp, p95_vr_temp, p95_power, error_percent,
		accepted_delta, rejected_delta, measured_at, entered_at, entry_attempt_id, reference_hash
	) VALUES (?, 500, 1000, ?, 0, 0, 0, 0, 0, 0, 0, NULL, 0, 0, 0, ?, 0, 0)`,
		testMAC, PointEntered, timeValue(now)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenOptimizerStore(path); err == nil {
		t.Fatal("off-grid operating point record was accepted on reopen")
	}
}

func TestReopenRejectsMutationAttemptForUnknownMiner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatal(err)
	}
	_, now := bootstrapTestMiner(t, store)
	unknownMAC := "aa:bb:cc:dd:ee:99"
	if _, err := store.conn.ExecContext(context.Background(), `INSERT INTO mutation_attempts (
		mac_addr, kind, reason, from_frequency, from_core_voltage,
		target_frequency, target_core_voltage, intent_created_at, started_at,
		patch_requested_at, configured_verified_at, configured_verified_uptime_seconds,
		restart_requested_at, reboot_verified_at, completed_at, first_positive_at,
		mining_resumed_at, failed_at, failure_stage
	) VALUES (?, ?, '', 525, 1150, 0, 0, ?, ?, 0, 0, -1, 0, 0, 0, 0, 0, ?, ?)`,
		unknownMAC, MutationMiningConfiguration, timeValue(now), timeValue(now.Add(time.Second)),
		timeValue(now.Add(2*time.Second)), MutationFailurePreflight); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenOptimizerStore(path); err == nil {
		t.Fatal("mutation attempt for unknown miner was accepted on reopen")
	}
}

func TestResetOptimizationPassPersistsCompleteBoundarySnapshot(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	selected := state.CurrentPoint()
	baseline := OperatingPointRecord{
		MacAddr: testMAC, Frequency: selected.Frequency, CoreVoltage: selected.CoreVoltage,
		Status: PointValidated, MedianHash: 100, ExpectedHash: 100, Attainment: 1,
		MeanTemp: 55, P95Temp: 56, P95VRTemp: 70, P95Power: 18,
		MeasuredAt: now.Add(10 * time.Minute), EnteredAt: now,
	}
	if err := store.FinalizeBaseline(&state, baseline, false, baseline.MeasuredAt); err != nil {
		t.Fatalf("finalize baseline: %v", err)
	}
	settledAt := now.Add(20 * time.Minute)
	state.Phase = PhaseHold
	state.HoldReason = HoldOptimized
	state.SettledAt = settledAt
	state.RampUntil = now
	state.EvidenceDeadlineAt = time.Time{}
	if err := store.SaveMiner(&state); err != nil {
		t.Fatalf("save settled hold: %v", err)
	}

	passStart := now.Add(time.Hour)
	if err := store.ResetOptimizationPass(
		testMAC, selected, passStart, passStart.Add(time.Minute), passStart.Add(21*time.Minute),
	); err != nil {
		t.Fatalf("reset optimization pass: %v", err)
	}
	loaded, err := store.LoadMiner(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.PassReferenceHash != 100 ||
		loaded.PassReferenceFrequency != selected.Frequency ||
		loaded.PassReferenceCoreVoltage != selected.CoreVoltage ||
		!loaded.PassReferenceSettledAt.Equal(settledAt) {
		t.Fatalf("boundary snapshot = %+v", loaded)
	}

	path := storePath(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	reopenedState, err := reopened.LoadMiner(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	if reopenedState.PassReferenceFrequency != selected.Frequency ||
		reopenedState.PassReferenceCoreVoltage != selected.CoreVoltage ||
		!reopenedState.PassReferenceSettledAt.Equal(settledAt) {
		t.Fatalf("reopened boundary snapshot = %+v", reopenedState)
	}

	mutated := reopenedState
	mutated.PassReferenceFrequency = 400
	mutated.PassReferenceCoreVoltage = 1000
	mutated.PassReferenceSettledAt = passStart.Add(-time.Minute)
	if err := reopened.SaveMiner(&mutated); err != nil {
		t.Fatalf("ordinary save: %v", err)
	}
	preserved, err := reopened.LoadMiner(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	if preserved.PassReferenceFrequency != selected.Frequency ||
		preserved.PassReferenceCoreVoltage != selected.CoreVoltage ||
		!preserved.PassReferenceSettledAt.Equal(settledAt) {
		t.Fatalf("ordinary save replaced boundary snapshot = %+v", preserved)
	}
}

func TestManualRetuneKeepsArmSnapshotAbsentAfterBaseline(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	state.Phase = PhaseHold
	state.HoldReason = HoldManual
	state.SettledAt = now.Add(time.Hour)
	state.RampUntil = now
	state.EvidenceDeadlineAt = time.Time{}
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	passStart := now.Add(2 * time.Hour)
	if err := store.ResetOptimizationPass(
		testMAC, state.CurrentPoint(), passStart, passStart.Add(time.Minute), passStart.Add(21*time.Minute),
	); err != nil {
		t.Fatalf("manual retune: %v", err)
	}
	baseline := OperatingPointRecord{
		MacAddr: testMAC, Frequency: state.CurrentFrequency, CoreVoltage: state.CurrentCoreVoltage,
		Status: PointValidated, MedianHash: 100, ExpectedHash: 100, Attainment: 1,
		MeanTemp: 55, P95Temp: 56, P95VRTemp: 70, P95Power: 18,
		MeasuredAt: passStart.Add(10 * time.Minute), EnteredAt: passStart,
	}
	if err := store.FinalizeBaseline(&state, baseline, false, baseline.MeasuredAt); err != nil {
		t.Fatalf("manual baseline: %v", err)
	}
	if state.PassTrigger != PassOperator || state.PassReferenceHash != 0 ||
		state.PassReferenceFrequency != 0 || state.PassReferenceCoreVoltage != 0 ||
		!state.PassReferenceSettledAt.IsZero() {
		t.Fatalf("manual retune created an arm snapshot: %+v", state)
	}
}

func TestReopenRejectsInvalidPassReferenceSnapshot(t *testing.T) {
	cases := []struct {
		name   string
		update string
		args   []any
	}{
		{
			name:   "operator hash without snapshot",
			update: `UPDATE optimizer_miners SET pass_trigger = 'operator', pass_reference_hash = 100, pass_reference_frequency = 0, pass_reference_core_voltage = 0, pass_reference_settled_at = 0 WHERE mac_addr = ?`,
			args:   []any{testMAC},
		},
		{
			name:   "off-grid point",
			update: `UPDATE optimizer_miners SET pass_trigger = 'operator', pass_reference_hash = 100, pass_reference_frequency = 500, pass_reference_core_voltage = 1000, pass_reference_settled_at = pass_started_at - 1 WHERE mac_addr = ?`,
			args:   []any{testMAC},
		},
		{
			name:   "settlement after pass start",
			update: `UPDATE optimizer_miners SET pass_trigger = 'operator', pass_reference_hash = 100, pass_reference_frequency = 525, pass_reference_core_voltage = 1150, pass_reference_settled_at = pass_started_at + 1 WHERE mac_addr = ?`,
			args:   []any{testMAC},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "optimizer.db")
			store, err := OpenOptimizerStore(path)
			if err != nil {
				t.Fatal(err)
			}
			bootstrapTestMiner(t, store)
			if _, err := store.conn.ExecContext(context.Background(), testCase.update, testCase.args...); err != nil {
				_ = store.Close()
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenOptimizerStore(path); err == nil {
				t.Fatal("invalid boundary snapshot was accepted")
			}
		})
	}
}

func TestResetOptimizationPassRejectsSafetyHold(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	state.Phase = PhaseHold
	state.HoldReason = HoldSafety
	state.SafetyReason = SafetyReasonASICLimit
	state.SettledAt = now.Add(time.Hour)
	state.PhaseStartedAt = now.Add(time.Hour)
	state.EvidenceDeadlineAt = time.Time{}
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	if err := store.ResetOptimizationPass(
		testMAC, state.CurrentPoint(), now.Add(2*time.Hour),
		now.Add(3*time.Hour), now.Add(23*time.Hour),
	); err == nil {
		t.Fatal("retune was accepted from a safety-derived hold")
	}
}

func storePath(t *testing.T, store *OptimizerStore) string {
	t.Helper()
	var path string
	if err := store.conn.QueryRowContext(context.Background(), "PRAGMA database_list").Scan(new(int), new(string), &path); err != nil {
		t.Fatalf("database path: %v", err)
	}
	return path
}

func TestRejectSchemaV3WithoutMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.conn.ExecContext(context.Background(), "PRAGMA user_version = 3"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenOptimizerStore(path); err == nil {
		t.Fatal("schema v3 was accepted")
	}
}

func TestRejectSchemaV4WithoutMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.conn.ExecContext(context.Background(), "PRAGMA user_version = 4"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenOptimizerStore(path); err == nil {
		t.Fatal("schema v4 was accepted")
	}
}

func TestReopenRejectsOrphanedUnfinishedMutationAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatal(err)
	}
	_, now := bootstrapTestMiner(t, store)
	if _, err := store.conn.ExecContext(context.Background(), `INSERT INTO mutation_attempts (
		mac_addr, kind, reason, from_frequency, from_core_voltage,
		target_frequency, target_core_voltage, intent_created_at, started_at,
		patch_requested_at, configured_verified_at, configured_verified_uptime_seconds,
		restart_requested_at, reboot_verified_at, completed_at, first_positive_at,
		mining_resumed_at, failed_at, failure_stage
	) VALUES (?, ?, '', 525, 1150, 525, 1100, ?, ?, 0, 0, -1, 0, 0, 0, 0, 0, 0, '')`,
		testMAC, MutationOperatingPoint, timeValue(now), timeValue(now)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenOptimizerStore(path); err == nil {
		t.Fatal("orphaned unfinished mutation authority was accepted")
	}
}

func TestAdmitTrialBindsOneEntryAttemptAndFinalizesReturn(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	baseline := state.CurrentPoint()
	state.BestHashRate = 100
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	candidate := OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	if _, err := store.AdmitTrial(&state, candidate, baseline, PhaseUndervolt, 99, now.Add(time.Minute), now.Add(21*time.Minute)); err == nil {
		t.Fatal("stale frozen reference was accepted")
	}
	state.PassReferenceHash = 100
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	state.PassReferenceHash = 1
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadMiner(testMAC)
	if err != nil || loaded.PassReferenceHash != 100 {
		t.Fatalf("positive pass reference was overwritten: %+v, %v", loaded, err)
	}
	state = loaded
	attemptID, err := store.AdmitTrial(&state, candidate, baseline, PhaseUndervolt, 100, now.Add(time.Minute), now.Add(21*time.Minute))
	if err != nil {
		t.Fatalf("admit trial: %v", err)
	}
	if attemptID <= 0 || state.PendingPoint() != candidate || state.FallbackPoint() != baseline {
		t.Fatalf("admission state = %+v", state)
	}
	attempt, ok, err := store.UnfinishedMutationAttempt(testMAC)
	if err != nil || !ok || attempt.ID != attemptID {
		t.Fatalf("entry attempt = %+v, %t, %v", attempt, ok, err)
	}
	points, err := store.ListPoints(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	entry, found := findTestPoint(points, candidate)
	if !found || entry.EntryAttemptID != attemptID || entry.ReferenceHash != 100 {
		t.Fatalf("candidate binding = %+v", entry)
	}
	terminal := entry
	terminal.Status = PointNoGain
	terminal.MedianHash = 99
	terminal.ExpectedHash = 100
	terminal.Attainment = .99
	terminal.MeanTemp = 55
	terminal.P95Temp = 56
	terminal.P95VRTemp = 70
	terminal.P95Power = 18
	terminal.MeasuredAt = now.Add(12 * time.Minute)
	if err := store.FailMutationAndFinalizeTrial(
		&state, terminal, TrialReturn, attemptID, MutationFailurePreflight,
		terminal.MeasuredAt, time.Time{}, time.Time{},
	); err != nil {
		t.Fatalf("finalize return: %v", err)
	}
	if state.PendingKind != "" || state.FallbackPoint() != (OperatingPoint{}) || state.Phase != PhaseBaseline {
		t.Fatalf("return state = %+v", state)
	}
	if _, err := store.AdmitTrial(&state, OperatingPoint{Frequency: 525, CoreVoltage: 1100}, baseline, PhaseUndervolt, 100, now, now.Add(time.Hour)); err == nil {
		t.Fatal("duplicate candidate was admitted")
	}
}

func TestFailedTrialClosesAttemptAndPointAtomically(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	state.BestHashRate = 100
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	candidate := OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	attemptID, err := store.AdmitTrial(&state, candidate, state.CurrentPoint(), PhaseUndervolt, 100, now.Add(time.Minute), now.Add(21*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.ListPoints(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	record, found := findTestPoint(records, candidate)
	if !found {
		t.Fatal("candidate row is missing")
	}
	record.Status = PointUnobservable
	record.MeasuredAt = now.Add(3 * time.Minute)
	if err := store.FailMutationAndFinalizeTrial(
		&state, record, TrialReturn, attemptID, MutationFailurePreflight,
		now.Add(3*time.Minute), now.Add(4*time.Minute), now.Add(24*time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	if state.Phase != PhaseBaseline || state.PendingKind != "" || state.FallbackPoint() != (OperatingPoint{}) {
		t.Fatalf("failed trial state = %+v", state)
	}
	records, err = store.ListPoints(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	record, found = findTestPoint(records, candidate)
	if !found || record.Status != PointUnobservable {
		t.Fatalf("failed candidate row = %+v", record)
	}
	attempts, err := store.ListMutationAttempts(testMAC)
	if err != nil || len(attempts) != 1 || attempts[0].FailureStage != MutationFailurePreflight {
		t.Fatalf("failed attempt = %+v, %v", attempts, err)
	}
}

func TestMutationMilestonesAndAtomicResume(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	target := OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	state.SetPendingMutation(MutationOperatingPoint, target, now)
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	attempt := MutationAttempt{
		MacAddr: testMAC, Kind: MutationOperatingPoint,
		FromFrequency: 525, FromCoreVoltage: 1150,
		TargetFrequency: 525, TargetCoreVoltage: 1100,
		IntentCreatedAt: now, StartedAt: now,
		ConfiguredVerifiedUptimeSeconds: -1,
	}
	id, err := store.StartMutationAttempt(&attempt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartMutationAttempt(&MutationAttempt{
		MacAddr: testMAC, Kind: MutationOperatingPoint,
		FromFrequency: 525, FromCoreVoltage: 1150,
		TargetFrequency: 525, TargetCoreVoltage: 1200,
		IntentCreatedAt: now, StartedAt: now,
		ConfiguredVerifiedUptimeSeconds: -1,
	}); err == nil {
		t.Fatal("second unfinished attempt was accepted")
	}
	if err := store.AdvanceMutationAttempt(id, MutationMilestonePatchRequested, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, MutationMilestonePatchRequested, now.Add(2*time.Second)); err == nil {
		t.Fatal("conflicting PATCH milestone was accepted")
	}
	if err := store.RecordConfiguredVerification(id, now.Add(2*time.Second), 101); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, MutationMilestoneRestartRequested, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, MutationMilestoneRebootVerified, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	state.SetCurrentPoint(target)
	state.ClearPendingMutation()
	state.Phase = PhaseHold
	state.HoldReason = HoldBlocked
	state.SetFallbackPoint(OperatingPoint{})
	state.RampUntil = now.Add(5 * time.Second)
	if err := store.CompleteMutationAttempt(&state, id, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadMiner(testMAC)
	if err != nil || loaded.Phase != PhaseBaseline || loaded.HoldReason != "" || loaded.CurrentPoint() != target {
		t.Fatalf("completion was not store-derived: %+v, %v", loaded, err)
	}
	if err := store.RecordFirstPositive(id, now.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	state, err = store.LoadMiner(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteMiningResume(&state, id, now.Add(7*time.Second), now.Add(8*time.Second), now.Add(18*time.Minute)); err != nil {
		t.Fatal(err)
	}
	attempts, err := store.ListMutationAttempts(testMAC)
	if err != nil || len(attempts) != 1 || attempts[0].MiningResumedAt.IsZero() {
		t.Fatalf("resumed attempt = %+v, %v", attempts, err)
	}
	if _, ok, err := store.PendingMutationResume(testMAC); err != nil || ok {
		t.Fatalf("pending resume = %t, %v", ok, err)
	}
}

func TestStartMutationAttemptRequiresIntentTimestampMatch(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	target := OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	state.SetPendingMutation(MutationOperatingPoint, target, now)
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	attempt := MutationAttempt{
		MacAddr: testMAC, Kind: MutationOperatingPoint,
		FromFrequency: state.CurrentFrequency, FromCoreVoltage: state.CurrentCoreVoltage,
		TargetFrequency: target.Frequency, TargetCoreVoltage: target.CoreVoltage,
		IntentCreatedAt: now.Add(time.Second), StartedAt: now.Add(time.Second),
		ConfiguredVerifiedUptimeSeconds: -1,
	}
	if _, err := store.StartMutationAttempt(&attempt); err == nil {
		t.Fatal("mutation attempt with a mismatched intent timestamp was accepted")
	}
}

func TestQuarantineMutationClosesAmbiguousCandidateAtomically(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	state.BestHashRate = 100
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	candidate := OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	attemptID, err := store.AdmitTrial(&state, candidate, state.CurrentPoint(), PhaseFrequencyTest, 100, now.Add(time.Minute), now.Add(21*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(attemptID, MutationMilestonePatchRequested, now.Add(90*time.Second)); err != nil {
		t.Fatal(err)
	}
	state.ClearPendingMutation()
	state.SetFallbackPoint(OperatingPoint{})
	state.Phase = PhaseOverheat
	state.HoldReason = ""
	state.SettledAt = time.Time{}
	state.EvidenceDeadlineAt = time.Time{}
	state.SafetyReason = SafetyReasonMutationUncertain
	state.OverheatCount = 1
	state.CooldownUntil = now.Add(time.Hour)
	failedAt := now.Add(2 * time.Minute)
	if err := store.QuarantineMutation(&state, attemptID, MutationFailureConfiguredVerification, failedAt); err != nil {
		t.Fatal(err)
	}
	records, err := store.ListPoints(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	record, found := findTestPoint(records, candidate)
	if !found || record.Status != PointUnobservable {
		t.Fatalf("quarantined candidate = %+v", record)
	}
	attempts, err := store.ListMutationAttempts(testMAC)
	if err != nil || len(attempts) != 1 || attempts[0].FailureStage != MutationFailureConfiguredVerification {
		t.Fatalf("quarantined attempt = %+v, %v", attempts, err)
	}
	loaded, err := store.LoadMiner(testMAC)
	if err != nil || loaded.Phase != PhaseOverheat || loaded.SafetyReason != SafetyReasonMutationUncertain || loaded.PendingKind != "" {
		t.Fatalf("quarantined state = %+v, %v", loaded, err)
	}
}

func TestSafetyTransitionSupersedesChangedSafetyAttempt(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	minimum := OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	state.Phase = PhaseCooldown
	state.SafetyReason = SafetyReasonASICLimit
	state.SetPendingMutation(MutationSafetyRollback, minimum, now)
	state.CooldownUntil = now
	if err := store.SaveMiner(&state); err != nil {
		t.Fatal(err)
	}
	attempt := MutationAttempt{
		MacAddr: testMAC, Kind: MutationSafetyRollback, Reason: SafetyReasonASICLimit,
		FromFrequency: 525, FromCoreVoltage: 1150,
		TargetFrequency: 400, TargetCoreVoltage: 1000,
		IntentCreatedAt: now, StartedAt: now,
		ConfiguredVerifiedUptimeSeconds: -1,
	}
	attemptID, err := store.StartMutationAttempt(&attempt)
	if err != nil {
		t.Fatal(err)
	}
	state, err = store.LoadMiner(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	expected := state
	state.Phase = PhaseOverheat
	state.SafetyReason = SafetyReasonFirmwareOverheat
	state.SetPendingMutation(MutationOverheatRecovery, minimum, now.Add(time.Minute))
	if err := store.PersistSafetyTransition(&expected, &state, nil, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	attempts, err := store.ListMutationAttempts(testMAC)
	if err != nil || len(attempts) != 1 || attempts[0].ID != attemptID ||
		attempts[0].FailureStage != MutationFailureSafetySuperseded {
		t.Fatalf("superseded safety attempt = %+v, %v", attempts, err)
	}
}

func TestHourlyCursorCASAndGeneralSaveCannotRegressIt(t *testing.T) {
	store := openTestStore(t)
	_, now := bootstrapTestMiner(t, store)
	end := now.Add(90 * time.Minute)
	fragments := []HourlyAggregate{
		{MacAddr: testMAC, HourStartedAt: now.Truncate(time.Hour), ObservedSeconds: 1800, UnknownGapSeconds: 1800, ActualHashSeconds: 180000},
		{MacAddr: testMAC, HourStartedAt: now.Add(time.Hour).Truncate(time.Hour), UnknownGapSeconds: 1800},
	}
	if err := store.CompareAndSetHourly(testMAC, now, end, fragments, end); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadMiner(testMAC)
	if err != nil || !loaded.AccountedThroughAt.Equal(end) {
		t.Fatalf("cursor = %v, %v", loaded.AccountedThroughAt, err)
	}
	loaded.AccountedThroughAt = now
	if err := store.SaveMiner(&loaded); err != nil {
		t.Fatal(err)
	}
	loaded, err = store.LoadMiner(testMAC)
	if err != nil || !loaded.AccountedThroughAt.Equal(end) {
		t.Fatalf("cursor regressed to %v, %v", loaded.AccountedThroughAt, err)
	}
	if err := store.CompareAndSetHourly(testMAC, now, end.Add(time.Minute), nil, end); !errors.Is(err, ErrAccountingCursorChanged) {
		t.Fatalf("stale cursor error = %v", err)
	}
	aggregates, err := store.ListHourly(testMAC, now.Add(-time.Hour), end.Add(time.Hour))
	if err != nil || len(aggregates) != 2 {
		t.Fatalf("hourly rows = %+v, %v", aggregates, err)
	}
}

func TestHourlyCursorRequiresExactRetainedCoverage(t *testing.T) {
	store := openTestStore(t)
	_, now := bootstrapTestMiner(t, store)
	end := now.Add(time.Hour)
	if err := store.CompareAndSetHourly(testMAC, now, end, []HourlyAggregate{
		{MacAddr: testMAC, HourStartedAt: now, ObservedSeconds: 1800, ActualHashSeconds: 180000},
	}, now); err == nil {
		t.Fatal("partial hourly coverage was accepted")
	}
}

func TestHourlyRetentionKeepsBucketWhoseEndIsAfterBoundary(t *testing.T) {
	store := openTestStore(t)
	_, now := bootstrapTestMiner(t, store)
	boundaryHour := now.Add(-LongTermRetentionHours * time.Hour)
	if _, err := store.conn.ExecContext(context.Background(), `INSERT INTO optimizer_hourly (
		mac_addr, hour_started_at, observed_seconds, unknown_gap_seconds,
		actual_hash_seconds, trial_actual_hash_seconds,
		incumbent_counterfactual_hash_seconds, settled_seconds, trial_seconds
	) VALUES (?, ?, 3600, 0, 360000, 0, 0, 0, 0)`, testMAC, boundaryHour.Unix()); err != nil {
		t.Fatal(err)
	}
	if err := store.CompareAndSetHourly(testMAC, now, now.Add(time.Hour), []HourlyAggregate{
		{MacAddr: testMAC, HourStartedAt: now, UnknownGapSeconds: 3600},
	}, now); err != nil {
		t.Fatal(err)
	}
	rows, err := store.ListHourly(testMAC, boundaryHour, now.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].HourStartedAt.Unix() != boundaryHour.Unix() {
		t.Fatalf("retention boundary rows = %+v", rows)
	}
}

func TestUnexpectedViewRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.conn.ExecContext(context.Background(), "CREATE VIEW unexpected_view AS SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenOptimizerStore(path); err == nil {
		t.Fatal("unexpected view was accepted")
	}
}

func findTestPoint(points []OperatingPointRecord, point OperatingPoint) (OperatingPointRecord, bool) {
	for _, record := range points {
		if record.Point() == point {
			return record, true
		}
	}
	return OperatingPointRecord{}, false
}
