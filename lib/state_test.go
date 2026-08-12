package lib

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	result, err := store.Apply(Bootstrap{
		Info: testInfo(), IP: testIP, PairAdvertised: true,
	}, now)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if !result.Created {
		t.Fatal("bootstrap did not create miner")
	}
	return result.State, now
}

func TestSchemaV11BootstrapAndReopen(t *testing.T) {
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
	if err != nil || version != 11 {
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

func TestMonitorReferencePersistsCombinedAssessmentAcrossReopen(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	point := state.CurrentPoint()
	baseline := mustOpenEpoch(t, store, state.MacAddr)
	record := OperatingPointRecord{
		MacAddr: state.MacAddr, Frequency: point.Frequency, CoreVoltage: point.CoreVoltage,
		Status: PointValidated, MedianHash: 100, ExpectedHash: 100, Attainment: 1,
		MeanTemp: 55, P95Temp: 56, P95VRTemp: 70, P95Power: 18,
		MeasuredAt: now.Add(time.Minute), EnteredAt: state.PassStartedAt,
	}
	result, err := store.Apply(CompleteBaseline{
		State: state, Record: record, Epoch: baseline, Decision: BaselinePlace,
		Selected: point, Best: point, BestHashRate: record.MedianHash,
	}, record.MeasuredAt)
	if err != nil {
		t.Fatal(err)
	}
	state = result.State
	first, err := NewWindowAggregate(30, 5*time.Minute, 100, 100, 1, 55, 56, 70, 18, nil, 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewWindowAggregate(30, 5*time.Minute, 90, 100, .9, 58, 60, 72, 19, nil, 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	epoch := mustOpenEpoch(t, store, state.MacAddr)
	progress := epoch.Progress
	if err := progress.CloseWindow(true, first); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply(AdvanceEpoch{Epoch: epoch, Progress: progress}, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	epoch = mustOpenEpoch(t, store, state.MacAddr)
	progress = epoch.Progress
	if err := progress.CloseWindow(true, second); err != nil {
		t.Fatal(err)
	}
	combined, err := first.Combine(second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply(CompleteMonitor{
		State: state, Epoch: epoch, Progress: progress, Aggregate: second,
		Decision: MonitorContinue, NextReason: MonitorSelected,
	}, now.Add(3*time.Minute)); err == nil {
		t.Fatal("single window was accepted as a combined monitor reference")
	}
	result, err = store.Apply(CompleteMonitor{
		State: state, Epoch: epoch, Progress: progress, Aggregate: combined,
		Decision: MonitorContinue, NextReason: MonitorSelected,
	}, now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	state = result.State
	if state.MonitorReferenceEpochID != epoch.ID {
		t.Fatalf("monitor reference epoch = %d, want %d", state.MonitorReferenceEpochID, epoch.ID)
	}
	storedEpoch, err := store.EvidenceEpochByID(epoch.ID)
	if err != nil {
		t.Fatal(err)
	}
	storedFirst, ok := storedEpoch.Progress.ClosedWindow()
	if !ok || storedEpoch.Progress.ClosedWindows() != 2 || storedFirst.MedianHash() != first.MedianHash() {
		t.Fatalf("evidence ledger did not retain the first window: %+v", storedEpoch.Progress)
	}
	reference, err := store.MonitorReference(epoch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reference.SampleCount() != 60 || reference.MedianHash() != 90 || reference.P95Temp() != 60 ||
		reference.AcceptedDelta() != 9 || reference.RejectedDelta() != 1 {
		t.Fatalf("combined monitor reference = %+v", reference)
	}
	path := storePath(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reference, err = reopened.MonitorReference(epoch.ID)
	if err != nil || reference.SampleCount() != 60 || reference.MedianHash() != 90 || reference.P95Temp() != 60 {
		t.Fatalf("reopened combined monitor reference = %+v, %v", reference, err)
	}
}

func TestReopenRejectsSelectedMonitorWithoutCombinedReference(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	state = closeInitialBaselineEpoch(t, store, state, now.Add(time.Minute))
	if _, err := store.conn.ExecContext(context.Background(),
		"DELETE FROM monitor_references WHERE evidence_epoch_id = ?", state.MonitorReferenceEpochID); err != nil {
		t.Fatal(err)
	}
	path := storePath(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, open := range []func(string) (*OptimizerStore, error){OpenOptimizerStore, OpenOptimizerStoreReadOnly} {
		reopened, err := open(path)
		if err == nil || !strings.Contains(err.Error(), "invalid monitor reference epoch") {
			if reopened != nil {
				_ = reopened.Close()
			}
			t.Fatalf("missing combined monitor reference reopen error = %v", err)
		}
	}
}

func TestBootstrapOffGridPairIsRejectedWithoutFrontierRow(t *testing.T) {
	store := openTestStore(t)
	offGrid := testInfo()
	offGrid.Frequency = 500
	offGrid.CoreVoltage = 1000
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	result, err := store.Apply(Bootstrap{
		Info: offGrid, IP: testIP, PairAdvertised: true,
	}, now)
	if err != nil || !result.Created {
		t.Fatalf("off-grid bootstrap = %+v, %t, %v", result.State, result.Created, err)
	}
	state := result.State
	if state.Phase != PhaseMonitor || state.MonitorReason != MonitorOffGrid ||
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
		if _, err := store.Apply(SaveState{State: candidate}, now); err == nil {
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

// TestRecoveryHealthyCountValidation pins validateMinerState's RecoveryHealthyCount boundary: a
// negative value is always rejected, a nonzero value is rejected outside COOLDOWN/EMERGENCY (the
// counter's only meaningful phases), and a nonzero value is accepted inside either.
func TestRecoveryHealthyCountValidation(t *testing.T) {
	store := openTestStore(t)
	state, _ := bootstrapTestMiner(t, store)
	if state.Phase == PhaseCooldown || state.Phase == PhaseEmergency {
		t.Fatalf("test fixture assumption violated: bootstrap phase = %s", state.Phase)
	}
	negative := state
	negative.RecoveryHealthyCount = -1
	if err := validateMinerState(negative); err == nil {
		t.Fatal("negative recovery healthy count was accepted")
	}
	outsideCooldown := state
	outsideCooldown.RecoveryHealthyCount = 3
	if err := validateMinerState(outsideCooldown); err == nil {
		t.Fatalf("nonzero recovery healthy count was accepted outside COOLDOWN/EMERGENCY: phase=%s", outsideCooldown.Phase)
	}
	for _, phase := range []OptimizerPhase{PhaseCooldown, PhaseEmergency} {
		inPhase := state
		inPhase.Phase = phase
		inPhase.RecoveryHealthyCount = 3
		if err := validateMinerState(inPhase); err != nil {
			t.Fatalf("nonzero recovery healthy count was rejected inside %s: %v", phase, err)
		}
	}
}

func TestUnobservablePointCannotPersistEvidence(t *testing.T) {
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	record := OperatingPointRecord{
		MacAddr: testMAC, Frequency: 525, CoreVoltage: 1150,
		Status: PointUnobservable, MedianHash: 1, EnteredAt: now,
		MeasuredAt: now.Add(time.Minute),
	}
	if err := validatePointRecord(record); err == nil {
		t.Fatal("unobservable point with evidence was accepted")
	}
	validated := record
	validated.Status = PointValidated
	validated.MedianHash = 0
	validated.MeanTemp = 55
	validated.P95Temp = 55
	validated.P95VRTemp = 70
	validated.P95Power = 18
	if err := validatePointRecord(validated); err == nil {
		t.Fatal("validated point without positive hash evidence was accepted")
	}
}

func TestAdoptExternalPointAllowsOffGridManualObservation(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	candidate := OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	admitResult := admitTestTrial(t, store, state, candidate, PhaseUndervolt, 100, now.Add(time.Minute))
	state = admitResult.State
	attemptID := admitResult.AttemptID
	offGrid := OperatingPoint{Frequency: 500, CoreVoltage: 1000}
	adoptResult, err := store.Apply(AdoptExternalPoint{
		State: state, Point: offGrid, AttemptID: attemptID, Stage: MutationFailurePreflight,
	}, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("adopt off-grid external point: %v", err)
	}
	state = adoptResult.State
	if state.CurrentPoint() != offGrid || state.Phase != PhaseMonitor || state.MonitorReason != MonitorManual {
		t.Fatalf("off-grid manual state = %+v", state)
	}
	points, err := store.ListPoints(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := findTestPoint(points, offGrid); found {
		t.Fatalf("off-grid manual point entered frontier history: %+v", points)
	}
	candidateRecord, found := findTestPoint(points, candidate)
	if !found || candidateRecord.Status != PointUnobservable {
		t.Fatalf("external candidate record = %+v", candidateRecord)
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
		accepted_delta, rejected_delta, measured_at, entered_at, entry_attempt_id, reference_hash,
		evidence_epoch_id
	) VALUES (?, 500, 1000, ?, 0, 0, 0, 0, 0, 0, 0, NULL, 0, 0, 0, ?, 0, 0, 0)`,
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
	epochForBaseline := mustOpenEpoch(t, store, testMAC)
	finalizeResult, err := store.Apply(CompleteBaseline{
		State: state, Record: baseline, Epoch: epochForBaseline, Decision: BaselinePlace,
		Selected: selected, Best: selected, BestHashRate: baseline.MedianHash,
	}, baseline.MeasuredAt)
	if err != nil {
		t.Fatalf("complete baseline: %v", err)
	}
	state = finalizeResult.State
	settledAt := now.Add(20 * time.Minute)
	state = settleSelectedMonitor(t, store, state, settledAt)

	passStart := now.Add(time.Hour)
	if _, err := store.Apply(StartPass{
		MacAddr: testMAC, Point: selected, Trigger: PassOperator,
	}, passStart); err != nil {
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
	if _, err := reopened.Apply(SaveState{State: mutated}, passStart); err != nil {
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
	state = closeInitialBaselineEpoch(t, store, state, now)
	state.Phase = PhaseMonitor
	state.MonitorReason = MonitorManual
	state.SettledAt = now.Add(time.Hour)
	if _, err := store.Apply(SaveState{State: state}, now); err != nil {
		t.Fatal(err)
	}
	passStart := now.Add(2 * time.Hour)
	resetResult, err := store.Apply(StartPass{
		MacAddr: testMAC, Point: state.CurrentPoint(), Trigger: PassOperator,
	}, passStart)
	if err != nil {
		t.Fatalf("manual retune: %v", err)
	}
	state = resetResult.State
	baseline := OperatingPointRecord{
		MacAddr: testMAC, Frequency: state.CurrentFrequency, CoreVoltage: state.CurrentCoreVoltage,
		Status: PointValidated, MedianHash: 100, ExpectedHash: 100, Attainment: 1,
		MeanTemp: 55, P95Temp: 56, P95VRTemp: 70, P95Power: 18,
		MeasuredAt: passStart.Add(10 * time.Minute), EnteredAt: passStart,
	}
	epochForBaseline := mustOpenEpoch(t, store, testMAC)
	finalizeResult, err := store.Apply(CompleteBaseline{
		State: state, Record: baseline, Epoch: epochForBaseline, Decision: BaselinePlace,
		Selected: state.CurrentPoint(), Best: state.CurrentPoint(), BestHashRate: baseline.MedianHash,
	}, baseline.MeasuredAt)
	if err != nil {
		t.Fatalf("manual baseline: %v", err)
	}
	state = finalizeResult.State
	if state.PassTrigger != PassOperator || state.PassReferenceHash != 0 ||
		state.PassReferenceFrequency != 0 || state.PassReferenceCoreVoltage != 0 ||
		!state.PassReferenceSettledAt.IsZero() {
		t.Fatalf("manual retune created an arm snapshot: %+v", state)
	}
}

func TestUnsettledManualHoldPersistsWithoutEvidenceDeadline(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	state = closeInitialBaselineEpoch(t, store, state, now)
	result, err := store.Apply(AdoptManualPoint{MacAddr: state.MacAddr, Point: state.CurrentPoint()}, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("unsettled manual hold: %v", err)
	}
	if result.State.Phase != PhaseMonitor || result.State.MonitorReason != MonitorManual || !result.State.SettledAt.IsZero() {
		t.Fatalf("manual hold state = %+v", result.State)
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

// closeInitialBaselineEpoch completes the bootstrap baseline into a settled optimized hold. Tests
// that need a candidate use admitTestTrial instead so baseline closure and admission stay atomic.
func closeInitialBaselineEpoch(t *testing.T, store *OptimizerStore, state MinerState, at time.Time) MinerState {
	t.Helper()
	epoch := mustOpenEpoch(t, store, state.MacAddr)
	record := OperatingPointRecord{
		MacAddr: state.MacAddr, Frequency: state.CurrentFrequency, CoreVoltage: state.CurrentCoreVoltage,
		Status: PointValidated, MedianHash: 100, ExpectedHash: 100, Attainment: 1,
		MeanTemp: 55, P95Temp: 56, P95VRTemp: 70, P95Power: 18,
		MeasuredAt: at, EnteredAt: state.PassStartedAt,
	}
	result, err := store.Apply(CompleteBaseline{
		State: state, Record: record, Epoch: epoch, Decision: BaselinePlace,
		Selected: state.CurrentPoint(), Best: state.CurrentPoint(), BestHashRate: record.MedianHash,
	}, at)
	if err != nil {
		t.Fatalf("close initial baseline epoch: %v", err)
	}
	return settleSelectedMonitor(t, store, result.State, at)
}

func settleSelectedMonitor(t *testing.T, store *OptimizerStore, state MinerState, at time.Time) MinerState {
	t.Helper()
	monitorEpoch := mustOpenEpoch(t, store, state.MacAddr)
	window, err := NewWindowAggregate(30, 5*time.Minute, 100, 100, 1, 55, 56, 70, 18, nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	progress := monitorEpoch.Progress
	if err := progress.CloseWindow(true, window); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply(AdvanceEpoch{Epoch: monitorEpoch, Progress: progress}, at); err != nil {
		t.Fatal(err)
	}
	monitorEpoch = mustOpenEpoch(t, store, state.MacAddr)
	progress = monitorEpoch.Progress
	if err := progress.CloseWindow(true, window); err != nil {
		t.Fatal(err)
	}
	combined, err := window.Combine(window)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Apply(CompleteMonitor{
		State: state, Epoch: monitorEpoch, Progress: progress, Aggregate: combined,
		Decision: MonitorContinue, NextReason: MonitorSelected,
	}, at)
	if err != nil {
		t.Fatalf("settle initial selected monitor: %v", err)
	}
	return result.State
}

func admitTestTrial(
	t *testing.T,
	store *OptimizerStore,
	state MinerState,
	candidate OperatingPoint,
	phase OptimizerPhase,
	referenceHash float64,
	at time.Time,
) TransitionResult {
	t.Helper()
	epoch := mustOpenEpoch(t, store, state.MacAddr)
	record := OperatingPointRecord{
		MacAddr: state.MacAddr, Frequency: state.CurrentFrequency, CoreVoltage: state.CurrentCoreVoltage,
		Status: PointValidated, MedianHash: 100, ExpectedHash: 100, Attainment: 1,
		MeanTemp: 55, P95Temp: 56, P95VRTemp: 70, P95Power: 18,
		MeasuredAt: at, EnteredAt: state.PassStartedAt,
	}
	result, err := store.Apply(CompleteBaseline{
		State: state, Record: record, Epoch: epoch, Decision: BaselineContinue,
		Candidate: candidate, CandidatePhase: phase, ReferenceHash: referenceHash,
	}, at)
	if err != nil {
		t.Fatalf("admit test trial: %v", err)
	}
	return result
}

func mustOpenEpoch(t *testing.T, store *OptimizerStore, macAddr string) EvidenceEpoch {
	t.Helper()
	epoch, open, err := store.OpenEvidenceEpochFor(macAddr)
	if err != nil {
		t.Fatalf("open evidence epoch: %v", err)
	}
	if !open {
		t.Fatalf("miner %s has no open evidence epoch", macAddr)
	}
	return epoch
}

func storePath(t *testing.T, store *OptimizerStore) string {
	t.Helper()
	var path string
	if err := store.conn.QueryRowContext(context.Background(), "PRAGMA database_list").Scan(new(int), new(string), &path); err != nil {
		t.Fatalf("database path: %v", err)
	}
	return path
}

// Earlier schemas encode terminal monitoring semantics. Reject them whole rather than silently
// reinterpreting durable state under the schema-11 continuous-monitor contract.
func TestRejectPriorSchemasWithoutMigration(t *testing.T) {
	for _, version := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "optimizer.db")
			store, err := OpenOptimizerStore(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.conn.ExecContext(context.Background(), fmt.Sprintf("PRAGMA user_version = %d", version)); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenOptimizerStore(path); err == nil {
				t.Fatalf("schema v%d was accepted", version)
			}
			if _, err := OpenOptimizerStoreReadOnly(path); err == nil {
				t.Fatalf("schema v%d was accepted read-only", version)
			}
			database, err := sql.Open("sqlite3", path)
			if err != nil {
				t.Fatal(err)
			}
			var storedVersion int
			if err := database.QueryRow("PRAGMA user_version").Scan(&storedVersion); err != nil {
				_ = database.Close()
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			if storedVersion != version {
				t.Fatalf("rejected schema was modified: got v%d, want v%d", storedVersion, version)
			}
		})
	}
}

// TestSchemaV11ExactColumnAndIndexSet locks the evidence_epochs shape and the two indexes
// (evidence_epochs_one_open, evidence_epochs_mac_opened) into the schema-validation boundary: an
// unexpected column or a missing/altered index must fail loudly, not silently.
func TestSchemaV11ExactColumnAndIndexSet(t *testing.T) {
	store := openTestStore(t)
	columns, err := tableColumns(context.Background(), store.conn, "evidence_epochs")
	if err != nil {
		t.Fatal(err)
	}
	expected := optimizerSchema["evidence_epochs"]
	if len(columns) != len(expected) {
		t.Fatalf("evidence_epochs column count = %d, want %d", len(columns), len(expected))
	}
	for index := range expected {
		if columns[index] != expected[index] {
			t.Fatalf("evidence_epochs column %d = %+v, want %+v", index, columns[index], expected[index])
		}
	}
	if err := validateOptimizerIndexes(context.Background(), store.conn); err != nil {
		t.Fatalf("index validation failed on a freshly created schema: %v", err)
	}
	if _, err := store.conn.ExecContext(context.Background(), "DROP INDEX evidence_epochs_mac_opened"); err != nil {
		t.Fatal(err)
	}
	if err := validateOptimizerIndexes(context.Background(), store.conn); err == nil {
		t.Fatal("index validation accepted a missing evidence_epochs_mac_opened index")
	}
}

// TestNewEpochProgressRejectsInvalidCombinations covers the constructor rejection table required by
// the evidence-epoch boundary tests: negative counters, a window without a matching closed-window
// count, and vice versa.
func TestNewEpochProgressRejectsInvalidCombinations(t *testing.T) {
	window, err := NewWindowAggregate(30, 5*time.Minute, 100, 100, 1, 55, 56, 70, 18, nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name                              string
		settled, closed, rejected, missed int
		window                            *WindowAggregate
	}{
		{name: "negative settled", settled: -1},
		{name: "negative closed", closed: -1},
		{name: "negative rejected", rejected: -1},
		{name: "negative missed", missed: -1},
		{name: "closed windows exceed two", closed: 3, window: &window},
		{name: "closed windows without a window", closed: 1},
		{name: "window without closed windows", window: &window},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := newEpochProgress(testCase.settled, testCase.closed, testCase.rejected, testCase.missed, testCase.window); err == nil {
				t.Fatalf("%s: invalid epoch progress was accepted", testCase.name)
			}
		})
	}
	if _, err := newEpochProgress(6, 1, 2, 0, &window); err != nil {
		t.Fatalf("valid epoch progress was rejected: %v", err)
	}
}

// TestDecodeRejectsUnknownEnumValues is the constructor/decode rejection path required for every
// closed set this commit introduces or extends: a hand-edited row with an out-of-set value must be
// rejected at load, not carried into the core.
func TestDecodeRejectsUnknownEnumValues(t *testing.T) {
	if validPointStatus(PointStatus("bogus")) {
		t.Fatal("unknown point status validated")
	}
	if validEpochPurpose(EpochPurpose("bogus")) {
		t.Fatal("unknown epoch purpose validated")
	}
	if validEpochOutcome(EpochOutcome("bogus")) {
		t.Fatal("unknown epoch outcome validated")
	}
	if validMonitorReason(MonitorReason("bogus")) {
		t.Fatal("unknown hold reason validated")
	}
	window, err := NewWindowAggregate(30, 5*time.Minute, 100, 100, 1, 55, 56, 70, 18, nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	progress, err := newEpochProgress(6, 1, 0, 0, &window)
	if err != nil {
		t.Fatal(err)
	}
	epoch := EvidenceEpoch{
		ID: 1, MacAddr: testMAC, Point: OperatingPoint{Frequency: 525, CoreVoltage: 1150},
		Purpose: EpochPurpose("bogus"), RequiredWindows: 2, OpenedAt: time.Now().UTC(), Progress: progress,
	}
	if err := validateEvidenceEpoch(epoch, true); err == nil {
		t.Fatal("evidence epoch with an unknown purpose was accepted")
	}
	epoch.Purpose = EpochBaseline
	epoch.Outcome = EpochOutcome("bogus")
	epoch.ClosedAt = time.Now().UTC()
	if err := validateEvidenceEpoch(epoch, true); err == nil {
		t.Fatal("evidence epoch with an unknown outcome was accepted")
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

func TestBaselineContinuationBindsOneEntryAttemptAndFinalizesReturn(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	baseline := state.CurrentPoint()
	candidate := OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	epoch := mustOpenEpoch(t, store, testMAC)
	baselineRecord := OperatingPointRecord{
		MacAddr: testMAC, Frequency: baseline.Frequency, CoreVoltage: baseline.CoreVoltage,
		Status: PointValidated, MedianHash: 100, ExpectedHash: 100, Attainment: 1,
		MeanTemp: 55, P95Temp: 56, P95VRTemp: 70, P95Power: 18,
		MeasuredAt: now.Add(time.Minute), EnteredAt: state.PassStartedAt,
	}
	if _, err := store.Apply(CompleteBaseline{
		State: state, Record: baselineRecord, Epoch: epoch, Decision: BaselineContinue,
		Candidate: candidate, CandidatePhase: PhaseUndervolt, ReferenceHash: 99,
	}, now.Add(time.Minute)); err == nil {
		t.Fatal("stale frozen reference was accepted")
	}
	state.PassReferenceHash = 100
	if _, err := store.Apply(SaveState{State: state}, now); err != nil {
		t.Fatal(err)
	}
	state.PassReferenceHash = 1
	if _, err := store.Apply(SaveState{State: state}, now); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadMiner(testMAC)
	if err != nil || loaded.PassReferenceHash != 100 {
		t.Fatalf("positive pass reference was overwritten: %+v, %v", loaded, err)
	}
	state = loaded
	admitResult := admitTestTrial(t, store, state, candidate, PhaseUndervolt, 100, now.Add(time.Minute))
	state = admitResult.State
	attemptID := admitResult.AttemptID
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
	// A mutation failure this early (preflight, before mining ever resumed) never observed real
	// telemetry and never reached CompleteResume, so the candidate never had a trial epoch to
	// close — matching mutation.go's real FailMutationFinalizeTrial call sites, which always zero
	// the measurement fields and use PointUnstable/PointUnobservable, never a measured status.
	terminal := entry
	terminal.Status = PointUnstable
	terminal.MeasuredAt = now.Add(12 * time.Minute)
	failResult, err := store.Apply(FailMutationFinalizeTrial{
		State: state, Record: terminal, Decision: TrialReturn, AttemptID: attemptID,
		Stage: MutationFailurePreflight,
	}, terminal.MeasuredAt)
	if err != nil {
		t.Fatalf("finalize return: %v", err)
	}
	state = failResult.State
	if state.PendingKind != "" || state.FallbackPoint() != (OperatingPoint{}) || state.Phase != PhaseBaseline {
		t.Fatalf("return state = %+v", state)
	}
	epoch = mustOpenEpoch(t, store, testMAC)
	baselineRecord.MeasuredAt = now.Add(13 * time.Minute)
	if _, err := store.Apply(CompleteBaseline{
		State: state, Record: baselineRecord, Epoch: epoch, Decision: BaselineContinue,
		Candidate: candidate, CandidatePhase: PhaseUndervolt, ReferenceHash: 100,
	}, baselineRecord.MeasuredAt); err == nil {
		t.Fatal("duplicate candidate was admitted")
	}
}

func TestFailedTrialClosesAttemptAndPointAtomically(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	candidate := OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	admitResult := admitTestTrial(t, store, state, candidate, PhaseUndervolt, 100, now.Add(time.Minute))
	state = admitResult.State
	attemptID := admitResult.AttemptID
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
	mismatched := record
	mismatched.EntryAttemptID = attemptID + 1
	if _, err := store.Apply(FailMutationFinalizeTrial{
		State: state, Record: mismatched, Decision: TrialReturn, AttemptID: attemptID,
		Stage: MutationFailurePreflight,
	}, now.Add(3*time.Minute)); err == nil {
		t.Fatal("trial record for another attempt was accepted")
	}
	failResult, err := store.Apply(FailMutationFinalizeTrial{
		State: state, Record: record, Decision: TrialReturn, AttemptID: attemptID,
		Stage: MutationFailurePreflight,
	}, now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	state = failResult.State
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

// TestCompleteResumeDoesNotPreopenCooldownEpoch is a regression test for a bug found analyzing a
// live deployment: applyCompleteResume opened a safety_validation epoch immediately for any
// completed MutationSafetyRollback/MutationFirmwareRecovery attempt (the old resume mapping
// treated PhaseCooldown as safety_validation), which happens well before controlMinerAfterSafety's
// recovery
// predicate (recoveryHealthyPolls consecutive safeToRecover polls) has ever run. Once that epoch
// existed, every later COOLDOWN poll took the "epoch already open" branch and the healthy-poll
// count it should have been counting never advanced past whatever it happened to be at that
// instant — observed in production as a miner permanently stuck in COOLDOWN with
// RecoveryHealthyCount frozen far below threshold, which in turn fleet-wide-blocked all other
// miners' normal optimization work (mutationCoordinator.Advance treats any COOLDOWN/EMERGENCY miner
// as safetyBlocked). This test drives a real safety-rollback attempt through its full milestone
// lifecycle to CompleteResume and asserts no epoch is open afterward — only the recovery predicate
// may open one now.
func TestCompleteResumeDoesNotPreopenCooldownEpoch(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	baseline := mustOpenEpoch(t, store, testMAC)
	minimum := OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	incumbent := state.CurrentPoint()
	state.Phase = PhaseCooldown
	state.PhaseStartedAt = now
	state.SafetyReason = SafetyReasonTelemetryUnavailable
	state.SetPendingMutation(MutationSafetyRollback, minimum, now)
	if _, err := store.Apply(CloseEpoch{State: state, Epoch: baseline, Outcome: EpochContradicted}, now); err != nil {
		t.Fatal(err)
	}
	attempt := MutationAttempt{
		MacAddr: testMAC, Kind: MutationSafetyRollback, Reason: SafetyReasonTelemetryUnavailable,
		FromFrequency: incumbent.Frequency, FromCoreVoltage: incumbent.CoreVoltage,
		TargetFrequency: minimum.Frequency, TargetCoreVoltage: minimum.CoreVoltage,
		IntentCreatedAt: now, StartedAt: now,
		ConfiguredVerifiedUptimeSeconds: -1,
	}
	id, err := store.StartMutationAttempt(&attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, MutationMilestonePatchRequested, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordConfiguredVerification(id, now.Add(2*time.Second), 34); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, MutationMilestoneRestartRequested, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, MutationMilestoneRebootVerified, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	completeResult, err := store.Apply(CompleteMutation{
		MacAddr: state.MacAddr, IP: state.IP, AttemptID: id,
	}, now.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	state = completeResult.State
	if state.Phase != PhaseCooldown || state.SafetyReason == "" {
		t.Fatalf("safety completion did not land in COOLDOWN with SafetyReason intact: %+v", state)
	}
	if err := store.RecordFirstPositive(id, now.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply(CompleteResume{MacAddr: state.MacAddr, AttemptID: id}, now.Add(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, open, err := store.OpenEvidenceEpochFor(testMAC); err != nil {
		t.Fatal(err)
	} else if open {
		t.Fatal("CompleteResume opened a safety_validation epoch before the recovery predicate ran")
	}
	loaded, err := store.LoadMiner(testMAC)
	if err != nil || loaded.Phase != PhaseCooldown || loaded.SafetyReason == "" || loaded.PendingKind != "" {
		t.Fatalf("post-resume state = %+v, %v", loaded, err)
	}
}

func TestSafetyValidationEpochRequiresCompletedRecoveryAndRollsBack(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	epoch := mustOpenEpoch(t, store, state.MacAddr)
	state.Phase = PhaseCooldown
	state.PhaseStartedAt = now
	state.SafetyReason = SafetyReasonASICLimit
	result, err := store.Apply(CloseEpoch{State: state, Epoch: epoch, Outcome: EpochContradicted}, now)
	if err != nil {
		t.Fatal(err)
	}
	state = result.State

	if _, err := store.Apply(OpenEpoch{
		State: state, Purpose: EpochSafetyValidation, Point: state.CurrentPoint(), RequiredWindows: 1,
	}, now.Add(time.Minute)); err == nil || !strings.Contains(err.Error(), "lacks completed recovery proof") {
		t.Fatalf("pre-recovery safety epoch error = %v", err)
	}
	if _, open, err := store.OpenEvidenceEpochFor(state.MacAddr); err != nil || open {
		t.Fatalf("rejected transition left an open epoch: open=%t err=%v", open, err)
	}
	loaded, err := store.LoadMiner(state.MacAddr)
	if err != nil || loaded != state {
		t.Fatalf("rejected transition changed miner: loaded=%+v want=%+v err=%v", loaded, state, err)
	}

	state.SafetyReason = ""
	if _, err := store.Apply(OpenEpoch{
		State: state, Purpose: EpochSafetyValidation, Point: state.CurrentPoint(), RequiredWindows: 1,
	}, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("open proven safety epoch: %v", err)
	}
	path := storePath(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatalf("reopen canonical safety epoch: %v", err)
	}
	defer reopened.Close()
}

func TestNormalContradictionAtomicallyOpensDerivedSuccessor(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	interrupted := mustOpenEpoch(t, store, state.MacAddr)
	result, err := store.Apply(CloseEpoch{
		State: state, Epoch: interrupted, Outcome: EpochContradicted,
	}, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	resumed := mustOpenEpoch(t, store, state.MacAddr)
	if resumed.ID == interrupted.ID || resumed.Purpose != EpochBaseline || resumed.RequiredWindows != 2 ||
		resumed.Point != result.State.CurrentPoint() {
		t.Fatalf("normal contradiction successor = %+v", resumed)
	}
}

func TestApplyRollsBackNormalPhaseWithoutEpochAuthority(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	epoch := mustOpenEpoch(t, store, state.MacAddr)
	if _, err := store.Apply(CloseEpoch{
		State: state, Epoch: epoch, Outcome: EpochValidated,
	}, now.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "has no open baseline evidence epoch") {
		t.Fatalf("missing epoch postcondition error = %v", err)
	}
	open := mustOpenEpoch(t, store, state.MacAddr)
	if open.ID != epoch.ID || !open.ClosedAt.IsZero() {
		t.Fatalf("failed transition did not roll back epoch: %+v", open)
	}
}

func TestReopenRejectsNormalPhaseWithoutEpochAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatal(err)
	}
	state, now := bootstrapTestMiner(t, store)
	epoch := mustOpenEpoch(t, store, state.MacAddr)
	if _, err := store.conn.ExecContext(context.Background(),
		"UPDATE evidence_epochs SET closed_at = ?, outcome = ? WHERE id = ?",
		timeValue(now.Add(time.Second)), EpochContradicted, epoch.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, open := range []func(string) (*OptimizerStore, error){OpenOptimizerStore, OpenOptimizerStoreReadOnly} {
		if reopened, err := open(path); err == nil || !strings.Contains(err.Error(), "has no open baseline evidence epoch") {
			if reopened != nil {
				_ = reopened.Close()
			}
			t.Fatalf("missing epoch reopen error = %v", err)
		}
	}
}

func TestReopenRejectsPointEvidenceFromWrongEpochPurpose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatal(err)
	}
	state, now := bootstrapTestMiner(t, store)
	point := state.CurrentPoint()
	baseline := mustOpenEpoch(t, store, state.MacAddr)
	record := OperatingPointRecord{
		MacAddr: state.MacAddr, Frequency: point.Frequency, CoreVoltage: point.CoreVoltage,
		Status: PointValidated, MedianHash: 100, ExpectedHash: 100, Attainment: 1,
		MeanTemp: 55, P95Temp: 56, P95VRTemp: 70, P95Power: 18,
		MeasuredAt: now.Add(time.Minute), EnteredAt: state.PassStartedAt,
	}
	result, err := store.Apply(CompleteBaseline{
		State: state, Record: record, Epoch: baseline, Decision: BaselinePlace,
		Selected: point, Best: point, BestHashRate: record.MedianHash,
	}, record.MeasuredAt)
	if err != nil {
		t.Fatal(err)
	}
	state = settleSelectedMonitor(t, store, result.State, now.Add(2*time.Minute))
	if _, err := store.conn.ExecContext(context.Background(),
		"UPDATE operating_points SET evidence_epoch_id = ? WHERE mac_addr = ? AND frequency = ? AND core_voltage = ?",
		state.MonitorReferenceEpochID, state.MacAddr, point.Frequency, point.CoreVoltage); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, open := range []func(string) (*OptimizerStore, error){OpenOptimizerStore, OpenOptimizerStoreReadOnly} {
		if reopened, err := open(path); err == nil || !strings.Contains(err.Error(), "non-baseline evidence epoch") {
			if reopened != nil {
				_ = reopened.Close()
			}
			t.Fatalf("wrong point evidence reopen error = %v", err)
		}
	}
}

func TestReopenRejectsSafetyEpochWithoutRecoveryProof(t *testing.T) {
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := OpenOptimizerStore(path)
	if err != nil {
		t.Fatal(err)
	}
	state, now := bootstrapTestMiner(t, store)
	epoch := mustOpenEpoch(t, store, state.MacAddr)
	state.Phase = PhaseCooldown
	state.PhaseStartedAt = now
	state.SafetyReason = ""
	if _, err := store.Apply(CloseEpoch{
		State: state, Epoch: epoch, Outcome: EpochContradicted,
		Successor: &OpenEpoch{
			State: state, Purpose: EpochSafetyValidation, Point: state.CurrentPoint(), RequiredWindows: 1,
		},
	}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.conn.ExecContext(context.Background(),
		"UPDATE optimizer_miners SET safety_reason = ? WHERE mac_addr = ?",
		SafetyReasonTelemetryUnavailable, state.MacAddr); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenOptimizerStore(path); err == nil || !strings.Contains(err.Error(), "lacks completed recovery proof") {
		t.Fatalf("writable reopen error = %v", err)
	}
	if _, err := OpenOptimizerStoreReadOnly(path); err == nil || !strings.Contains(err.Error(), "lacks completed recovery proof") {
		t.Fatalf("read-only reopen error = %v", err)
	}
}

func TestOpenEpochPurposeMustMatchDurableState(t *testing.T) {
	states := []struct {
		name    string
		phase   OptimizerPhase
		reason  MonitorReason
		purpose EpochPurpose
		windows int
	}{
		{"baseline", PhaseBaseline, "", EpochBaseline, 2},
		{"undervolt", PhaseUndervolt, "", EpochTrial, 2},
		{"frequency", PhaseFrequencyTest, "", EpochTrial, 2},
		{"voltage", PhaseVoltageTest, "", EpochTrial, 2},
		{"manual monitor", PhaseMonitor, MonitorManual, EpochMonitor, 2},
		{"selected monitor", PhaseMonitor, MonitorSelected, EpochMonitor, 2},
		{"rejected monitor", PhaseMonitor, MonitorRejected, EpochMonitor, 2},
		{"starved monitor", PhaseMonitor, MonitorStarved, EpochMonitor, 2},
		{"off-grid monitor", PhaseMonitor, MonitorOffGrid, EpochMonitor, 2},
		{"cooldown", PhaseCooldown, "", EpochSafetyValidation, 1},
	}
	for _, testCase := range states {
		t.Run(testCase.name, func(t *testing.T) {
			state := MinerState{MacAddr: testMAC, Phase: testCase.phase, MonitorReason: testCase.reason,
				CurrentFrequency: 400, CurrentCoreVoltage: 1000}
			epoch := &EvidenceEpoch{MacAddr: testMAC, Point: state.CurrentPoint(),
				Purpose: testCase.purpose, RequiredWindows: testCase.windows}
			if err := validateOpenEpochState(state, epoch, false, false); err != nil {
				t.Fatalf("canonical shape: %v", err)
			}
			epoch.RequiredWindows = 3 - testCase.windows
			if err := validateOpenEpochState(state, epoch, false, false); err == nil {
				t.Fatal("wrong required-window count was accepted")
			}
			epoch.RequiredWindows = testCase.windows
			epoch.Purpose = EpochBaseline
			if testCase.purpose == EpochBaseline {
				epoch.Purpose = EpochSafetyValidation
			}
			if err := validateOpenEpochState(state, epoch, false, false); err == nil {
				t.Fatal("wrong purpose was accepted")
			}
		})
	}
	for _, state := range []MinerState{
		{MacAddr: testMAC, Phase: PhaseEmergency, CurrentFrequency: 400, CurrentCoreVoltage: 1000},
	} {
		epoch := &EvidenceEpoch{MacAddr: testMAC, Point: state.CurrentPoint(), Purpose: EpochBaseline, RequiredWindows: 2}
		if err := validateOpenEpochState(state, epoch, false, false); err == nil {
			t.Fatalf("state %s/%s accepted an open epoch", state.Phase, state.MonitorReason)
		}
	}
}

func TestMutationMilestonesAndAtomicResume(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	state = closeInitialBaselineEpoch(t, store, state, now)
	target := OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	state.Phase = PhaseBaseline
	state.MonitorReason = ""
	state.MonitorReferenceEpochID = 0
	state.SettledAt = time.Time{}
	state.SetPendingMutation(MutationOperatingPoint, target, now)
	if _, err := store.Apply(SaveState{State: state}, now); err != nil {
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
	state.Phase = PhaseMonitor
	state.MonitorReason = MonitorRejected
	state.SetFallbackPoint(OperatingPoint{})
	completeResult, err := store.Apply(CompleteMutation{
		MacAddr: state.MacAddr, IP: state.IP, AttemptID: id,
	}, now.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	state = completeResult.State
	loaded, err := store.LoadMiner(testMAC)
	if err != nil || loaded.Phase != PhaseBaseline || loaded.MonitorReason != "" || loaded.CurrentPoint() != target {
		t.Fatalf("completion was not store-derived: %+v, %v", loaded, err)
	}
	if err := store.RecordFirstPositive(id, now.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	state, err = store.LoadMiner(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	resumeResult, err := store.Apply(CompleteResume{
		MacAddr: state.MacAddr, AttemptID: id,
	}, now.Add(7*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	state = resumeResult.State
	attempts, err := store.ListMutationAttempts(testMAC)
	if err != nil || len(attempts) != 1 || attempts[0].MiningResumedAt.IsZero() {
		t.Fatalf("resumed attempt = %+v, %v", attempts, err)
	}
	if _, ok, err := store.PendingMutationResume(testMAC); err != nil || ok {
		t.Fatalf("pending resume = %t, %v", ok, err)
	}
}

func TestMiningConfigurationRestartReopensInterruptedNormalEpoch(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	interrupted := mustOpenEpoch(t, store, testMAC)
	state.MiningPending = true
	result, err := store.Apply(SaveState{State: state}, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	state = result.State
	attempt := MutationAttempt{
		MacAddr: state.MacAddr, Kind: MutationMiningConfiguration,
		FromFrequency: state.CurrentFrequency, FromCoreVoltage: state.CurrentCoreVoltage,
		IntentCreatedAt: now.Add(time.Second), StartedAt: now.Add(time.Second),
		ConfiguredVerifiedUptimeSeconds: -1,
	}
	id, err := store.StartMutationAttempt(&attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, MutationMilestonePatchRequested, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordConfiguredVerification(id, now.Add(3*time.Second), 101); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, MutationMilestoneRestartRequested, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceMutationAttempt(id, MutationMilestoneRebootVerified, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	result, err = store.Apply(CompleteMutation{
		MacAddr: state.MacAddr, IP: state.IP, AttemptID: id,
	}, now.Add(6*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	state = result.State
	if _, open, err := store.OpenEvidenceEpochFor(testMAC); err != nil || open {
		t.Fatalf("completed restart retained stale epoch: open=%t err=%v", open, err)
	}
	if err := store.RecordFirstPositive(id, now.Add(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	result, err = store.Apply(CompleteResume{MacAddr: testMAC, AttemptID: id}, now.Add(8*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	state = result.State
	resumed := mustOpenEpoch(t, store, testMAC)
	if resumed.ID == interrupted.ID || resumed.Purpose != EpochBaseline || resumed.RequiredWindows != 2 ||
		resumed.Point != state.CurrentPoint() || state.MiningPending || state.Phase != PhaseBaseline {
		t.Fatalf("mining restart resume = state:%+v epoch:%+v", state, resumed)
	}
}

func TestStartMutationAttemptRequiresIntentTimestampMatch(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	state = closeInitialBaselineEpoch(t, store, state, now)
	target := OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	state.SetPendingMutation(MutationOperatingPoint, target, now)
	if _, err := store.Apply(SaveState{State: state}, now); err != nil {
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

func TestSafetyTransitionSupersedesChangedSafetyAttempt(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	state = closeInitialBaselineEpoch(t, store, state, now)
	minimum := OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	state.Phase = PhaseCooldown
	state.MonitorReason = ""
	state.MonitorReferenceEpochID = 0
	state.SettledAt = time.Time{}
	state.SafetyReason = SafetyReasonASICLimit
	state.SetPendingMutation(MutationSafetyRollback, minimum, now)
	if _, err := store.Apply(SaveState{State: state}, now); err != nil {
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
	state.Phase = PhaseEmergency
	state.SafetyReason = SafetyReasonFirmwareOverheat
	state.SetPendingMutation(MutationFirmwareRecovery, minimum, now.Add(time.Minute))
	transitionAt := now.Add(2 * time.Minute)
	transitionResult, err := store.Apply(SafetyTransition{Expected: expected, State: state, Record: nil}, transitionAt)
	if err != nil {
		t.Fatal(err)
	}
	attempts, err := store.ListMutationAttempts(testMAC)
	if err != nil || len(attempts) != 1 || attempts[0].ID != attemptID ||
		attempts[0].FailureStage != MutationFailureSafetySuperseded {
		t.Fatalf("superseded safety attempt = %+v, %v", attempts, err)
	}
	if !attempts[0].FailedAt.Equal(transitionAt) {
		t.Fatalf("superseded safety timestamp = %s, want %s", attempts[0].FailedAt, transitionAt)
	}

	// A returning worker may observe the same supersession after SafetyTransition committed it.
	// That is one terminal fact, even though the worker's observation has a later timestamp and a
	// now-obsolete proposed state.
	durable := transitionResult.State
	lateState := durable
	lateState.ClearPendingMutation()
	lateResult, err := store.Apply(SupersedeMutation{
		Expected: durable, State: lateState, AttemptID: attemptID,
	}, now.Add(3*time.Minute))
	if err != nil {
		t.Fatalf("late convergent supersession: %v", err)
	}
	if lateResult.State != durable {
		t.Fatalf("late supersession rewrote durable state: got %+v want %+v", lateResult.State, durable)
	}
	attempts, err = store.ListMutationAttempts(testMAC)
	if err != nil || len(attempts) != 1 || !attempts[0].FailedAt.Equal(transitionAt) {
		t.Fatalf("late supersession rewrote the first terminal timestamp: %+v, %v", attempts, err)
	}
}

func TestSafetyTransitionSupersedesReissuedSafetyAuthority(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	state = closeInitialBaselineEpoch(t, store, state, now)
	minimum := OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	state.Phase = PhaseCooldown
	state.MonitorReason = ""
	state.MonitorReferenceEpochID = 0
	state.SettledAt = time.Time{}
	state.SafetyReason = SafetyReasonASICLimit
	state.SetPendingMutation(MutationSafetyRollback, minimum, now)
	if result, err := store.Apply(SaveState{State: state}, now); err != nil {
		t.Fatal(err)
	} else {
		state = result.State
	}
	attempt := MutationAttempt{
		MacAddr: testMAC, Kind: MutationSafetyRollback, Reason: state.SafetyReason,
		FromFrequency: state.CurrentFrequency, FromCoreVoltage: state.CurrentCoreVoltage,
		TargetFrequency: minimum.Frequency, TargetCoreVoltage: minimum.CoreVoltage,
		IntentCreatedAt: state.PendingSince, StartedAt: now, ConfiguredVerifiedUptimeSeconds: -1,
	}
	id, err := store.StartMutationAttempt(&attempt)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := store.LoadMiner(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	reissued := expected
	reissued.SetPendingMutation(MutationSafetyRollback, minimum, now.Add(time.Minute))
	if _, err := store.Apply(SafetyTransition{Expected: expected, State: reissued}, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	attempts, err := store.ListMutationAttempts(testMAC)
	if err != nil || len(attempts) != 1 || attempts[0].ID != id ||
		attempts[0].FailureStage != MutationFailureSafetySuperseded {
		t.Fatalf("reissued authority did not supersede its old attempt: %+v, %v", attempts, err)
	}
}

func TestFirmwareRecoveryRequiresFirmwareCause(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	state = closeInitialBaselineEpoch(t, store, state, now)
	state.Phase = PhaseEmergency
	state.SafetyReason = SafetyReasonTelemetryUnavailable
	state.SetPendingMutation(MutationFirmwareRecovery, OperatingPoint{Frequency: 400, CoreVoltage: 1000}, now)
	if _, err := store.Apply(SaveState{State: state}, now); err == nil {
		t.Fatal("firmware recovery without a firmware-overheat cause was accepted")
	}
}

func TestHourlyCursorCASAndGeneralSaveCannotRegressIt(t *testing.T) {
	store := openTestStore(t)
	_, now := bootstrapTestMiner(t, store)
	end := now.Add(90 * time.Minute)
	fragments := []HourlyAggregate{
		{MacAddr: testMAC, HourStartedAt: now.Truncate(time.Hour), ObservedDuration: 30 * time.Minute, UnknownGapDuration: 30 * time.Minute, ActualHashSeconds: 180000},
		{MacAddr: testMAC, HourStartedAt: now.Add(time.Hour).Truncate(time.Hour), UnknownGapDuration: 30 * time.Minute},
	}
	if err := store.CompareAndSetHourly(testMAC, now, end, fragments, end); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadMiner(testMAC)
	if err != nil || !loaded.AccountedThroughAt.Equal(end) {
		t.Fatalf("cursor = %v, %v", loaded.AccountedThroughAt, err)
	}
	loaded.AccountedThroughAt = now
	if _, err := store.Apply(SaveState{State: loaded}, now); err != nil {
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
		{MacAddr: testMAC, HourStartedAt: now, ObservedDuration: 30 * time.Minute, ActualHashSeconds: 180000},
	}, now); err == nil {
		t.Fatal("partial hourly coverage was accepted")
	}
}

func TestHourlyAggregateDurationUsesExactBounds(t *testing.T) {
	hour := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		aggregate HourlyAggregate
		wantErr   bool
	}{
		{name: "exact hour", aggregate: HourlyAggregate{HourStartedAt: hour, ObservedDuration: time.Hour}},
		{name: "exact partial hour", aggregate: HourlyAggregate{HourStartedAt: hour, ObservedDuration: 3527*time.Second + 352860000*time.Nanosecond, UnknownGapDuration: 72*time.Second + 647140000*time.Nanosecond}},
		{name: "duration overage", aggregate: HourlyAggregate{HourStartedAt: hour, ObservedDuration: time.Hour, UnknownGapDuration: time.Nanosecond}, wantErr: true},
		{name: "duration exceeds hour", aggregate: HourlyAggregate{HourStartedAt: hour, ObservedDuration: time.Hour + time.Nanosecond}, wantErr: true},
		{name: "settled exceeds observed", aggregate: HourlyAggregate{HourStartedAt: hour, ObservedDuration: time.Minute, SettledDuration: time.Minute + time.Nanosecond}, wantErr: true},
		{name: "trial exceeds observed", aggregate: HourlyAggregate{HourStartedAt: hour, ObservedDuration: time.Minute, TrialDuration: time.Minute + time.Nanosecond}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateHourlyAggregate(test.aggregate)
			if (err != nil) != test.wantErr {
				t.Fatalf("aggregate %+v error = %v, want error: %t", test.aggregate, err, test.wantErr)
			}
		})
	}
}

func TestHourlyCursorUsesExactDurationClosure(t *testing.T) {
	store := openTestStore(t)
	hour := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	cursor := hour.Add(3527*time.Second + 352860000*time.Nanosecond)
	end := hour.Add(time.Hour)
	observed := cursor.Sub(hour)
	if result, err := store.Apply(Bootstrap{
		Info: testInfo(), IP: testIP, PairAdvertised: true,
	}, cursor); err != nil || !result.Created {
		t.Fatalf("bootstrap = created:%t err:%v", result.Created, err)
	}
	if _, err := store.conn.ExecContext(context.Background(), `INSERT INTO optimizer_hourly (
		mac_addr, hour_started_at, observed_duration_nanos, unknown_gap_duration_nanos,
		actual_hash_seconds, trial_actual_hash_seconds,
		incumbent_counterfactual_hash_seconds, settled_duration_nanos, trial_duration_nanos
	) VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0)`,
		testMAC, hour.Unix(), observed.Nanoseconds()); err != nil {
		t.Fatal(err)
	}
	if err := store.CompareAndSetHourly(testMAC, cursor, end, []HourlyAggregate{
		{MacAddr: testMAC, HourStartedAt: hour, UnknownGapDuration: end.Sub(cursor)},
	}, end); err != nil {
		t.Fatalf("rounded hour closure: %v", err)
	}
	state, err := store.LoadMiner(testMAC)
	if err != nil || !state.AccountedThroughAt.Equal(end) {
		t.Fatalf("cursor after rounded closure = %v, %v", state.AccountedThroughAt, err)
	}
	rows, err := store.ListHourly(testMAC, hour, end.Add(time.Hour))
	if err != nil || len(rows) != 1 {
		t.Fatalf("exact closure rows = %+v, %v", rows, err)
	}
	if rows[0].ObservedDuration != observed || rows[0].UnknownGapDuration != end.Sub(cursor) ||
		rows[0].ObservedDuration+rows[0].UnknownGapDuration != time.Hour {
		t.Fatalf("exact closure durations = %+v", rows[0])
	}
}

func TestHourlyRetentionKeepsBucketWhoseEndIsAfterBoundary(t *testing.T) {
	store := openTestStore(t)
	_, now := bootstrapTestMiner(t, store)
	boundaryHour := now.Add(-LongTermRetentionHours * time.Hour)
	if _, err := store.conn.ExecContext(context.Background(), `INSERT INTO optimizer_hourly (
		mac_addr, hour_started_at, observed_duration_nanos, unknown_gap_duration_nanos,
		actual_hash_seconds, trial_actual_hash_seconds,
		incumbent_counterfactual_hash_seconds, settled_duration_nanos, trial_duration_nanos
	) VALUES (?, ?, ?, 0, 360000, 0, 0, 0, 0)`, testMAC, boundaryHour.Unix(), time.Hour.Nanoseconds()); err != nil {
		t.Fatal(err)
	}
	if err := store.CompareAndSetHourly(testMAC, now, now.Add(time.Hour), []HourlyAggregate{
		{MacAddr: testMAC, HourStartedAt: now, UnknownGapDuration: time.Hour},
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

// TestApplyIsExhaustiveOverTransitionVariants enumerates every Transition variant with a
// zero-valued literal and asserts Apply's switch reaches a domain validation error for each one
// rather than falling through to the "unhandled %T" backstop. This is the enforcement point for
// the closed transition set: it fails when a variant is added to the set but forgotten in the
// switch.
func TestApplyIsExhaustiveOverTransitionVariants(t *testing.T) {
	store := openTestStore(t)
	at := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	transitions := []Transition{
		Bootstrap{},
		StartPass{},
		CompleteMonitor{},
		FinalizeTrial{},
		CompleteBaseline{},
		AdoptManualPoint{},
		AdoptExternalPoint{},
		SaveState{},
		CompleteResume{},
		ResumePassAfterSafety{},
		SafetyTransition{},
		FailMutation{},
		FailMutationFinalizeTrial{},
		SupersedeMutation{},
		CompleteMutation{},
		OpenEpoch{},
		AdvanceEpoch{},
		CloseEpoch{},
	}
	for _, transition := range transitions {
		_, err := store.Apply(transition, at)
		if err == nil {
			t.Fatalf("%T: zero-valued transition unexpectedly succeeded", transition)
		}
		if strings.Contains(err.Error(), "unhandled") {
			t.Fatalf("%T: Apply did not route to a case for this transition: %v", transition, err)
		}
	}
}

// TestApplyRollsBackCompletelyOnSecondWriteFailure forces the second of CompleteMutation's two
// writes to fail after the first write has already executed inside the same transaction, and
// asserts neither the miner row nor the mutation-attempt row changed. This is the guarantee Apply's
// single BeginTx/Commit exists to provide.
func TestApplyRollsBackCompletelyOnSecondWriteFailure(t *testing.T) {
	store := openTestStore(t)
	state, now := bootstrapTestMiner(t, store)
	state = closeInitialBaselineEpoch(t, store, state, now)
	target := OperatingPoint{Frequency: 525, CoreVoltage: 1100}
	state.SetPendingMutation(MutationOperatingPoint, target, now)
	if _, err := store.Apply(SaveState{State: state}, now); err != nil {
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
	if err := store.AdvanceMutationAttempt(id, MutationMilestonePatchRequested, now.Add(time.Second)); err != nil {
		t.Fatal(err)
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

	beforeMiner, err := store.LoadMiner(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	beforeAttempts, err := store.ListMutationAttempts(testMAC)
	if err != nil {
		t.Fatal(err)
	}

	// A trigger that aborts specifically the completed_at UPDATE lets the miner-row write earlier
	// in the same transaction execute normally before the second write fails, exercising rollback
	// of an already-applied write rather than merely a pre-write validation error.
	if _, err := store.conn.ExecContext(context.Background(), `CREATE TRIGGER force_complete_mutation_failure
		BEFORE UPDATE OF completed_at ON mutation_attempts
		BEGIN SELECT RAISE(ABORT, 'forced failure for rollback test'); END`); err != nil {
		t.Fatal(err)
	}

	completedAt := now.Add(5 * time.Second)
	if _, err := store.Apply(CompleteMutation{
		MacAddr: state.MacAddr, IP: state.IP, AttemptID: id,
	}, completedAt); err == nil {
		t.Fatal("completion succeeded despite forced second-write failure")
	}

	afterMiner, err := store.LoadMiner(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	if afterMiner != beforeMiner {
		t.Fatalf("miner row changed despite rollback: before=%+v after=%+v", beforeMiner, afterMiner)
	}
	afterAttempts, err := store.ListMutationAttempts(testMAC)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterAttempts) != len(beforeAttempts) || len(afterAttempts) != 1 || afterAttempts[0] != beforeAttempts[0] {
		t.Fatalf("attempt row changed despite rollback: before=%+v after=%+v", beforeAttempts, afterAttempts)
	}
}
