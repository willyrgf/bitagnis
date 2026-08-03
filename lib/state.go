package lib

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
)

const optimizerSchemaVersion = 5

// LongTermRetentionHours bounds the credential-free hourly accounting history.
const LongTermRetentionHours = 384

// ErrAccountingCursorChanged means another serialized accounting transaction
// advanced the durable cursor after the caller loaded it.
var ErrAccountingCursorChanged = errors.New("optimizer hourly accounting cursor changed")

type OptimizerPhase string

const (
	PhaseBaseline      OptimizerPhase = "BASELINE"
	PhaseUndervolt     OptimizerPhase = "UNDERVOLT"
	PhaseFrequencyTest OptimizerPhase = "FREQ_TEST"
	PhaseVoltageTest   OptimizerPhase = "VOLT_TEST"
	PhaseHold          OptimizerPhase = "HOLD"
	PhaseCooldown      OptimizerPhase = "COOLDOWN"
	PhaseOverheat      OptimizerPhase = "OVERHEAT"
)

const (
	PointEntered      = "entered"
	PointValidated    = "validated"
	PointUnstable     = "unstable"
	PointNoGain       = "no_gain"
	PointUnobservable = "unobservable"
	PointThermal      = "thermal"
	PointPower        = "power"
	PointVRHot        = "vr_hot"
)

// PassTrigger identifies the event that created a finite optimization pass.
type PassTrigger string

const (
	PassInitial  PassTrigger = "initial"
	PassOperator PassTrigger = "operator"
)

// HoldReason describes why normal optimization is not selecting more work.
type HoldReason string

const (
	HoldOptimized HoldReason = "optimized"
	HoldSafety    HoldReason = "safety"
	HoldManual    HoldReason = "manual"
	HoldBlocked   HoldReason = "blocked"
)

// SafetyReason is the durable cause of a safety episode. It is deliberately
// finite so it can be audited without retaining raw telemetry or free-form
// device errors.
type SafetyReason string

const (
	SafetyReasonASICLimit         SafetyReason = "asic_limit"
	SafetyReasonHostCutoff        SafetyReason = "host_cutoff"
	SafetyReasonFirmwareOverheat  SafetyReason = "firmware_overheat"
	SafetyReasonFirmwareTrip      SafetyReason = "firmware_trip"
	SafetyReasonPowerLimit        SafetyReason = "power_limit"
	SafetyReasonVRLimit           SafetyReason = "vr_limit"
	SafetyReasonMutationUncertain SafetyReason = "mutation_uncertain"
)

// MutationKind identifies the durable hardware mutation represented by a
// pending operating-point pair.
type MutationKind string

const (
	MutationOperatingPoint      MutationKind = "operating_point"
	MutationSafetyRollback      MutationKind = "safety_rollback"
	MutationOverheatRecovery    MutationKind = "overheat_recovery"
	MutationMiningConfiguration MutationKind = "mining_configuration"
)

// MinerState contains only durable optimizer control and mutation-recovery
// state. Telemetry samples and resolved credentials remain in memory.
type MinerState struct {
	MacAddr  string
	Hostname string
	IP       string

	Phase          OptimizerPhase
	PhaseStartedAt time.Time
	RampUntil      time.Time

	CurrentFrequency   int
	CurrentCoreVoltage int

	BestFrequency   int
	BestCoreVoltage int
	BestHashRate    float64

	FallbackFrequency   int
	FallbackCoreVoltage int

	PendingKind        MutationKind
	PendingFrequency   int
	PendingCoreVoltage int
	PendingSince       time.Time
	MiningPending      bool

	ObservedFrequency   int
	ObservedCoreVoltage int
	ObservedCount       int

	ConsecutiveBadWindows int
	OverheatCount         int
	CooldownUntil         time.Time

	PassStartedAt            time.Time
	PassTrigger              PassTrigger
	PassReferenceHash        float64
	PassReferenceFrequency   int
	PassReferenceCoreVoltage int
	PassReferenceSettledAt   time.Time
	SafetyReason             SafetyReason
	HoldReason               HoldReason
	SettledAt                time.Time
	EvidenceDeadlineAt       time.Time
	AccountedThroughAt       time.Time
}

func (state MinerState) CurrentPoint() OperatingPoint {
	return OperatingPoint{
		Frequency:   state.CurrentFrequency,
		CoreVoltage: state.CurrentCoreVoltage,
	}
}

func (state MinerState) BestPoint() OperatingPoint {
	return OperatingPoint{
		Frequency:   state.BestFrequency,
		CoreVoltage: state.BestCoreVoltage,
	}
}

func (state MinerState) FallbackPoint() OperatingPoint {
	return OperatingPoint{
		Frequency:   state.FallbackFrequency,
		CoreVoltage: state.FallbackCoreVoltage,
	}
}

func (state MinerState) PendingPoint() OperatingPoint {
	return OperatingPoint{
		Frequency:   state.PendingFrequency,
		CoreVoltage: state.PendingCoreVoltage,
	}
}

func (state *MinerState) SetCurrentPoint(point OperatingPoint) {
	state.CurrentFrequency = point.Frequency
	state.CurrentCoreVoltage = point.CoreVoltage
}

func (state *MinerState) SetBestPoint(point OperatingPoint) {
	state.BestFrequency = point.Frequency
	state.BestCoreVoltage = point.CoreVoltage
}

func (state *MinerState) SetFallbackPoint(point OperatingPoint) {
	state.FallbackFrequency = point.Frequency
	state.FallbackCoreVoltage = point.CoreVoltage
}

// SetPendingMutation records one complete operating-point intent.
func (state *MinerState) SetPendingMutation(
	kind MutationKind,
	point OperatingPoint,
	since time.Time,
) {
	state.PendingKind = kind
	state.PendingFrequency = point.Frequency
	state.PendingCoreVoltage = point.CoreVoltage
	state.PendingSince = since
	state.EvidenceDeadlineAt = time.Time{}
}

// ClearPendingMutation clears only the operating-point mutation; an independent
// mining obligation remains intact.
func (state *MinerState) ClearPendingMutation() {
	state.PendingKind = ""
	state.PendingFrequency = 0
	state.PendingCoreVoltage = 0
	state.PendingSince = time.Time{}
}

type OperatingPointRecord struct {
	MacAddr        string
	Frequency      int
	CoreVoltage    int
	Status         string
	MedianHash     float64
	ExpectedHash   float64
	Attainment     float64
	MeanTemp       float64
	P95Temp        float64
	P95VRTemp      float64
	P95Power       float64
	ErrorPercent   *float64
	AcceptedDelta  uint64
	RejectedDelta  uint64
	MeasuredAt     time.Time
	EnteredAt      time.Time
	EntryAttemptID int64
	ReferenceHash  float64
}

func (record OperatingPointRecord) Point() OperatingPoint {
	return OperatingPoint{
		Frequency:   record.Frequency,
		CoreVoltage: record.CoreVoltage,
	}
}

// MutationMilestone identifies one durable stage in a controller-owned
// hardware mutation attempt.
type MutationMilestone string

const (
	MutationMilestonePatchRequested     MutationMilestone = "patch_requested"
	MutationMilestoneConfiguredVerified MutationMilestone = "configured_verified"
	MutationMilestoneRestartRequested   MutationMilestone = "restart_requested"
	MutationMilestoneRebootVerified     MutationMilestone = "reboot_verified"
	MutationMilestoneFirstPositive      MutationMilestone = "first_positive"
	MutationMilestoneMiningResumed      MutationMilestone = "mining_resumed"
)

// MutationFailureStage identifies the stage at which a mutation attempt
// stopped without confirmed healthy mining.
type MutationFailureStage string

const (
	MutationFailurePreflight              MutationFailureStage = "preflight"
	MutationFailureConfiguredVerification MutationFailureStage = "configured_verification"
	MutationFailureRebootVerification     MutationFailureStage = "reboot_verification"
	MutationFailureMiningResume           MutationFailureStage = "mining_resume"
	MutationFailureSafetySuperseded       MutationFailureStage = "safety_superseded"
)

// MutationAttempt is one durable controller-owned hardware mutation
// lifecycle. It records only lifecycle summaries, never credentials or raw
// telemetry.
type MutationAttempt struct {
	ID      int64
	MacAddr string
	Kind    MutationKind
	Reason  SafetyReason

	FromFrequency     int
	FromCoreVoltage   int
	TargetFrequency   int
	TargetCoreVoltage int

	IntentCreatedAt                 time.Time
	StartedAt                       time.Time
	PatchRequestedAt                time.Time
	ConfiguredVerifiedAt            time.Time
	ConfiguredVerifiedUptimeSeconds int
	RestartRequestedAt              time.Time
	RebootVerifiedAt                time.Time
	CompletedAt                     time.Time
	FirstPositiveAt                 time.Time
	MiningResumedAt                 time.Time
	FailedAt                        time.Time
	FailureStage                    MutationFailureStage
}

// FromPoint returns the configured operating point observed before mutation.
func (attempt MutationAttempt) FromPoint() OperatingPoint {
	return OperatingPoint{
		Frequency:   attempt.FromFrequency,
		CoreVoltage: attempt.FromCoreVoltage,
	}
}

// TargetPoint returns the complete requested operating point. Mining-only
// attempts return the zero point because their operating point is unchanged.
func (attempt MutationAttempt) TargetPoint() OperatingPoint {
	return OperatingPoint{
		Frequency:   attempt.TargetFrequency,
		CoreVoltage: attempt.TargetCoreVoltage,
	}
}

// OptimizerStore owns one pinned SQLite connection and keeps its exclusive
// locking mode for the store lifetime. All access is serialized through mu.
type OptimizerStore struct {
	mu     sync.Mutex
	db     *sql.DB
	conn   *sql.Conn
	closed bool
}

func OpenOptimizerStore(path string) (*OptimizerStore, error) {
	return openOptimizerStore(path, false)
}

// OpenOptimizerStoreReadOnly opens an existing schema-v5 database without
// creating, migrating, locking, or otherwise mutating it. Report mode uses
// this path so a missing or incompatible database fails rather than changing
// durable state.
func OpenOptimizerStoreReadOnly(path string) (*OptimizerStore, error) {
	return openOptimizerStore(path, true)
}

func openOptimizerStore(path string, readOnly bool) (*OptimizerStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("open optimizer database: path cannot be empty")
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("open optimizer database: resolve path: %w", err)
	}

	database, err := sql.Open("sqlite3", optimizerSQLiteDSN(absolutePath, readOnly))
	if err != nil {
		return nil, fmt.Errorf("open optimizer database: %w", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)

	closeDatabase := true
	defer func() {
		if closeDatabase {
			_ = database.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := database.Conn(ctx)
	if err != nil {
		if sqliteBusy(err) {
			return nil, fmt.Errorf("open optimizer database: database is already in use")
		}
		return nil, fmt.Errorf("open optimizer database connection: %w", err)
	}
	closeConnection := true
	defer func() {
		if closeConnection {
			_ = conn.Close()
		}
	}()

	if _, err := conn.ExecContext(ctx, "PRAGMA busy_timeout = 1000"); err != nil {
		return nil, fmt.Errorf("configure optimizer database timeout: %w", err)
	}
	if readOnly {
		if _, err := conn.ExecContext(ctx, "PRAGMA query_only = ON"); err != nil {
			return nil, fmt.Errorf("configure read-only optimizer database: %w", err)
		}
	} else {
		if _, err := conn.ExecContext(ctx, "PRAGMA locking_mode = EXCLUSIVE"); err != nil {
			return nil, fmt.Errorf("configure exclusive optimizer database ownership: %w", err)
		}
		if _, err := conn.ExecContext(ctx, "BEGIN EXCLUSIVE"); err != nil {
			if sqliteBusy(err) {
				return nil, fmt.Errorf("open optimizer database: database is already in use")
			}
			return nil, fmt.Errorf("acquire exclusive optimizer database ownership: %w", err)
		}
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			return nil, fmt.Errorf("acquire exclusive optimizer database ownership: %w", err)
		}
	}

	if !readOnly {
		if err := ensureOptimizerSchema(ctx, conn); err != nil {
			return nil, err
		}
	} else {
		version, err := schemaVersion(ctx, conn)
		if err != nil {
			return nil, fmt.Errorf("inspect optimizer schema version: %w", err)
		}
		if version != optimizerSchemaVersion {
			return nil, incompatibleSchema(version)
		}
		if err := validateOptimizerSchema(ctx, conn); err != nil {
			return nil, fmt.Errorf("validate optimizer schema: %w", err)
		}
		if err := validateOptimizerIndexes(ctx, conn); err != nil {
			return nil, fmt.Errorf("validate optimizer indexes: %w", err)
		}
	}
	if err := validateStoredOptimizerData(ctx, conn); err != nil {
		return nil, fmt.Errorf("validate optimizer database contents: %w", err)
	}

	closeConnection = false
	closeDatabase = false
	return &OptimizerStore{db: database, conn: conn}, nil
}

func optimizerSQLiteDSN(path string, readOnly bool) string {
	query := "_busy_timeout=1000"
	if readOnly {
		query += "&mode=ro"
	}
	return (&url.URL{
		Scheme:   "file",
		Path:     path,
		RawQuery: query,
	}).String()
}

// BootstrapMiner is the canonical new-miner load path. Creation of the
// optimizer row and its baseline entered row is one transaction with a fixed
// evidence deadline; a process reopen cannot observe a miner without its pass
// authority.
func (store *OptimizerStore) BootstrapMiner(
	info Info,
	ip string,
	now time.Time,
	rampUp time.Duration,
	evaluationWindow time.Duration,
	pairAdvertised bool,
) (MinerState, bool, error) {
	if strings.TrimSpace(info.Hostname) == "" || strings.TrimSpace(info.MacAddr) == "" {
		return MinerState{}, false, fmt.Errorf("bootstrap miner: hostname and MAC address are required")
	}
	if net.ParseIP(ip) == nil || net.ParseIP(ip).To4() == nil || now.IsZero() ||
		rampUp < 0 || evaluationWindow <= 0 {
		return MinerState{}, false, fmt.Errorf("bootstrap miner: invalid identity, timing, or IP")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("bootstrap miner"); err != nil {
		return MinerState{}, false, err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return MinerState{}, false, fmt.Errorf("bootstrap miner: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	state, err := queryMiner(tx, info.MacAddr)
	if err == nil {
		changed := state.Hostname != info.Hostname || state.IP != ip
		state.Hostname = info.Hostname
		state.IP = ip
		if changed {
			if err := saveMiner(tx, &state); err != nil {
				return MinerState{}, false, fmt.Errorf("bootstrap miner: update route: %w", err)
			}
		}
		if err := tx.Commit(); err != nil {
			return MinerState{}, false, fmt.Errorf("bootstrap miner: commit existing miner: %w", err)
		}
		rollback = false
		return state, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return MinerState{}, false, fmt.Errorf("bootstrap miner: load existing miner: %w", err)
	}
	passStartedAt := now.UTC()
	state = MinerState{
		MacAddr:            info.MacAddr,
		Hostname:           info.Hostname,
		IP:                 ip,
		Phase:              PhaseBaseline,
		PhaseStartedAt:     passStartedAt,
		PassStartedAt:      passStartedAt,
		PassTrigger:        PassInitial,
		RampUntil:          passStartedAt.Add(rampUp),
		EvidenceDeadlineAt: passStartedAt.Add(rampUp + 4*evaluationWindow),
		AccountedThroughAt: passStartedAt,
	}
	point := pointFromInfo(info)
	emergency := info.OverHeatMode != 0 || point.Frequency == 50
	if emergency {
		state.Phase = PhaseOverheat
		state.OverheatCount = 1
		state.SafetyReason = SafetyReasonFirmwareOverheat
		state.EvidenceDeadlineAt = time.Time{}
	} else if pairAdvertised && validCanonicalPoint(point) {
		state.SetCurrentPoint(point)
		state.SetBestPoint(OperatingPoint{})
	} else if validStoredPoint(point) {
		state.SetCurrentPoint(point)
		state.Phase = PhaseHold
		state.HoldReason = HoldBlocked
		state.EvidenceDeadlineAt = time.Time{}
	} else {
		state.Phase = PhaseHold
		state.HoldReason = HoldBlocked
		state.EvidenceDeadlineAt = time.Time{}
	}
	if err := saveMiner(tx, &state); err != nil {
		return MinerState{}, false, fmt.Errorf("bootstrap miner: save state: %w", err)
	}
	if !emergency && state.Phase == PhaseBaseline && validCanonicalPoint(point) {
		if err := insertPoint(tx, OperatingPointRecord{
			MacAddr:     info.MacAddr,
			Frequency:   point.Frequency,
			CoreVoltage: point.CoreVoltage,
			Status:      PointEntered,
			EnteredAt:   passStartedAt,
		}); err != nil {
			return MinerState{}, false, fmt.Errorf("bootstrap miner: insert baseline: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return MinerState{}, false, fmt.Errorf("bootstrap miner: commit new miner: %w", err)
	}
	rollback = false
	return state, true, nil
}

// ResetOptimizationPass atomically deletes the selected miner's prior point
// summaries and starts the one explicitly authorized operator pass. The
// mutation coordinator's lock is the qualification boundary: it holds the
// validated live observation while this serialized store transaction
// rechecks durable HOLD/current/attempt state. The mutation history and hourly
// accounting cursor remain untouched.
func (store *OptimizerStore) ResetOptimizationPass(
	macAddr string,
	point OperatingPoint,
	startedAt time.Time,
	rampUntil time.Time,
	evidenceDeadlineAt time.Time,
) error {
	return store.resetOptimizationPass(
		macAddr,
		point,
		PassOperator,
		startedAt,
		rampUntil,
		evidenceDeadlineAt,
	)
}

func (store *OptimizerStore) resetOptimizationPass(
	macAddr string,
	point OperatingPoint,
	trigger PassTrigger,
	startedAt time.Time,
	rampUntil time.Time,
	evidenceDeadlineAt time.Time,
) error {
	if strings.TrimSpace(macAddr) == "" || !validCanonicalPoint(point) {
		return fmt.Errorf("start optimization pass: invalid miner or operating point")
	}
	if !validPassTrigger(trigger) || startedAt.IsZero() || rampUntil.IsZero() ||
		evidenceDeadlineAt.IsZero() || evidenceDeadlineAt.Before(rampUntil) {
		return fmt.Errorf("start optimization pass: invalid pass timing or reference")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("start optimization pass"); err != nil {
		return err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("start optimization pass: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	state, err := queryMiner(tx, macAddr)
	if err != nil {
		return fmt.Errorf("start optimization pass: load miner: %w", err)
	}
	if state.PendingKind != "" || state.MiningPending {
		return fmt.Errorf("start optimization pass: miner has pending mutation work")
	}
	if state.Phase != PhaseHold || state.HoldReason == HoldBlocked || state.SettledAt.IsZero() {
		return fmt.Errorf("start optimization pass: miner is not settled in HOLD")
	}
	if state.CurrentPoint() != point {
		return fmt.Errorf("start optimization pass: current point changed")
	}
	passReferenceHash := 0.0
	passReferencePoint := OperatingPoint{}
	passReferenceSettledAt := time.Time{}
	var unfinished int
	if err := tx.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM mutation_attempts
		 WHERE mac_addr = ? AND failed_at = 0 AND mining_resumed_at = 0`, macAddr).Scan(&unfinished); err != nil {
		return fmt.Errorf("start optimization pass: check mutation attempts: %w", err)
	}
	if unfinished != 0 {
		return fmt.Errorf("start optimization pass: unfinished mutation attempt exists")
	}
	if state.HoldReason == HoldOptimized {
		var status string
		var medianHash float64
		err := tx.QueryRowContext(context.Background(), `SELECT status, median_hash
			FROM operating_points WHERE mac_addr = ? AND frequency = ? AND core_voltage = ?`,
			macAddr, point.Frequency, point.CoreVoltage).Scan(&status, &medianHash)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("start optimization pass: optimized hold has no selected point")
		}
		if err != nil {
			return fmt.Errorf("start optimization pass: load selected point: %w", err)
		}
		if status != PointValidated || !finite(medianHash) || medianHash <= 0 {
			return fmt.Errorf("start optimization pass: optimized hold has no validated current selection")
		}
		passReferenceHash = medianHash
		passReferencePoint = point
		passReferenceSettledAt = state.SettledAt
	}
	if _, err := tx.ExecContext(context.Background(),
		"DELETE FROM operating_points WHERE mac_addr = ?", macAddr); err != nil {
		return fmt.Errorf("start optimization pass: delete point history: %w", err)
	}
	state.SetCurrentPoint(point)
	state.SetBestPoint(OperatingPoint{})
	state.BestHashRate = 0
	state.SetFallbackPoint(OperatingPoint{})
	state.ClearPendingMutation()
	state.ObservedFrequency = 0
	state.ObservedCoreVoltage = 0
	state.ObservedCount = 0
	state.ConsecutiveBadWindows = 0
	state.PassStartedAt = startedAt
	state.PassTrigger = trigger
	state.PassReferenceHash = passReferenceHash
	state.PassReferenceFrequency = passReferencePoint.Frequency
	state.PassReferenceCoreVoltage = passReferencePoint.CoreVoltage
	state.PassReferenceSettledAt = passReferenceSettledAt
	state.SafetyReason = ""
	state.HoldReason = ""
	state.SettledAt = time.Time{}
	state.EvidenceDeadlineAt = evidenceDeadlineAt
	state.Phase = PhaseBaseline
	state.PhaseStartedAt = startedAt
	state.RampUntil = rampUntil
	if err := saveMinerForPassReset(tx, &state); err != nil {
		return fmt.Errorf("start optimization pass: save miner: %w", err)
	}
	baseline := OperatingPointRecord{
		MacAddr:     macAddr,
		Frequency:   point.Frequency,
		CoreVoltage: point.CoreVoltage,
		Status:      PointEntered,
		EnteredAt:   startedAt,
	}
	if err := insertPoint(tx, baseline); err != nil {
		return fmt.Errorf("start optimization pass: insert baseline: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("start optimization pass: commit: %w", err)
	}
	rollback = false
	return nil
}

// TrialDecision is the one atomic state transition after a candidate window.
type TrialDecision string

const (
	TrialPromote TrialDecision = "promote"
	TrialReturn  TrialDecision = "return"
	TrialBlock   TrialDecision = "block"
)

// AdmitTrial consumes a previously unseen candidate and persists its pending
// hardware intent, entry attempt, fallback incumbent, and trial phase in one
// transaction. The row insertion is the eligibility decision: a duplicate
// candidate cannot be admitted again in the same pass.
func (store *OptimizerStore) AdmitTrial(
	state *MinerState,
	candidate OperatingPoint,
	incumbent OperatingPoint,
	phase OptimizerPhase,
	referenceHash float64,
	enteredAt time.Time,
	evidenceDeadlineAt time.Time,
) (int64, error) {
	if state == nil {
		return 0, fmt.Errorf("admit trial: state is nil")
	}
	if !validCanonicalPoint(candidate) || !validCanonicalPoint(incumbent) || candidate == incumbent ||
		(phase != PhaseUndervolt && phase != PhaseFrequencyTest && phase != PhaseVoltageTest) ||
		!finite(referenceHash) || referenceHash <= 0 || enteredAt.IsZero() || evidenceDeadlineAt.IsZero() {
		return 0, fmt.Errorf("admit trial: invalid candidate, incumbent, phase, or evidence timing")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("admit trial"); err != nil {
		return 0, err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, fmt.Errorf("admit trial: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	durable, err := queryMiner(tx, state.MacAddr)
	if err != nil {
		return 0, fmt.Errorf("admit trial: load miner: %w", err)
	}
	if count, countErr := unfinishedMutationCount(tx, durable.MacAddr); countErr != nil {
		return 0, fmt.Errorf("admit trial: check mutation authority: %w", countErr)
	} else if count != 0 {
		return 0, fmt.Errorf("admit trial: unfinished mutation attempt exists")
	}
	if durable.CurrentPoint() != incumbent || durable.FallbackPoint() != (OperatingPoint{}) ||
		durable.PendingKind != "" || durable.Phase == PhaseOverheat ||
		durable.Phase == PhaseCooldown || durable.HoldReason != "" {
		return 0, fmt.Errorf("admit trial: durable incumbent is not established")
	}
	if !finite(durable.BestHashRate) || durable.BestHashRate <= 0 || referenceHash != durable.BestHashRate {
		return 0, fmt.Errorf("admit trial: frozen reference does not match durable pass maximum")
	}
	var exists int
	err = tx.QueryRowContext(context.Background(),
		`SELECT 1 FROM operating_points WHERE mac_addr = ? AND frequency = ? AND core_voltage = ?`,
		durable.MacAddr, candidate.Frequency, candidate.CoreVoltage).Scan(&exists)
	if err == nil {
		return 0, fmt.Errorf("admit trial: candidate was already entered")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("admit trial: check candidate: %w", err)
	}
	entry := OperatingPointRecord{
		MacAddr:       durable.MacAddr,
		Frequency:     candidate.Frequency,
		CoreVoltage:   candidate.CoreVoltage,
		Status:        PointEntered,
		EnteredAt:     enteredAt,
		ReferenceHash: referenceHash,
	}
	if err := insertPoint(tx, entry); err != nil {
		return 0, fmt.Errorf("admit trial: insert candidate: %w", err)
	}
	attempt := MutationAttempt{
		MacAddr:                         durable.MacAddr,
		Kind:                            MutationOperatingPoint,
		FromFrequency:                   incumbent.Frequency,
		FromCoreVoltage:                 incumbent.CoreVoltage,
		TargetFrequency:                 candidate.Frequency,
		TargetCoreVoltage:               candidate.CoreVoltage,
		IntentCreatedAt:                 enteredAt,
		StartedAt:                       enteredAt,
		ConfiguredVerifiedUptimeSeconds: -1,
	}
	if err := validateMutationAttempt(attempt, false); err != nil {
		return 0, fmt.Errorf("admit trial: validate attempt: %w", err)
	}
	result, err := tx.ExecContext(context.Background(),
		`INSERT INTO mutation_attempts (
			mac_addr, kind, reason, from_frequency, from_core_voltage,
			target_frequency, target_core_voltage, intent_created_at, started_at,
			patch_requested_at, configured_verified_at,
			configured_verified_uptime_seconds, restart_requested_at,
			reboot_verified_at, completed_at, first_positive_at, mining_resumed_at,
			failed_at, failure_stage
		) VALUES (?, ?, '', ?, ?, ?, ?, ?, ?, 0, 0, -1, 0, 0, 0, 0, 0, 0, '')`,
		attempt.MacAddr, attempt.Kind, attempt.FromFrequency, attempt.FromCoreVoltage,
		attempt.TargetFrequency, attempt.TargetCoreVoltage,
		timeValue(attempt.IntentCreatedAt), timeValue(attempt.StartedAt),
	)
	if err != nil {
		return 0, fmt.Errorf("admit trial: insert entry attempt: %w", err)
	}
	attemptID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("admit trial: read entry attempt ID: %w", err)
	}
	if _, err := tx.ExecContext(context.Background(),
		`UPDATE operating_points SET entry_attempt_id = ?
		 WHERE mac_addr = ? AND frequency = ? AND core_voltage = ? AND entry_attempt_id = 0`,
		attemptID, durable.MacAddr, candidate.Frequency, candidate.CoreVoltage); err != nil {
		return 0, fmt.Errorf("admit trial: bind entry attempt: %w", err)
	}
	durable.SetFallbackPoint(incumbent)
	durable.SetPendingMutation(MutationOperatingPoint, candidate, enteredAt)
	durable.Phase = phase
	durable.PhaseStartedAt = enteredAt
	durable.RampUntil = time.Time{}
	// A pending hardware mutation has no evidence deadline. The supplied
	// deadline is applied atomically when verified mining resumes.
	durable.EvidenceDeadlineAt = time.Time{}
	durable.SettledAt = time.Time{}
	durable.HoldReason = ""
	durable.ObservedFrequency = 0
	durable.ObservedCoreVoltage = 0
	durable.ObservedCount = 0
	durable.ConsecutiveBadWindows = 0
	if err := saveMiner(tx, &durable); err != nil {
		return 0, fmt.Errorf("admit trial: save miner: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("admit trial: commit: %w", err)
	}
	rollback = false
	*state = durable
	return attemptID, nil
}

// FinalizeTrial writes terminal evidence for an entered candidate and
// atomically either reserves its incumbent return, promotes the candidate, or
// blocks the pass.
func (store *OptimizerStore) FinalizeTrial(
	state *MinerState,
	record OperatingPointRecord,
	decision TrialDecision,
	now time.Time,
	rampUntil time.Time,
	evidenceDeadlineAt time.Time,
) error {
	if state == nil || record.Status == PointEntered || record.EntryAttemptID <= 0 || now.IsZero() {
		return fmt.Errorf("finalize trial: invalid state or terminal record")
	}
	if decision != TrialPromote && decision != TrialReturn && decision != TrialBlock {
		return fmt.Errorf("finalize trial: invalid decision %q", decision)
	}
	if err := validatePointRecord(record); err != nil {
		return fmt.Errorf("finalize trial: %w", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("finalize trial"); err != nil {
		return err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("finalize trial: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	durable, err := queryMiner(tx, state.MacAddr)
	if err != nil {
		return fmt.Errorf("finalize trial: load miner: %w", err)
	}
	attempt, err := queryMutationAttempt(tx, record.EntryAttemptID)
	if err != nil {
		return fmt.Errorf("finalize trial: load entry attempt: %w", err)
	}
	if attempt.MacAddr != durable.MacAddr || attempt.Kind != MutationOperatingPoint ||
		attempt.TargetPoint() != record.Point() || !attempt.FailedAt.IsZero() || attempt.MiningResumedAt.IsZero() {
		return fmt.Errorf("finalize trial: candidate has not completed healthy mutation resumption")
	}
	if durable.CurrentPoint() != state.CurrentPoint() ||
		durable.FallbackPoint() != state.FallbackPoint() {
		return fmt.Errorf("finalize trial: stale miner state")
	}
	if err := finalizeTrialTx(tx, &durable, record, decision, now, rampUntil, evidenceDeadlineAt); err != nil {
		return fmt.Errorf("finalize trial: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("finalize trial: commit: %w", err)
	}
	rollback = false
	*state = durable
	return nil
}

func finalizeTrialTx(
	tx *sql.Tx,
	durable *MinerState,
	record OperatingPointRecord,
	decision TrialDecision,
	now time.Time,
	rampUntil time.Time,
	evidenceDeadlineAt time.Time,
) error {
	if record.MacAddr != durable.MacAddr ||
		(durable.PendingKind == MutationOperatingPoint && durable.PendingPoint() != record.Point()) {
		return fmt.Errorf("record is not the durable trial target")
	}
	var current OperatingPointRecord
	var errorPercent sql.NullFloat64
	var measuredAt, enteredAt int64
	if err := tx.QueryRowContext(context.Background(), `SELECT
		mac_addr, frequency, core_voltage, status, median_hash, expected_hash,
		attainment, mean_temp, p95_temp, p95_vr_temp, p95_power, error_percent,
		accepted_delta, rejected_delta, measured_at, entered_at, entry_attempt_id,
		reference_hash FROM operating_points WHERE mac_addr = ? AND frequency = ?
		AND core_voltage = ?`, record.MacAddr, record.Frequency, record.CoreVoltage).Scan(
		&current.MacAddr, &current.Frequency, &current.CoreVoltage, &current.Status,
		&current.MedianHash, &current.ExpectedHash, &current.Attainment,
		&current.MeanTemp, &current.P95Temp, &current.P95VRTemp, &current.P95Power,
		&errorPercent, &current.AcceptedDelta, &current.RejectedDelta,
		&measuredAt, &enteredAt, &current.EntryAttemptID, &current.ReferenceHash,
	); err != nil {
		return fmt.Errorf("load point: %w", err)
	}
	current.MeasuredAt = storedTime(measuredAt)
	current.EnteredAt = storedTime(enteredAt)
	if current.Status != PointEntered || current.EntryAttemptID != record.EntryAttemptID {
		return fmt.Errorf("point is not the reserved entered row")
	}
	if _, err := tx.ExecContext(context.Background(), `UPDATE operating_points SET
		status = ?, median_hash = ?, expected_hash = ?, attainment = ?, mean_temp = ?,
		p95_temp = ?, p95_vr_temp = ?, p95_power = ?, error_percent = ?,
		accepted_delta = ?, rejected_delta = ?, measured_at = ?
		WHERE mac_addr = ? AND frequency = ? AND core_voltage = ? AND status = ?`,
		record.Status, record.MedianHash, record.ExpectedHash, record.Attainment,
		record.MeanTemp, record.P95Temp, record.P95VRTemp, record.P95Power,
		nullableFloat(record.ErrorPercent), record.AcceptedDelta, record.RejectedDelta,
		timeValue(record.MeasuredAt), record.MacAddr, record.Frequency,
		record.CoreVoltage, PointEntered); err != nil {
		return fmt.Errorf("update point: %w", err)
	}
	switch decision {
	case TrialPromote:
		durable.SetCurrentPoint(record.Point())
		if record.MedianHash >= durable.BestHashRate {
			durable.SetBestPoint(record.Point())
			durable.BestHashRate = record.MedianHash
		}
		durable.SetFallbackPoint(OperatingPoint{})
		durable.ClearPendingMutation()
		durable.Phase = PhaseBaseline
		durable.PhaseStartedAt = now
		durable.RampUntil = rampUntil
		durable.EvidenceDeadlineAt = evidenceDeadlineAt
		durable.HoldReason = ""
		durable.SettledAt = time.Time{}
	case TrialReturn:
		returnPoint := durable.FallbackPoint()
		if !validStoredPoint(returnPoint) {
			return fmt.Errorf("reserved incumbent is invalid")
		}
		if durable.CurrentPoint() == returnPoint && durable.PendingKind == MutationOperatingPoint &&
			durable.PendingPoint() == record.Point() {
			durable.ClearPendingMutation()
			durable.SetFallbackPoint(OperatingPoint{})
			durable.Phase = PhaseBaseline
			durable.PhaseStartedAt = now
			durable.RampUntil = rampUntil
			durable.EvidenceDeadlineAt = evidenceDeadlineAt
			durable.SettledAt = time.Time{}
		} else {
			durable.SetPendingMutation(MutationOperatingPoint, returnPoint, now)
			durable.EvidenceDeadlineAt = time.Time{}
			durable.SettledAt = time.Time{}
		}
	case TrialBlock:
		durable.ClearPendingMutation()
		durable.SetFallbackPoint(OperatingPoint{})
		durable.Phase = PhaseHold
		durable.HoldReason = HoldBlocked
		durable.SettledAt = time.Time{}
		durable.EvidenceDeadlineAt = time.Time{}
	default:
		return fmt.Errorf("invalid trial decision %q", decision)
	}
	return saveMiner(tx, durable)
}

// FailMutationAndFinalizeTrial atomically closes a failed operating-point
// attempt, finalizes its candidate evidence, and applies the reserved return
// or blocked state. It is the terminal boundary for entry and resume failure.
func (store *OptimizerStore) FailMutationAndFinalizeTrial(
	state *MinerState,
	record OperatingPointRecord,
	decision TrialDecision,
	id int64,
	stage MutationFailureStage,
	now time.Time,
	rampUntil time.Time,
	evidenceDeadlineAt time.Time,
) error {
	if state == nil || id <= 0 || now.IsZero() || stage == "" {
		return fmt.Errorf("fail mutation and finalize trial: invalid state or timing")
	}
	if err := validatePointRecord(record); err != nil {
		return fmt.Errorf("fail mutation and finalize trial: %w", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("fail mutation and finalize trial"); err != nil {
		return err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("fail mutation and finalize trial: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	attempt, err := queryMutationAttempt(tx, id)
	if err != nil {
		return fmt.Errorf("fail mutation and finalize trial: load attempt: %w", err)
	}
	durable, err := queryMiner(tx, state.MacAddr)
	if err != nil {
		return fmt.Errorf("fail mutation and finalize trial: load miner: %w", err)
	}
	if attempt.MacAddr != durable.MacAddr || attempt.Kind != MutationOperatingPoint {
		return fmt.Errorf("fail mutation and finalize trial: attempt does not belong to operating-point trial")
	}
	if !attempt.FailedAt.IsZero() {
		if attempt.FailureStage != stage {
			return fmt.Errorf("fail mutation and finalize trial: attempt already failed at %q", attempt.FailureStage)
		}
		if !attempt.FailedAt.Equal(now) {
			return fmt.Errorf("fail mutation and finalize trial: failure timestamp conflicts with stored value")
		}
		*state = durable
		return nil
	}
	if durable.CurrentPoint() != state.CurrentPoint() || durable.FallbackPoint() != state.FallbackPoint() ||
		durable.AccountedThroughAt != state.AccountedThroughAt {
		return fmt.Errorf("fail mutation and finalize trial: stale miner state")
	}
	if err := finalizeTrialTx(tx, &durable, record, decision, now, rampUntil, evidenceDeadlineAt); err != nil {
		return fmt.Errorf("fail mutation and finalize trial: %w", err)
	}
	attempt.FailedAt = now
	attempt.FailureStage = stage
	if err := validateMutationAttempt(attempt, true); err != nil {
		return fmt.Errorf("fail mutation and finalize trial: %w", err)
	}
	if _, err := tx.ExecContext(context.Background(), `UPDATE mutation_attempts
		SET failed_at = ?, failure_stage = ? WHERE id = ? AND failed_at = 0`,
		timeValue(now), stage, id); err != nil {
		return fmt.Errorf("fail mutation and finalize trial: close attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("fail mutation and finalize trial: commit: %w", err)
	}
	rollback = false
	*state = durable
	return nil
}

// FinalizeBaseline atomically closes the pass bootstrap row and updates the
// incumbent state. A rejected baseline enters blocked HOLD; it never creates a
// speculative exploration fallback.
func (store *OptimizerStore) FinalizeBaseline(
	state *MinerState,
	record OperatingPointRecord,
	block bool,
	now time.Time,
) error {
	if state == nil || record.Status == PointEntered || record.EntryAttemptID != 0 || now.IsZero() {
		return fmt.Errorf("finalize baseline: invalid state or record")
	}
	if err := validatePointRecord(record); err != nil {
		return fmt.Errorf("finalize baseline: %w", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("finalize baseline"); err != nil {
		return err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("finalize baseline: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	durable, err := queryMiner(tx, state.MacAddr)
	if err != nil {
		return fmt.Errorf("finalize baseline: load miner: %w", err)
	}
	if count, countErr := unfinishedMutationCount(tx, durable.MacAddr); countErr != nil {
		return fmt.Errorf("finalize baseline: check mutation authority: %w", countErr)
	} else if count != 0 {
		return fmt.Errorf("finalize baseline: unfinished mutation attempt exists")
	}
	if durable.CurrentPoint() != state.CurrentPoint() || durable.PendingKind != "" {
		return fmt.Errorf("finalize baseline: stale or pending miner state")
	}
	if record.MacAddr != durable.MacAddr || record.Point() != durable.CurrentPoint() {
		return fmt.Errorf("finalize baseline: record is not the durable incumbent")
	}
	var status string
	if err := tx.QueryRowContext(context.Background(),
		`SELECT status FROM operating_points WHERE mac_addr = ? AND frequency = ? AND core_voltage = ?`,
		record.MacAddr, record.Frequency, record.CoreVoltage).Scan(&status); err != nil {
		return fmt.Errorf("finalize baseline: load point: %w", err)
	}
	if status != PointEntered {
		return fmt.Errorf("finalize baseline: baseline row is already terminal")
	}
	if _, err := tx.ExecContext(context.Background(), `UPDATE operating_points SET
		status = ?, median_hash = ?, expected_hash = ?, attainment = ?, mean_temp = ?,
		p95_temp = ?, p95_vr_temp = ?, p95_power = ?, error_percent = ?,
		accepted_delta = ?, rejected_delta = ?, measured_at = ?
		WHERE mac_addr = ? AND frequency = ? AND core_voltage = ? AND status = ?`,
		record.Status, record.MedianHash, record.ExpectedHash, record.Attainment,
		record.MeanTemp, record.P95Temp, record.P95VRTemp, record.P95Power,
		nullableFloat(record.ErrorPercent), record.AcceptedDelta, record.RejectedDelta,
		timeValue(record.MeasuredAt), record.MacAddr, record.Frequency,
		record.CoreVoltage, PointEntered); err != nil {
		return fmt.Errorf("finalize baseline: update point: %w", err)
	}
	if block {
		durable.SetBestPoint(OperatingPoint{})
		durable.BestHashRate = 0
		durable.Phase = PhaseHold
		durable.HoldReason = HoldBlocked
		durable.SettledAt = time.Time{}
		durable.EvidenceDeadlineAt = time.Time{}
	} else {
		if durable.PassTrigger == PassInitial && durable.PassReferenceHash == 0 && record.MedianHash > 0 {
			durable.PassReferenceHash = record.MedianHash
		}
		if record.MedianHash >= durable.BestHashRate {
			durable.SetBestPoint(record.Point())
			durable.BestHashRate = record.MedianHash
		}
		if durable.Phase == PhaseHold && durable.HoldReason == HoldManual {
			durable.SettledAt = now
			durable.EvidenceDeadlineAt = time.Time{}
		} else {
			durable.Phase = PhaseBaseline
			durable.HoldReason = ""
			durable.SettledAt = time.Time{}
		}
	}
	if err := saveMiner(tx, &durable); err != nil {
		return fmt.Errorf("finalize baseline: save miner: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("finalize baseline: commit: %w", err)
	}
	rollback = false
	*state = durable
	return nil
}

// AdoptManualPoint records a confirmed external complete pair and starts a
// fresh baseline window without authorizing hardware or deleting history.
func (store *OptimizerStore) AdoptManualPoint(
	state *MinerState,
	point OperatingPoint,
	enteredAt time.Time,
	rampUntil time.Time,
	evidenceDeadlineAt time.Time,
) error {
	if state == nil || !validStoredPoint(point) || point.Frequency == 50 || enteredAt.IsZero() ||
		rampUntil.IsZero() || evidenceDeadlineAt.IsZero() || evidenceDeadlineAt.Before(rampUntil) {
		return fmt.Errorf("adopt manual point: invalid point or timing")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("adopt manual point"); err != nil {
		return err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("adopt manual point: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	durable, err := queryMiner(tx, state.MacAddr)
	if err != nil {
		return fmt.Errorf("adopt manual point: load miner: %w", err)
	}
	if count, countErr := unfinishedMutationCount(tx, durable.MacAddr); countErr != nil {
		return fmt.Errorf("adopt manual point: check mutation authority: %w", countErr)
	} else if count != 0 {
		return fmt.Errorf("adopt manual point: unfinished mutation attempt exists")
	}
	if durable.PendingKind != "" || durable.MiningPending || durable.Phase == PhaseOverheat ||
		durable.Phase == PhaseCooldown || durable.HoldReason == HoldSafety || durable.SafetyReason != "" {
		return fmt.Errorf("adopt manual point: miner is not in a clean manual-adoption state")
	}
	durable.SetCurrentPoint(point)
	durable.SetFallbackPoint(OperatingPoint{})
	durable.ClearPendingMutation()
	durable.Phase = PhaseHold
	durable.HoldReason = HoldManual
	durable.SafetyReason = ""
	durable.SettledAt = time.Time{}
	durable.PhaseStartedAt = enteredAt
	durable.RampUntil = rampUntil
	durable.EvidenceDeadlineAt = evidenceDeadlineAt
	durable.ObservedFrequency = 0
	durable.ObservedCoreVoltage = 0
	durable.ObservedCount = 0
	if err := saveMiner(tx, &durable); err != nil {
		return fmt.Errorf("adopt manual point: save miner: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("adopt manual point: commit: %w", err)
	}
	rollback = false
	*state = durable
	return nil
}

// AdoptExternalPoint atomically resolves a pre-PATCH operating-point attempt
// after two safe observations of an externally changed pair. The observed
// pair becomes manual HOLD state; it never becomes a normal automation target.
func (store *OptimizerStore) AdoptExternalPoint(
	state *MinerState,
	point OperatingPoint,
	attemptID int64,
	at time.Time,
	rampUntil time.Time,
	evidenceDeadlineAt time.Time,
) error {
	if state == nil || !validStoredPoint(point) || point.Frequency == 50 || attemptID <= 0 ||
		at.IsZero() || rampUntil.IsZero() || evidenceDeadlineAt.IsZero() || evidenceDeadlineAt.Before(rampUntil) {
		return fmt.Errorf("adopt external point: invalid state, point, attempt, or timing")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("adopt external point"); err != nil {
		return err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("adopt external point: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	durable, err := queryMiner(tx, state.MacAddr)
	if err != nil {
		return fmt.Errorf("adopt external point: load miner: %w", err)
	}
	attempt, err := queryMutationAttempt(tx, attemptID)
	if err != nil {
		return fmt.Errorf("adopt external point: load attempt: %w", err)
	}
	if attempt.Kind != MutationOperatingPoint || attempt.MacAddr != durable.MacAddr {
		return fmt.Errorf("adopt external point: attempt is not an operating-point attempt for this miner")
	}
	if !attempt.FailedAt.IsZero() {
		if attempt.FailureStage == MutationFailurePreflight && durable.Phase == PhaseHold && durable.HoldReason == HoldManual &&
			durable.CurrentPoint() == point {
			*state = durable
			return nil
		}
		return fmt.Errorf("adopt external point: attempt %d is already closed", attemptID)
	}
	if durable.CurrentPoint() != state.CurrentPoint() || durable.FallbackPoint() != state.FallbackPoint() ||
		durable.PendingKind != MutationOperatingPoint || durable.PendingPoint() != attempt.TargetPoint() {
		return fmt.Errorf("adopt external point: stale or changed durable intent")
	}
	var record OperatingPointRecord
	var errorPercent sql.NullFloat64
	var measuredAt, enteredAt int64
	err = tx.QueryRowContext(context.Background(), `SELECT
		mac_addr, frequency, core_voltage, status, median_hash, expected_hash,
		attainment, mean_temp, p95_temp, p95_vr_temp, p95_power, error_percent,
		accepted_delta, rejected_delta, measured_at, entered_at, entry_attempt_id,
		reference_hash FROM operating_points WHERE mac_addr = ? AND frequency = ? AND core_voltage = ?`,
		attempt.MacAddr, attempt.TargetFrequency, attempt.TargetCoreVoltage).Scan(
		&record.MacAddr, &record.Frequency, &record.CoreVoltage, &record.Status,
		&record.MedianHash, &record.ExpectedHash, &record.Attainment,
		&record.MeanTemp, &record.P95Temp, &record.P95VRTemp, &record.P95Power,
		&errorPercent, &record.AcceptedDelta, &record.RejectedDelta,
		&measuredAt, &enteredAt, &record.EntryAttemptID, &record.ReferenceHash,
	)
	if err != nil {
		return fmt.Errorf("adopt external point: load target row: %w", err)
	}
	if errorPercent.Valid {
		value := errorPercent.Float64
		record.ErrorPercent = &value
	}
	record.MeasuredAt = storedTime(measuredAt)
	record.EnteredAt = storedTime(enteredAt)
	if record.Status == PointEntered && record.EntryAttemptID == attemptID {
		record.Status = PointUnobservable
		record.MeasuredAt = at
		if record.MeasuredAt.Before(record.EnteredAt) {
			record.MeasuredAt = record.EnteredAt
		}
		if _, err := tx.ExecContext(context.Background(), `UPDATE operating_points SET
			status = ?, median_hash = 0, expected_hash = 0, attainment = 0,
			mean_temp = 0, p95_temp = 0, p95_vr_temp = 0, p95_power = 0,
			error_percent = NULL, accepted_delta = 0, rejected_delta = 0,
			measured_at = ? WHERE mac_addr = ? AND frequency = ? AND core_voltage = ?
			AND status = ? AND entry_attempt_id = ?`,
			record.Status, timeValue(record.MeasuredAt), record.MacAddr,
			record.Frequency, record.CoreVoltage, PointEntered, attemptID); err != nil {
			return fmt.Errorf("adopt external point: finalize target row: %w", err)
		}
	} else if record.Status == PointEntered && record.EntryAttemptID != 0 {
		return fmt.Errorf("adopt external point: target row is bound to another unfinished entry")
	} else if record.Status != PointEntered && (record.MeasuredAt.IsZero() || record.MeasuredAt.Before(record.EnteredAt)) {
		return fmt.Errorf("adopt external point: terminal target row has invalid evidence")
	}
	durable.SetCurrentPoint(point)
	durable.SetFallbackPoint(OperatingPoint{})
	durable.ClearPendingMutation()
	durable.Phase = PhaseHold
	durable.HoldReason = HoldManual
	durable.SafetyReason = ""
	durable.SettledAt = time.Time{}
	durable.PhaseStartedAt = at
	durable.RampUntil = rampUntil
	durable.EvidenceDeadlineAt = evidenceDeadlineAt
	durable.ObservedFrequency = 0
	durable.ObservedCoreVoltage = 0
	durable.ObservedCount = 0
	attempt.FailedAt = at
	attempt.FailureStage = MutationFailurePreflight
	if err := validateMutationAttempt(attempt, true); err != nil {
		return fmt.Errorf("adopt external point: validate attempt: %w", err)
	}
	if err := validateMinerState(durable); err != nil {
		return fmt.Errorf("adopt external point: validate state: %w", err)
	}
	if err := saveMiner(tx, &durable); err != nil {
		return fmt.Errorf("adopt external point: save state: %w", err)
	}
	if _, err := tx.ExecContext(context.Background(), `UPDATE mutation_attempts
		SET failed_at = ?, failure_stage = ? WHERE id = ? AND failed_at = 0`,
		timeValue(attempt.FailedAt), attempt.FailureStage, attemptID); err != nil {
		return fmt.Errorf("adopt external point: close attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("adopt external point: commit: %w", err)
	}
	rollback = false
	*state = durable
	return nil
}

func (store *OptimizerStore) SaveMiner(state *MinerState) error {
	if state == nil {
		return fmt.Errorf("save miner state: state is nil")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("save miner state"); err != nil {
		return err
	}
	return saveMiner(store.conn, state)
}

// LoadMiner returns the current durable state for a known MAC address.
func (store *OptimizerStore) LoadMiner(macAddr string) (MinerState, error) {
	if strings.TrimSpace(macAddr) == "" {
		return MinerState{}, fmt.Errorf("load miner state: MAC address is empty")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("load miner state"); err != nil {
		return MinerState{}, err
	}
	state, err := queryMiner(store.conn, macAddr)
	if errors.Is(err, sql.ErrNoRows) {
		return MinerState{}, fmt.Errorf("load miner state: miner %s does not exist", macAddr)
	}
	if err != nil {
		return MinerState{}, fmt.Errorf("load miner state: %w", err)
	}
	if err := validateMinerState(state); err != nil {
		return MinerState{}, fmt.Errorf("load miner state: %w", err)
	}
	return state, nil
}

// LoadMinerByHostname returns the uniquely named durable miner required by a
// read-only operator report.
func (store *OptimizerStore) LoadMinerByHostname(hostname string) (MinerState, error) {
	if strings.TrimSpace(hostname) == "" || strings.TrimSpace(hostname) != hostname || hasControl(hostname) {
		return MinerState{}, fmt.Errorf("load miner by hostname: hostname is invalid")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("load miner by hostname"); err != nil {
		return MinerState{}, err
	}
	rows, err := store.conn.QueryContext(context.Background(),
		"SELECT mac_addr FROM optimizer_miners WHERE hostname = ? ORDER BY mac_addr", hostname)
	if err != nil {
		return MinerState{}, fmt.Errorf("load miner by hostname: query: %w", err)
	}
	defer rows.Close()
	var macAddr string
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return MinerState{}, fmt.Errorf("load miner by hostname: query: %w", err)
		}
		return MinerState{}, fmt.Errorf("load miner by hostname: %q does not exist", hostname)
	}
	if err := rows.Scan(&macAddr); err != nil {
		return MinerState{}, fmt.Errorf("load miner by hostname: read MAC: %w", err)
	}
	if rows.Next() {
		return MinerState{}, fmt.Errorf("load miner by hostname: %q maps to multiple MAC addresses", hostname)
	}
	if err := rows.Err(); err != nil {
		return MinerState{}, fmt.Errorf("load miner by hostname: query: %w", err)
	}
	state, err := queryMiner(store.conn, macAddr)
	if err != nil {
		return MinerState{}, fmt.Errorf("load miner by hostname: load state: %w", err)
	}
	if err := validateMinerState(state); err != nil {
		return MinerState{}, fmt.Errorf("load miner by hostname: validate state: %w", err)
	}
	return state, nil
}

func insertPoint(executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, record OperatingPointRecord) error {
	if err := validatePointRecord(record); err != nil {
		return err
	}
	_, err := executor.ExecContext(
		context.Background(),
		`INSERT INTO operating_points (
			mac_addr, frequency, core_voltage, status, median_hash, expected_hash,
			attainment, mean_temp, p95_temp, p95_vr_temp, p95_power, error_percent,
			accepted_delta, rejected_delta, measured_at, entered_at,
			entry_attempt_id, reference_hash
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.MacAddr,
		record.Frequency,
		record.CoreVoltage,
		record.Status,
		record.MedianHash,
		record.ExpectedHash,
		record.Attainment,
		record.MeanTemp,
		record.P95Temp,
		record.P95VRTemp,
		record.P95Power,
		nullableFloat(record.ErrorPercent),
		record.AcceptedDelta,
		record.RejectedDelta,
		timeValue(record.MeasuredAt),
		timeValue(record.EnteredAt),
		record.EntryAttemptID,
		record.ReferenceHash,
	)
	return err
}

func (store *OptimizerStore) ListPoints(macAddr string) ([]OperatingPointRecord, error) {
	if strings.TrimSpace(macAddr) == "" {
		return nil, fmt.Errorf("list operating points: MAC address is empty")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("list operating points"); err != nil {
		return nil, err
	}
	rows, err := store.conn.QueryContext(
		context.Background(),
		`SELECT mac_addr, frequency, core_voltage, status, median_hash,
			expected_hash, attainment, mean_temp, p95_temp, p95_vr_temp,
			p95_power, error_percent, accepted_delta, rejected_delta,
			measured_at, entered_at, entry_attempt_id, reference_hash
		FROM operating_points
		WHERE mac_addr = ?
		ORDER BY frequency ASC, core_voltage ASC`,
		macAddr,
	)
	if err != nil {
		return nil, fmt.Errorf("list operating points: %w", err)
	}
	defer rows.Close()

	records, err := scanPointRows(rows)
	if err != nil {
		return nil, fmt.Errorf("list operating points: %w", err)
	}
	return records, nil
}

// StartMutationAttempt durably records a mutation attempt before hardware work.
// An unfinished attempt is a crash-recovery obligation and is never closed as
// a side effect of creating another attempt.
func (store *OptimizerStore) StartMutationAttempt(
	attempt *MutationAttempt,
) (int64, error) {
	if attempt == nil {
		return 0, fmt.Errorf("start mutation attempt: attempt is nil")
	}
	if attempt.ID != 0 {
		return 0, fmt.Errorf("start mutation attempt: ID must be zero")
	}
	if err := validateMutationAttempt(*attempt, false); err != nil {
		return 0, fmt.Errorf("start mutation attempt: %w", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("start mutation attempt"); err != nil {
		return 0, err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, fmt.Errorf("start mutation attempt: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	durable, err := queryMiner(tx, attempt.MacAddr)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("start mutation attempt: miner does not exist")
	} else if err != nil {
		return 0, fmt.Errorf("start mutation attempt: load miner: %w", err)
	}
	if attempt.Kind == MutationMiningConfiguration {
		if !durable.MiningPending || durable.PendingKind != "" {
			return 0, fmt.Errorf("start mutation attempt: mining obligation is not pending")
		}
	} else if durable.PendingKind != attempt.Kind || durable.PendingPoint() != attempt.TargetPoint() {
		return 0, fmt.Errorf("start mutation attempt: operating-point intent does not match durable state")
	}
	if attempt.Kind != MutationMiningConfiguration &&
		(durable.PendingSince.IsZero() || attempt.IntentCreatedAt.IsZero() ||
			!attempt.IntentCreatedAt.Equal(durable.PendingSince)) {
		return 0, fmt.Errorf("start mutation attempt: intent timestamp does not match durable pending intent")
	}
	if attempt.Kind == MutationOperatingPoint && durable.CurrentPoint() != attempt.FromPoint() {
		return 0, fmt.Errorf("start mutation attempt: source operating point does not match durable state")
	}
	if attempt.Kind == MutationSafetyRollback || attempt.Kind == MutationOverheatRecovery {
		if durable.SafetyReason == "" || durable.SafetyReason != attempt.Reason {
			return 0, fmt.Errorf("start mutation attempt: safety reason does not match durable episode")
		}
	}
	var unfinishedID int64
	err = tx.QueryRowContext(context.Background(),
		`SELECT id FROM mutation_attempts
		 WHERE mac_addr = ? AND failed_at = 0 AND mining_resumed_at = 0
		 ORDER BY id DESC LIMIT 1`, attempt.MacAddr).Scan(&unfinishedID)
	if err == nil {
		return 0, fmt.Errorf("start mutation attempt: unfinished attempt %d already exists", unfinishedID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("start mutation attempt: check unfinished attempt: %w", err)
	}
	result, err := tx.ExecContext(
		context.Background(),
		`INSERT INTO mutation_attempts (
			mac_addr, kind, reason, from_frequency, from_core_voltage,
			target_frequency, target_core_voltage, intent_created_at,
			started_at, patch_requested_at, configured_verified_at,
			configured_verified_uptime_seconds, restart_requested_at,
			reboot_verified_at, completed_at, first_positive_at,
			mining_resumed_at,
			failed_at, failure_stage
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, -1, 0, 0, 0, 0, 0, 0, '')`,
		attempt.MacAddr,
		attempt.Kind,
		attempt.Reason,
		attempt.FromFrequency,
		attempt.FromCoreVoltage,
		attempt.TargetFrequency,
		attempt.TargetCoreVoltage,
		timeValue(attempt.IntentCreatedAt),
		timeValue(attempt.StartedAt),
	)
	if err != nil {
		return 0, fmt.Errorf("start mutation attempt: insert: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("start mutation attempt: read ID: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("start mutation attempt: commit: %w", err)
	}
	rollback = false
	attempt.ID = id
	return id, nil
}

// AdvanceMutationAttempt records one ordered mutation lifecycle milestone.
func (store *OptimizerStore) AdvanceMutationAttempt(
	id int64,
	milestone MutationMilestone,
	at time.Time,
) error {
	if id <= 0 {
		return fmt.Errorf("advance mutation attempt: ID must be positive")
	}
	if at.IsZero() {
		return fmt.Errorf("advance mutation attempt: timestamp is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("advance mutation attempt"); err != nil {
		return err
	}
	attempt, err := queryMutationAttempt(store.conn, id)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("advance mutation attempt: attempt %d does not exist", id)
	}
	if err != nil {
		return fmt.Errorf("advance mutation attempt: %w", err)
	}
	if !attempt.FailedAt.IsZero() {
		return fmt.Errorf("advance mutation attempt: attempt %d already failed", id)
	}
	switch milestone {
	case MutationMilestonePatchRequested:
		if !attempt.PatchRequestedAt.IsZero() {
			if attempt.PatchRequestedAt.Equal(at) {
				return nil
			}
			return fmt.Errorf("advance mutation attempt: PATCH milestone conflicts with stored value")
		}
		attempt.PatchRequestedAt = at
	case MutationMilestoneConfiguredVerified:
		return fmt.Errorf("advance mutation attempt: configured verification requires uptime")
	case MutationMilestoneRestartRequested:
		if !attempt.RestartRequestedAt.IsZero() {
			if attempt.RestartRequestedAt.Equal(at) {
				return nil
			}
			return fmt.Errorf("advance mutation attempt: restart milestone conflicts with stored value")
		}
		attempt.RestartRequestedAt = at
	case MutationMilestoneRebootVerified:
		if !attempt.RebootVerifiedAt.IsZero() {
			if attempt.RebootVerifiedAt.Equal(at) {
				return nil
			}
			return fmt.Errorf("advance mutation attempt: reboot milestone conflicts with stored value")
		}
		attempt.RebootVerifiedAt = at
	case MutationMilestoneFirstPositive:
		if !attempt.FirstPositiveAt.IsZero() {
			if attempt.FirstPositiveAt.Equal(at) {
				return nil
			}
			return fmt.Errorf("advance mutation attempt: first-positive milestone conflicts with stored value")
		}
		attempt.FirstPositiveAt = at
	case MutationMilestoneMiningResumed:
		if !attempt.MiningResumedAt.IsZero() {
			if attempt.MiningResumedAt.Equal(at) {
				return nil
			}
			return fmt.Errorf("advance mutation attempt: mining-resumed milestone conflicts with stored value")
		}
		attempt.MiningResumedAt = at
	default:
		return fmt.Errorf("advance mutation attempt: milestone %q is invalid", milestone)
	}
	if err := validateMutationAttempt(attempt, true); err != nil {
		return fmt.Errorf("advance mutation attempt: %w", err)
	}
	_, err = store.conn.ExecContext(
		context.Background(),
		`UPDATE mutation_attempts SET
			patch_requested_at = ?, restart_requested_at = ?,
			reboot_verified_at = ?, first_positive_at = ?,
			mining_resumed_at = ?
		WHERE id = ?`,
		timeValue(attempt.PatchRequestedAt),
		timeValue(attempt.RestartRequestedAt),
		timeValue(attempt.RebootVerifiedAt),
		timeValue(attempt.FirstPositiveAt),
		timeValue(attempt.MiningResumedAt),
		id,
	)
	if err != nil {
		return fmt.Errorf("advance mutation attempt: update: %w", err)
	}
	return nil
}

// RecordConfiguredVerification stores the first exact configured readback and
// its boot-local uptime. Configured equality is evidence for reconciliation,
// never proof that the requested frequency is physically active.
func (store *OptimizerStore) RecordConfiguredVerification(
	id int64,
	at time.Time,
	uptimeSeconds int,
) error {
	if id <= 0 || at.IsZero() || uptimeSeconds < 0 {
		return fmt.Errorf("record configured verification: invalid milestone")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("record configured verification"); err != nil {
		return err
	}
	attempt, err := queryMutationAttempt(store.conn, id)
	if err != nil {
		return fmt.Errorf("record configured verification: %w", err)
	}
	if !attempt.FailedAt.IsZero() {
		return fmt.Errorf("record configured verification: attempt %d already failed", id)
	}
	if !attempt.ConfiguredVerifiedAt.IsZero() {
		if attempt.ConfiguredVerifiedAt.Equal(at) &&
			attempt.ConfiguredVerifiedUptimeSeconds == uptimeSeconds {
			return nil
		}
		return fmt.Errorf("record configured verification: milestone conflicts with stored value")
	}
	attempt.ConfiguredVerifiedAt = at
	attempt.ConfiguredVerifiedUptimeSeconds = uptimeSeconds
	if err := validateMutationAttempt(attempt, true); err != nil {
		return fmt.Errorf("record configured verification: %w", err)
	}
	_, err = store.conn.ExecContext(
		context.Background(),
		`UPDATE mutation_attempts SET configured_verified_at = ?,
			configured_verified_uptime_seconds = ? WHERE id = ?`,
		timeValue(at), uptimeSeconds, id,
	)
	if err != nil {
		return fmt.Errorf("record configured verification: %w", err)
	}
	return nil
}

// RecordFirstPositive stores the earliest safe positive observation after
// durable completion. It is reporting evidence; the second consecutive poll
// remains the mining-resumed control gate.
func (store *OptimizerStore) RecordFirstPositive(id int64, at time.Time) error {
	if id <= 0 || at.IsZero() {
		return fmt.Errorf("record first positive: invalid milestone")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("record first positive"); err != nil {
		return err
	}
	attempt, err := queryMutationAttempt(store.conn, id)
	if err != nil {
		return fmt.Errorf("record first positive: %w", err)
	}
	if !attempt.FirstPositiveAt.IsZero() {
		if attempt.FirstPositiveAt.Equal(at) {
			return nil
		}
		return fmt.Errorf("record first positive: milestone conflicts with stored value")
	}
	attempt.FirstPositiveAt = at
	if err := validateMutationAttempt(attempt, true); err != nil {
		return fmt.Errorf("record first positive: %w", err)
	}
	_, err = store.conn.ExecContext(
		context.Background(),
		"UPDATE mutation_attempts SET first_positive_at = ? WHERE id = ?",
		timeValue(at), id,
	)
	if err != nil {
		return fmt.Errorf("record first positive: %w", err)
	}
	return nil
}

// CompleteMiningResume atomically closes the healthy-mining gate and starts
// the next phase window. The attempt and miner row become visible together so
// a crash cannot report resumed mining without the corresponding deadline.
func (store *OptimizerStore) CompleteMiningResume(
	state *MinerState,
	id int64,
	at time.Time,
	rampUntil time.Time,
	evidenceDeadlineAt time.Time,
) error {
	if state == nil || id <= 0 || at.IsZero() || rampUntil.IsZero() || evidenceDeadlineAt.IsZero() ||
		evidenceDeadlineAt.Before(rampUntil) {
		return fmt.Errorf("complete mining resume: invalid state or timing")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("complete mining resume"); err != nil {
		return err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("complete mining resume: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	attempt, err := queryMutationAttempt(tx, id)
	if err != nil {
		return fmt.Errorf("complete mining resume: load attempt: %w", err)
	}
	durable, err := queryMiner(tx, attempt.MacAddr)
	if err != nil {
		return fmt.Errorf("complete mining resume: load miner: %w", err)
	}
	if durable.MacAddr != state.MacAddr || attempt.MacAddr != state.MacAddr {
		return fmt.Errorf("complete mining resume: miner does not match attempt")
	}
	if !attempt.MiningResumedAt.IsZero() {
		if !attempt.MiningResumedAt.Equal(at) {
			return fmt.Errorf("complete mining resume: resumption timestamp conflicts with stored value")
		}
		if err := validateMinerState(durable); err != nil {
			return fmt.Errorf("complete mining resume: stored state is invalid: %w", err)
		}
		*state = durable
		return nil
	}
	if attempt.FailedAt.IsZero() == false || attempt.CompletedAt.IsZero() || attempt.FirstPositiveAt.IsZero() {
		return fmt.Errorf("complete mining resume: attempt is not ready")
	}
	if attempt.Kind == MutationMiningConfiguration {
		if durable.MiningPending {
			return fmt.Errorf("complete mining resume: mining obligation remains pending")
		}
	} else if durable.PendingKind != "" || durable.CurrentPoint() != attempt.TargetPoint() {
		return fmt.Errorf("complete mining resume: operating-point state is not complete")
	}
	attempt.MiningResumedAt = at
	if err := validateMutationAttempt(attempt, true); err != nil {
		return fmt.Errorf("complete mining resume: %w", err)
	}
	durable.RampUntil = rampUntil
	durable.EvidenceDeadlineAt = evidenceDeadlineAt
	durable.ObservedFrequency = 0
	durable.ObservedCoreVoltage = 0
	durable.ObservedCount = 0
	if err := validateMinerState(durable); err != nil {
		return fmt.Errorf("complete mining resume: %w", err)
	}
	if err := saveMiner(tx, &durable); err != nil {
		return fmt.Errorf("complete mining resume: save miner: %w", err)
	}
	if _, err := tx.ExecContext(context.Background(),
		"UPDATE mutation_attempts SET mining_resumed_at = ? WHERE id = ?",
		timeValue(attempt.MiningResumedAt), id); err != nil {
		return fmt.Errorf("complete mining resume: update attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("complete mining resume: commit: %w", err)
	}
	rollback = false
	*state = durable
	return nil
}

// FailMutationAndSave atomically closes an unfinished mutation and persists
// the terminal non-trial state that owns the remaining safety or mining
// obligation.
func (store *OptimizerStore) FailMutationAndSave(
	state *MinerState,
	id int64,
	stage MutationFailureStage,
	at time.Time,
) error {
	if state == nil || id <= 0 || stage == "" || at.IsZero() {
		return fmt.Errorf("fail mutation and save: invalid state or timing")
	}
	if err := validateMinerState(*state); err != nil {
		return fmt.Errorf("fail mutation and save: %w", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("fail mutation and save"); err != nil {
		return err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("fail mutation and save: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	attempt, err := queryMutationAttempt(tx, id)
	if err != nil {
		return fmt.Errorf("fail mutation and save: load attempt: %w", err)
	}
	durable, err := queryMiner(tx, state.MacAddr)
	if err != nil {
		return fmt.Errorf("fail mutation and save: load miner: %w", err)
	}
	if attempt.MacAddr != durable.MacAddr {
		return fmt.Errorf("fail mutation and save: attempt does not belong to miner")
	}
	if !attempt.FailedAt.IsZero() {
		if attempt.FailureStage != stage {
			return fmt.Errorf("fail mutation and save: attempt already failed at %q", attempt.FailureStage)
		}
		if !attempt.FailedAt.Equal(at) {
			return fmt.Errorf("fail mutation and save: failure timestamp conflicts with stored value")
		}
		*state = durable
		return nil
	}
	if durable.AccountedThroughAt != state.AccountedThroughAt {
		return fmt.Errorf("fail mutation and save: stale accounting cursor")
	}
	if durable.CurrentPoint() != state.CurrentPoint() || durable.FallbackPoint() != state.FallbackPoint() ||
		durable.PendingKind != state.PendingKind || durable.PendingPoint() != state.PendingPoint() ||
		durable.MiningPending != state.MiningPending {
		return fmt.Errorf("fail mutation and save: stale mutation authority")
	}
	if attempt.Kind == MutationMiningConfiguration {
		if !durable.MiningPending || durable.PendingKind != "" {
			return fmt.Errorf("fail mutation and save: mining obligation is no longer pending")
		}
	} else if durable.PendingKind != attempt.Kind || durable.PendingPoint() != attempt.TargetPoint() {
		// Completed-but-unresumed attempts have already cleared their pending
		// authority. They are handled by the resumption failure path using the
		// same durable current point.
		if attempt.CompletedAt.IsZero() || durable.CurrentPoint() != attempt.TargetPoint() {
			return fmt.Errorf("fail mutation and save: mutation obligation is no longer pending")
		}
	}
	attempt.FailedAt = at
	attempt.FailureStage = stage
	if err := validateMutationAttempt(attempt, true); err != nil {
		return fmt.Errorf("fail mutation and save: %w", err)
	}
	if err := saveMiner(tx, state); err != nil {
		return fmt.Errorf("fail mutation and save: save miner: %w", err)
	}
	if _, err := tx.ExecContext(context.Background(), `UPDATE mutation_attempts
		SET failed_at = ?, failure_stage = ? WHERE id = ? AND failed_at = 0`,
		timeValue(at), stage, id); err != nil {
		return fmt.Errorf("fail mutation and save: close attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("fail mutation and save: commit: %w", err)
	}
	rollback = false
	return nil
}

// QuarantineMutation atomically closes a post-request mutation whose device
// state is unavailable or ambiguous. An operating-point candidate is made
// unobservable, all actuatable authority is cleared, and the miner enters the
// durable mutation-uncertain emergency block without claiming containment.
func (store *OptimizerStore) QuarantineMutation(
	state *MinerState,
	id int64,
	stage MutationFailureStage,
	at time.Time,
) error {
	if state == nil || id <= 0 || stage == "" || at.IsZero() {
		return fmt.Errorf("quarantine mutation: invalid state or timing")
	}
	if err := validateMinerState(*state); err != nil {
		return fmt.Errorf("quarantine mutation: %w", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("quarantine mutation"); err != nil {
		return err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("quarantine mutation: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	attempt, err := queryMutationAttempt(tx, id)
	if err != nil {
		return fmt.Errorf("quarantine mutation: load attempt: %w", err)
	}
	durable, err := queryMiner(tx, state.MacAddr)
	if err != nil {
		return fmt.Errorf("quarantine mutation: load miner: %w", err)
	}
	if attempt.MacAddr != durable.MacAddr || durable.CurrentPoint() != state.CurrentPoint() ||
		durable.AccountedThroughAt != state.AccountedThroughAt {
		return fmt.Errorf("quarantine mutation: stale miner state")
	}
	if !attempt.FailedAt.IsZero() {
		if attempt.FailureStage != stage || !attempt.FailedAt.Equal(at) {
			return fmt.Errorf("quarantine mutation: failure milestone conflicts with stored value")
		}
		*state = durable
		return nil
	}
	if attempt.Kind == MutationOperatingPoint {
		var status string
		var enteredAt int64
		var entryAttemptID int64
		err := tx.QueryRowContext(context.Background(), `SELECT status, entered_at, entry_attempt_id
			FROM operating_points WHERE mac_addr = ? AND frequency = ? AND core_voltage = ?`,
			attempt.MacAddr, attempt.TargetFrequency, attempt.TargetCoreVoltage).Scan(
			&status, &enteredAt, &entryAttemptID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("quarantine mutation: load candidate: %w", err)
		}
		if err == nil && status == PointEntered && entryAttemptID == attempt.ID {
			measuredAt := at
			entered := storedTime(enteredAt)
			if measuredAt.Before(entered) {
				measuredAt = entered
			}
			if _, err := tx.ExecContext(context.Background(), `UPDATE operating_points SET
				status = ?, median_hash = 0, expected_hash = 0, attainment = 0,
				mean_temp = 0, p95_temp = 0, p95_vr_temp = 0, p95_power = 0,
				error_percent = NULL, accepted_delta = 0, rejected_delta = 0,
				measured_at = ? WHERE mac_addr = ? AND frequency = ? AND core_voltage = ?
				AND status = ? AND entry_attempt_id = ?`,
				PointUnobservable, timeValue(measuredAt), attempt.MacAddr,
				attempt.TargetFrequency, attempt.TargetCoreVoltage, PointEntered, attempt.ID); err != nil {
				return fmt.Errorf("quarantine mutation: finalize candidate: %w", err)
			}
		}
	}
	attempt.FailedAt = at
	attempt.FailureStage = stage
	if err := validateMutationAttempt(attempt, true); err != nil {
		return fmt.Errorf("quarantine mutation: %w", err)
	}
	if err := saveMiner(tx, state); err != nil {
		return fmt.Errorf("quarantine mutation: save state: %w", err)
	}
	if _, err := tx.ExecContext(context.Background(), `UPDATE mutation_attempts
		SET failed_at = ?, failure_stage = ? WHERE id = ? AND failed_at = 0`,
		timeValue(attempt.FailedAt), attempt.FailureStage, id); err != nil {
		return fmt.Errorf("quarantine mutation: close attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("quarantine mutation: commit: %w", err)
	}
	rollback = false
	return nil
}

// SupersedeMutation closes a normal or mining mutation that safety arbitration
// replaced, finalizing an entered candidate as unobservable when necessary and
// saving the replacing durable state in the same transaction.
func (store *OptimizerStore) SupersedeMutation(
	expected *MinerState,
	state *MinerState,
	id int64,
	at time.Time,
) error {
	if expected == nil || state == nil || expected.MacAddr != state.MacAddr || id <= 0 || at.IsZero() {
		return fmt.Errorf("supersede mutation: invalid state or attempt")
	}
	if err := validateMinerState(*expected); err != nil {
		return fmt.Errorf("supersede mutation: expected state: %w", err)
	}
	if err := validateMinerState(*state); err != nil {
		return fmt.Errorf("supersede mutation: %w", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("supersede mutation"); err != nil {
		return err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("supersede mutation: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	attempt, err := queryMutationAttempt(tx, id)
	if err != nil {
		return fmt.Errorf("supersede mutation: load attempt: %w", err)
	}
	if attempt.MacAddr != state.MacAddr {
		return fmt.Errorf("supersede mutation: miner does not match attempt")
	}
	durable, err := queryMiner(tx, state.MacAddr)
	if err != nil {
		return fmt.Errorf("supersede mutation: load miner: %w", err)
	}
	if durable != *expected {
		return fmt.Errorf("supersede mutation: stale miner state")
	}
	if !attempt.FailedAt.IsZero() {
		if attempt.FailureStage != MutationFailureSafetySuperseded || !attempt.FailedAt.Equal(at) {
			return fmt.Errorf("supersede mutation: failure milestone conflicts with stored value")
		}
	} else if attempt.MiningResumedAt.IsZero() {
		attempt.FailedAt = at
		attempt.FailureStage = MutationFailureSafetySuperseded
	}
	if attempt.FailureStage != MutationFailureSafetySuperseded {
		return fmt.Errorf("supersede mutation: attempt %d has failure stage %q", id, attempt.FailureStage)
	}
	if err := validateMutationAttempt(attempt, true); err != nil {
		return fmt.Errorf("supersede mutation: %w", err)
	}
	if attempt.Kind == MutationOperatingPoint {
		var record OperatingPointRecord
		var errorPercent sql.NullFloat64
		var measuredAt, enteredAt int64
		err := tx.QueryRowContext(context.Background(), `SELECT
			mac_addr, frequency, core_voltage, status, median_hash, expected_hash,
			attainment, mean_temp, p95_temp, p95_vr_temp, p95_power, error_percent,
			accepted_delta, rejected_delta, measured_at, entered_at, entry_attempt_id,
			reference_hash FROM operating_points WHERE mac_addr = ? AND frequency = ? AND core_voltage = ?`,
			attempt.MacAddr, attempt.TargetFrequency, attempt.TargetCoreVoltage).Scan(
			&record.MacAddr, &record.Frequency, &record.CoreVoltage, &record.Status,
			&record.MedianHash, &record.ExpectedHash, &record.Attainment,
			&record.MeanTemp, &record.P95Temp, &record.P95VRTemp, &record.P95Power,
			&errorPercent, &record.AcceptedDelta, &record.RejectedDelta,
			&measuredAt, &enteredAt, &record.EntryAttemptID, &record.ReferenceHash,
		)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("supersede mutation: load target point: %w", err)
		}
		if err == nil && record.Status == PointEntered && record.EntryAttemptID == id {
			if errorPercent.Valid {
				value := errorPercent.Float64
				record.ErrorPercent = &value
			}
			record.MeasuredAt = storedTime(measuredAt)
			record.EnteredAt = storedTime(enteredAt)
			record.Status = PointUnobservable
			record.MedianHash = 0
			record.ExpectedHash = 0
			record.Attainment = 0
			record.MeanTemp = 0
			record.P95Temp = 0
			record.P95VRTemp = 0
			record.P95Power = 0
			record.ErrorPercent = nil
			record.AcceptedDelta = 0
			record.RejectedDelta = 0
			record.MeasuredAt = at
			if record.MeasuredAt.Before(record.EnteredAt) {
				record.MeasuredAt = record.EnteredAt
			}
			if _, err := tx.ExecContext(context.Background(), `UPDATE operating_points SET
				status = ?, median_hash = 0, expected_hash = 0, attainment = 0,
				mean_temp = 0, p95_temp = 0, p95_vr_temp = 0, p95_power = 0,
				error_percent = NULL, accepted_delta = 0, rejected_delta = 0,
				measured_at = ? WHERE mac_addr = ? AND frequency = ? AND core_voltage = ?
				AND status = ? AND entry_attempt_id = ?`,
				record.Status, timeValue(record.MeasuredAt), record.MacAddr,
				record.Frequency, record.CoreVoltage, PointEntered, id); err != nil {
				return fmt.Errorf("supersede mutation: finalize target point: %w", err)
			}
		}
	}
	if err := saveMiner(tx, state); err != nil {
		return fmt.Errorf("supersede mutation: save state: %w", err)
	}
	if _, err := tx.ExecContext(context.Background(),
		`UPDATE mutation_attempts SET failed_at = ?, failure_stage = ?
		 WHERE id = ? AND failed_at = 0 AND mining_resumed_at = 0`,
		timeValue(attempt.FailedAt), attempt.FailureStage, id); err != nil {
		return fmt.Errorf("supersede mutation: close attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("supersede mutation: commit: %w", err)
	}
	rollback = false
	return nil
}

// PersistSafetyTransition atomically records an instantaneous safety result,
// closes any normal or mining attempt that it superseded, and saves the
// replacement safety state. The caller supplies a terminal point record only
// when the live point was an entered frontier candidate.
func (store *OptimizerStore) PersistSafetyTransition(
	expected *MinerState,
	state *MinerState,
	record *OperatingPointRecord,
	at time.Time,
) error {
	if expected == nil || state == nil || expected.MacAddr != state.MacAddr || at.IsZero() {
		return fmt.Errorf("persist safety transition: invalid state or timestamp")
	}
	if err := validateMinerState(*expected); err != nil {
		return fmt.Errorf("persist safety transition: expected state: %w", err)
	}
	if err := validateMinerState(*state); err != nil {
		return fmt.Errorf("persist safety transition: %w", err)
	}
	if record != nil {
		if record.MacAddr != state.MacAddr || record.Status == PointEntered {
			return fmt.Errorf("persist safety transition: invalid point record")
		}
		if err := validatePointRecord(*record); err != nil {
			return fmt.Errorf("persist safety transition: %w", err)
		}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("persist safety transition"); err != nil {
		return err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("persist safety transition: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	durable, err := queryMiner(tx, state.MacAddr)
	if err != nil {
		return fmt.Errorf("persist safety transition: load miner: %w", err)
	}
	if durable != *expected {
		return fmt.Errorf("persist safety transition: stale miner state")
	}
	if record != nil {
		var current OperatingPointRecord
		var errorPercent sql.NullFloat64
		var measuredAt, enteredAt int64
		if err := tx.QueryRowContext(context.Background(), `SELECT
			mac_addr, frequency, core_voltage, status, median_hash, expected_hash,
			attainment, mean_temp, p95_temp, p95_vr_temp, p95_power, error_percent,
			accepted_delta, rejected_delta, measured_at, entered_at, entry_attempt_id,
			reference_hash FROM operating_points WHERE mac_addr = ? AND frequency = ? AND core_voltage = ?`,
			record.MacAddr, record.Frequency, record.CoreVoltage).Scan(
			&current.MacAddr, &current.Frequency, &current.CoreVoltage, &current.Status,
			&current.MedianHash, &current.ExpectedHash, &current.Attainment,
			&current.MeanTemp, &current.P95Temp, &current.P95VRTemp, &current.P95Power,
			&errorPercent, &current.AcceptedDelta, &current.RejectedDelta,
			&measuredAt, &enteredAt, &current.EntryAttemptID, &current.ReferenceHash,
		); err != nil {
			return fmt.Errorf("persist safety transition: load point: %w", err)
		}
		current.MeasuredAt = storedTime(measuredAt)
		current.EnteredAt = storedTime(enteredAt)
		if current.EntryAttemptID != record.EntryAttemptID {
			return fmt.Errorf("persist safety transition: point entry authority changed")
		}
		if current.Status == PointEntered {
			if _, err := tx.ExecContext(context.Background(), `UPDATE operating_points SET
				status = ?, median_hash = ?, expected_hash = ?, attainment = ?, mean_temp = ?,
				p95_temp = ?, p95_vr_temp = ?, p95_power = ?, error_percent = ?,
				accepted_delta = ?, rejected_delta = ?, measured_at = ?
				WHERE mac_addr = ? AND frequency = ? AND core_voltage = ? AND status = ?`,
				record.Status, record.MedianHash, record.ExpectedHash, record.Attainment,
				record.MeanTemp, record.P95Temp, record.P95VRTemp, record.P95Power,
				nullableFloat(record.ErrorPercent), record.AcceptedDelta, record.RejectedDelta,
				timeValue(record.MeasuredAt), record.MacAddr, record.Frequency,
				record.CoreVoltage, PointEntered); err != nil {
				return fmt.Errorf("persist safety transition: finalize point: %w", err)
			}
		} else if current.Status != record.Status || !current.MeasuredAt.Equal(record.MeasuredAt) {
			return fmt.Errorf("persist safety transition: point is already finalized differently")
		}
	}
	var attempt MutationAttempt
	attempt, err = scanMutationAttempt(tx.QueryRowContext(context.Background(), mutationAttemptSelect+
		` WHERE mac_addr = ? AND failed_at = 0 AND mining_resumed_at = 0 ORDER BY id DESC LIMIT 1`, state.MacAddr))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("persist safety transition: load unfinished attempt: %w", err)
	}
	if err == nil {
		closeAttempt := attempt.Kind == MutationOperatingPoint || attempt.Kind == MutationMiningConfiguration
		if attempt.Kind == MutationSafetyRollback || attempt.Kind == MutationOverheatRecovery {
			closeAttempt = state.PendingKind != attempt.Kind || state.PendingPoint() != attempt.TargetPoint() ||
				state.SafetyReason != attempt.Reason
		}
		if closeAttempt && attempt.Kind == MutationOperatingPoint && record == nil {
			var status string
			var enteredAt int64
			var entryAttemptID int64
			if err := tx.QueryRowContext(context.Background(), `SELECT status, entered_at, entry_attempt_id
				FROM operating_points WHERE mac_addr = ? AND frequency = ? AND core_voltage = ?`,
				attempt.MacAddr, attempt.TargetFrequency, attempt.TargetCoreVoltage).Scan(
				&status, &enteredAt, &entryAttemptID); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("persist safety transition: load superseded candidate: %w", err)
			} else if err == nil && status == PointEntered && entryAttemptID == attempt.ID {
				measuredAt := at
				if measuredAt.Before(storedTime(enteredAt)) {
					measuredAt = storedTime(enteredAt)
				}
				if _, err := tx.ExecContext(context.Background(), `UPDATE operating_points SET
					status = ?, median_hash = 0, expected_hash = 0, attainment = 0,
					mean_temp = 0, p95_temp = 0, p95_vr_temp = 0, p95_power = 0,
					error_percent = NULL, accepted_delta = 0, rejected_delta = 0,
					measured_at = ? WHERE mac_addr = ? AND frequency = ? AND core_voltage = ?
					AND status = ? AND entry_attempt_id = ?`,
					PointUnobservable, timeValue(measuredAt), attempt.MacAddr,
					attempt.TargetFrequency, attempt.TargetCoreVoltage, PointEntered, attempt.ID); err != nil {
					return fmt.Errorf("persist safety transition: finalize superseded candidate: %w", err)
				}
			}
		}
		if closeAttempt {
			attempt.FailedAt = at
			attempt.FailureStage = MutationFailureSafetySuperseded
			if err := validateMutationAttempt(attempt, true); err != nil {
				return fmt.Errorf("persist safety transition: close attempt: %w", err)
			}
			if _, err := tx.ExecContext(context.Background(), `UPDATE mutation_attempts
				SET failed_at = ?, failure_stage = ? WHERE id = ? AND failed_at = 0`,
				timeValue(attempt.FailedAt), attempt.FailureStage, attempt.ID); err != nil {
				return fmt.Errorf("persist safety transition: close attempt: %w", err)
			}
		}
	}
	if err := saveMiner(tx, state); err != nil {
		return fmt.Errorf("persist safety transition: save state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("persist safety transition: commit: %w", err)
	}
	rollback = false
	return nil
}

// CompleteMutationAttempt atomically saves the verified device state and
// records that the mutation lifecycle completed.
func (store *OptimizerStore) CompleteMutationAttempt(
	state *MinerState,
	id int64,
	at time.Time,
) error {
	if state == nil {
		return fmt.Errorf("complete mutation attempt: state is nil")
	}
	if id <= 0 {
		return fmt.Errorf("complete mutation attempt: ID must be positive")
	}
	if at.IsZero() {
		return fmt.Errorf("complete mutation attempt: timestamp is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("complete mutation attempt"); err != nil {
		return err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("complete mutation attempt: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	attempt, err := queryMutationAttempt(tx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("complete mutation attempt: attempt %d does not exist", id)
	}
	if err != nil {
		return fmt.Errorf("complete mutation attempt: %w", err)
	}
	if attempt.MacAddr != state.MacAddr {
		return fmt.Errorf("complete mutation attempt: miner does not match attempt")
	}
	durable, err := queryMiner(tx, attempt.MacAddr)
	if err != nil {
		return fmt.Errorf("complete mutation attempt: load durable miner: %w", err)
	}
	if !attempt.CompletedAt.IsZero() {
		if !attempt.CompletedAt.Equal(at) {
			return fmt.Errorf("complete mutation attempt: completion timestamp conflicts with stored value")
		}
		if err := validateCompletedMutationShape(durable, attempt); err != nil {
			return fmt.Errorf("complete mutation attempt: stored completion is invalid: %w", err)
		}
		*state = durable
		return nil
	}
	if !attempt.FailedAt.IsZero() {
		return fmt.Errorf("complete mutation attempt: attempt already failed")
	}
	if state.IP == "" || net.ParseIP(state.IP) == nil || net.ParseIP(state.IP).To4() == nil {
		return fmt.Errorf("complete mutation attempt: caller has no valid rediscovered IP")
	}
	if state.RampUntil.IsZero() || state.RampUntil.Before(at) {
		return fmt.Errorf("complete mutation attempt: caller has no post-mutation ramp")
	}
	next, err := deriveCompletedMutationState(durable, attempt, state.IP, state.RampUntil, at)
	if err != nil {
		return fmt.Errorf("complete mutation attempt: %w", err)
	}
	attempt.CompletedAt = at
	if err := validateMutationAttempt(attempt, true); err != nil {
		return fmt.Errorf("complete mutation attempt: %w", err)
	}
	if err := saveMinerAfterMutationCompletion(tx, &next); err != nil {
		return fmt.Errorf("complete mutation attempt: %w", err)
	}
	if _, err := tx.ExecContext(
		context.Background(),
		"UPDATE mutation_attempts SET completed_at = ? WHERE id = ?",
		timeValue(attempt.CompletedAt),
		id,
	); err != nil {
		return fmt.Errorf("complete mutation attempt: update history: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("complete mutation attempt: commit: %w", err)
	}
	rollback = false
	*state = next
	return nil
}

func deriveCompletedMutationState(
	durable MinerState,
	attempt MutationAttempt,
	ip string,
	rampUntil time.Time,
	completedAt time.Time,
) (MinerState, error) {
	next := durable
	next.IP = ip
	next.EvidenceDeadlineAt = time.Time{}
	next.SettledAt = time.Time{}
	next.RampUntil = rampUntil
	next.ObservedFrequency = 0
	next.ObservedCoreVoltage = 0
	next.ObservedCount = 0
	if attempt.Kind == MutationMiningConfiguration {
		if !durable.MiningPending || durable.PendingKind != "" {
			return MinerState{}, fmt.Errorf("durable mining obligation is no longer pending")
		}
		next.MiningPending = false
		return next, nil
	}
	if durable.PendingKind != attempt.Kind || durable.PendingPoint() != attempt.TargetPoint() {
		return MinerState{}, fmt.Errorf("durable operating-point intent is no longer pending")
	}
	if attempt.Kind == MutationOperatingPoint && durable.MiningPending {
		return MinerState{}, fmt.Errorf("operating-point completion overlaps mining obligation")
	}
	phaseBefore := durable.Phase
	currentBefore := durable.CurrentPoint()
	fallbackBefore := durable.FallbackPoint()
	entryTrial := (phaseBefore == PhaseUndervolt || phaseBefore == PhaseFrequencyTest || phaseBefore == PhaseVoltageTest) &&
		currentBefore == fallbackBefore && durable.PendingPoint() != fallbackBefore
	reservedReturn := (phaseBefore == PhaseUndervolt || phaseBefore == PhaseFrequencyTest || phaseBefore == PhaseVoltageTest) &&
		currentBefore != fallbackBefore && durable.PendingPoint() == fallbackBefore
	finalPlacement := attempt.Kind == MutationOperatingPoint && phaseBefore == PhaseHold && durable.HoldReason == HoldOptimized
	next.SetCurrentPoint(attempt.TargetPoint())
	next.ClearPendingMutation()
	if !entryTrial {
		next.SetFallbackPoint(OperatingPoint{})
	}
	switch {
	case attempt.Kind == MutationOverheatRecovery || attempt.Kind == MutationSafetyRollback:
		next.Phase = PhaseCooldown
		next.PhaseStartedAt = completedAt
		next.HoldReason = ""
	case reservedReturn:
		next.Phase = PhaseBaseline
		next.PhaseStartedAt = completedAt
		next.HoldReason = ""
	case entryTrial:
		// Keep the trial phase and reserved fallback for its two fresh windows.
	case finalPlacement:
		next.Phase = PhaseHold
		next.HoldReason = HoldOptimized
		next.PhaseStartedAt = completedAt
	default:
		next.Phase = PhaseBaseline
		next.HoldReason = ""
		next.PhaseStartedAt = completedAt
	}
	return next, nil
}

func validateCompletedMutationShape(state MinerState, attempt MutationAttempt) error {
	if state.PendingKind != "" || !state.EvidenceDeadlineAt.IsZero() ||
		(state.MiningPending && attempt.Kind != MutationMiningConfiguration &&
			attempt.Kind != MutationSafetyRollback && attempt.Kind != MutationOverheatRecovery) {
		return fmt.Errorf("completed state retains pending authority")
	}
	if state.RampUntil.IsZero() || state.RampUntil.Before(attempt.CompletedAt) {
		return fmt.Errorf("completed state has no post-mutation ramp")
	}
	if attempt.Kind == MutationMiningConfiguration {
		if state.MiningPending {
			return fmt.Errorf("mining obligation remains pending")
		}
		if state.Phase == PhaseHold && state.HoldReason != HoldOptimized && state.HoldReason != HoldManual {
			return fmt.Errorf("mining completion has invalid HOLD state")
		}
		return nil
	}
	if state.CurrentPoint() != attempt.TargetPoint() {
		return fmt.Errorf("operating-point state does not match target")
	}
	switch attempt.Kind {
	case MutationOperatingPoint:
		switch state.Phase {
		case PhaseBaseline:
			if state.FallbackPoint() != (OperatingPoint{}) || state.HoldReason != "" {
				return fmt.Errorf("baseline completion retains trial state")
			}
		case PhaseUndervolt, PhaseFrequencyTest, PhaseVoltageTest:
			if !validStoredPoint(state.FallbackPoint()) || state.HoldReason != "" {
				return fmt.Errorf("trial completion has no reserved incumbent")
			}
		case PhaseHold:
			if state.HoldReason != HoldOptimized || state.FallbackPoint() != (OperatingPoint{}) {
				return fmt.Errorf("final placement completion has invalid HOLD state")
			}
		default:
			return fmt.Errorf("operating-point completion has invalid phase %q", state.Phase)
		}
	case MutationSafetyRollback, MutationOverheatRecovery:
		if state.Phase != PhaseCooldown || state.HoldReason != "" || state.SafetyReason == "" {
			return fmt.Errorf("safety completion has invalid cooldown state")
		}
	default:
		return fmt.Errorf("unsupported completed mutation kind %q", attempt.Kind)
	}
	return nil
}

// PendingMutationResume returns the completed attempt still waiting for two
// consecutive healthy mining observations.
func (store *OptimizerStore) PendingMutationResume(
	macAddr string,
) (MutationAttempt, bool, error) {
	if strings.TrimSpace(macAddr) == "" {
		return MutationAttempt{}, false, fmt.Errorf(
			"pending mutation resume: MAC address is empty",
		)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("pending mutation resume"); err != nil {
		return MutationAttempt{}, false, err
	}
	attempt, err := queryPendingMutationResume(store.conn, macAddr)
	if errors.Is(err, sql.ErrNoRows) {
		return MutationAttempt{}, false, nil
	}
	if err != nil {
		return MutationAttempt{}, false, fmt.Errorf("pending mutation resume: %w", err)
	}
	return attempt, true, nil
}

// UnfinishedMutationAttempt returns the single durable lifecycle obligation
// for a miner, including an entry that has already rebooted but has not yet
// reached its two healthy resumption polls.
func (store *OptimizerStore) UnfinishedMutationAttempt(
	macAddr string,
) (MutationAttempt, bool, error) {
	if strings.TrimSpace(macAddr) == "" {
		return MutationAttempt{}, false, fmt.Errorf("unfinished mutation attempt: MAC address is empty")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("unfinished mutation attempt"); err != nil {
		return MutationAttempt{}, false, err
	}
	attempt, err := scanMutationAttempt(store.conn.QueryRowContext(context.Background(),
		mutationAttemptSelect+` WHERE mac_addr = ? AND failed_at = 0
		AND mining_resumed_at = 0 ORDER BY id DESC LIMIT 1`, macAddr))
	if errors.Is(err, sql.ErrNoRows) {
		return MutationAttempt{}, false, nil
	}
	if err != nil {
		return MutationAttempt{}, false, fmt.Errorf("unfinished mutation attempt: %w", err)
	}
	return attempt, true, nil
}

// ListMutationAttempts returns durable mutation attempts in creation order.
func (store *OptimizerStore) ListMutationAttempts(
	macAddr string,
) ([]MutationAttempt, error) {
	if strings.TrimSpace(macAddr) == "" {
		return nil, fmt.Errorf("list mutation attempts: MAC address is empty")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("list mutation attempts"); err != nil {
		return nil, err
	}
	rows, err := store.conn.QueryContext(
		context.Background(),
		mutationAttemptSelect+` WHERE mac_addr = ? ORDER BY id`,
		macAddr,
	)
	if err != nil {
		return nil, fmt.Errorf("list mutation attempts: %w", err)
	}
	defer rows.Close()
	attempts, err := scanMutationAttempts(rows)
	if err != nil {
		return nil, fmt.Errorf("list mutation attempts: %w", err)
	}
	return attempts, nil
}

// HourlyAggregate is the credential-free, bounded reporting row persisted for
// one UTC hour. It contains no optimizer authority and no raw telemetry.
type HourlyAggregate struct {
	MacAddr                            string
	HourStartedAt                      time.Time
	ObservedSeconds                    float64
	UnknownGapSeconds                  float64
	ActualHashSeconds                  float64
	TrialActualHashSeconds             float64
	IncumbentCounterfactualHashSeconds float64
	SettledSeconds                     float64
	TrialSeconds                       float64
}

// CompareAndSetHourly applies additive hourly fragments and advances the
// miner's accounting cursor only when it still equals expectedCursor. The
// cursor is the idempotence boundary across reopen and ambiguous commits.
func (store *OptimizerStore) CompareAndSetHourly(
	macAddr string,
	expectedCursor time.Time,
	nextCursor time.Time,
	fragments []HourlyAggregate,
	now time.Time,
) error {
	if strings.TrimSpace(macAddr) == "" || expectedCursor.IsZero() || nextCursor.IsZero() ||
		nextCursor.Before(expectedCursor) || now.IsZero() {
		return fmt.Errorf("account hourly: invalid cursor")
	}
	if !nextCursor.After(expectedCursor) {
		return nil
	}
	for _, fragment := range fragments {
		if fragment.MacAddr != macAddr {
			return fmt.Errorf("account hourly: fragment MAC does not match cursor")
		}
		if err := validateHourlyAggregate(fragment); err != nil {
			return fmt.Errorf("account hourly: %w", err)
		}
	}
	coverageStart := expectedCursor.UTC()
	retentionStart := now.UTC().Add(-LongTermRetentionHours * time.Hour)
	if coverageStart.Before(retentionStart) {
		coverageStart = retentionStart
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("account hourly"); err != nil {
		return err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("account hourly: begin transaction: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	var cursor int64
	if err := tx.QueryRowContext(context.Background(),
		"SELECT accounted_through_at FROM optimizer_miners WHERE mac_addr = ?", macAddr,
	).Scan(&cursor); err != nil {
		return fmt.Errorf("account hourly: load cursor: %w", err)
	}
	if storedTime(cursor) != expectedCursor.UTC() {
		return fmt.Errorf("account hourly: %w", ErrAccountingCursorChanged)
	}
	if err := validateHourlyCoverage(coverageStart, nextCursor.UTC(), fragments); err != nil {
		return fmt.Errorf("account hourly: %w", err)
	}
	for _, fragment := range fragments {
		hour := fragment.HourStartedAt.UTC()
		_, err := tx.ExecContext(context.Background(), `INSERT INTO optimizer_hourly (
			mac_addr, hour_started_at, observed_seconds, unknown_gap_seconds,
			actual_hash_seconds, trial_actual_hash_seconds,
			incumbent_counterfactual_hash_seconds, settled_seconds, trial_seconds
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(mac_addr, hour_started_at) DO UPDATE SET
			observed_seconds = observed_seconds + excluded.observed_seconds,
			unknown_gap_seconds = unknown_gap_seconds + excluded.unknown_gap_seconds,
			actual_hash_seconds = actual_hash_seconds + excluded.actual_hash_seconds,
			trial_actual_hash_seconds = trial_actual_hash_seconds + excluded.trial_actual_hash_seconds,
			incumbent_counterfactual_hash_seconds = incumbent_counterfactual_hash_seconds + excluded.incumbent_counterfactual_hash_seconds,
			settled_seconds = settled_seconds + excluded.settled_seconds,
			trial_seconds = trial_seconds + excluded.trial_seconds`,
			macAddr, hour.Unix(), fragment.ObservedSeconds, fragment.UnknownGapSeconds,
			fragment.ActualHashSeconds, fragment.TrialActualHashSeconds,
			fragment.IncumbentCounterfactualHashSeconds, fragment.SettledSeconds,
			fragment.TrialSeconds,
		)
		if err != nil {
			return fmt.Errorf("account hourly: save %s: %w", hour, err)
		}
	}
	for _, fragment := range fragments {
		var aggregate HourlyAggregate
		var hour int64
		if err := tx.QueryRowContext(context.Background(), `SELECT
			mac_addr, hour_started_at, observed_seconds, unknown_gap_seconds,
			actual_hash_seconds, trial_actual_hash_seconds,
			incumbent_counterfactual_hash_seconds, settled_seconds, trial_seconds
			FROM optimizer_hourly WHERE mac_addr = ? AND hour_started_at = ?`,
			macAddr, fragment.HourStartedAt.UTC().Unix()).Scan(
			&aggregate.MacAddr, &hour, &aggregate.ObservedSeconds,
			&aggregate.UnknownGapSeconds, &aggregate.ActualHashSeconds,
			&aggregate.TrialActualHashSeconds,
			&aggregate.IncumbentCounterfactualHashSeconds,
			&aggregate.SettledSeconds, &aggregate.TrialSeconds,
		); err != nil {
			return fmt.Errorf("account hourly: validate merged row: %w", err)
		}
		aggregate.HourStartedAt = time.Unix(hour, 0).UTC()
		if err := validateHourlyAggregate(aggregate); err != nil {
			return fmt.Errorf("account hourly: validate merged row: %w", err)
		}
	}
	result, err := tx.ExecContext(context.Background(),
		`UPDATE optimizer_miners SET accounted_through_at = ?
		 WHERE mac_addr = ? AND accounted_through_at = ?`,
		timeValue(nextCursor), macAddr, timeValue(expectedCursor))
	if err != nil {
		return fmt.Errorf("account hourly: advance cursor: %w", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return fmt.Errorf("account hourly: %w", ErrAccountingCursorChanged)
	}
	cutoff := now.UTC().Add(-LongTermRetentionHours * time.Hour).Unix()
	if _, err := tx.ExecContext(context.Background(),
		"DELETE FROM optimizer_hourly WHERE hour_started_at + 3600 <= ?", cutoff); err != nil {
		return fmt.Errorf("account hourly: trim retention: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("account hourly: commit: %w", err)
	}
	rollback = false
	return nil
}

// ListHourly returns persisted hourly aggregates in UTC hour order.
func (store *OptimizerStore) ListHourly(macAddr string, from, to time.Time) ([]HourlyAggregate, error) {
	if strings.TrimSpace(macAddr) == "" || from.IsZero() || to.IsZero() || !to.After(from) {
		return nil, fmt.Errorf("list hourly aggregates: invalid range")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("list hourly aggregates"); err != nil {
		return nil, err
	}
	rows, err := store.conn.QueryContext(context.Background(), `SELECT
		mac_addr, hour_started_at, observed_seconds, unknown_gap_seconds,
		actual_hash_seconds, trial_actual_hash_seconds,
		incumbent_counterfactual_hash_seconds, settled_seconds, trial_seconds
		FROM optimizer_hourly WHERE mac_addr = ? AND hour_started_at >= ?
		AND hour_started_at < ? ORDER BY hour_started_at`,
		macAddr, from.UTC().Truncate(time.Hour).Unix(), to.UTC().Unix())
	if err != nil {
		return nil, fmt.Errorf("list hourly aggregates: %w", err)
	}
	defer rows.Close()
	var aggregates []HourlyAggregate
	for rows.Next() {
		var aggregate HourlyAggregate
		var hour int64
		if err := rows.Scan(&aggregate.MacAddr, &hour, &aggregate.ObservedSeconds,
			&aggregate.UnknownGapSeconds, &aggregate.ActualHashSeconds,
			&aggregate.TrialActualHashSeconds,
			&aggregate.IncumbentCounterfactualHashSeconds,
			&aggregate.SettledSeconds, &aggregate.TrialSeconds); err != nil {
			return nil, fmt.Errorf("list hourly aggregates: %w", err)
		}
		aggregate.HourStartedAt = time.Unix(hour, 0).UTC()
		if err := validateHourlyAggregate(aggregate); err != nil {
			return nil, fmt.Errorf("list hourly aggregates: %w", err)
		}
		aggregates = append(aggregates, aggregate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list hourly aggregates: %w", err)
	}
	return aggregates, nil
}

func validateHourlyAggregate(aggregate HourlyAggregate) error {
	if aggregate.HourStartedAt.IsZero() || aggregate.HourStartedAt.Unix() <= 0 ||
		aggregate.HourStartedAt.UTC().Truncate(time.Hour) != aggregate.HourStartedAt.UTC() {
		return fmt.Errorf("hour is not a positive UTC hour")
	}
	values := []float64{
		aggregate.ObservedSeconds,
		aggregate.UnknownGapSeconds,
		aggregate.ActualHashSeconds,
		aggregate.TrialActualHashSeconds,
		aggregate.IncumbentCounterfactualHashSeconds,
		aggregate.SettledSeconds,
		aggregate.TrialSeconds,
	}
	for _, value := range values {
		if !finite(value) || value < 0 {
			return fmt.Errorf("hour contains invalid aggregate")
		}
	}
	if aggregate.ObservedSeconds+aggregate.UnknownGapSeconds > 3600 ||
		aggregate.SettledSeconds > aggregate.ObservedSeconds ||
		aggregate.TrialSeconds > aggregate.ObservedSeconds ||
		aggregate.TrialActualHashSeconds > aggregate.ActualHashSeconds {
		return fmt.Errorf("hour aggregate violates bounds")
	}
	return nil
}

func validateHourlyCoverage(start, end time.Time, fragments []HourlyAggregate) error {
	if !end.After(start) {
		if len(fragments) != 0 {
			return fmt.Errorf("hourly fragments exist for an empty retained interval")
		}
		return nil
	}
	cursor := start.UTC()
	for _, fragment := range fragments {
		hour := cursor.Truncate(time.Hour)
		if !fragment.HourStartedAt.Equal(hour) {
			return fmt.Errorf("hourly fragments have a gap or overlap at %s", cursor)
		}
		segmentEnd := hour.Add(time.Hour)
		if segmentEnd.After(end.UTC()) {
			segmentEnd = end.UTC()
		}
		expectedSeconds := segmentEnd.Sub(cursor).Seconds()
		coveredSeconds := fragment.ObservedSeconds + fragment.UnknownGapSeconds
		if math.Abs(coveredSeconds-expectedSeconds) > 1e-9 {
			return fmt.Errorf("hourly fragment at %s covers %.9f seconds, expected %.9f", hour, coveredSeconds, expectedSeconds)
		}
		cursor = segmentEnd
	}
	if !cursor.Equal(end.UTC()) {
		return fmt.Errorf("hourly fragments stop at %s, expected %s", cursor, end.UTC())
	}
	return nil
}

func scanPointRows(rows *sql.Rows) ([]OperatingPointRecord, error) {
	var records []OperatingPointRecord
	for rows.Next() {
		var record OperatingPointRecord
		var errorPercent sql.NullFloat64
		var measuredAt int64
		var enteredAt int64
		if err := rows.Scan(
			&record.MacAddr,
			&record.Frequency,
			&record.CoreVoltage,
			&record.Status,
			&record.MedianHash,
			&record.ExpectedHash,
			&record.Attainment,
			&record.MeanTemp,
			&record.P95Temp,
			&record.P95VRTemp,
			&record.P95Power,
			&errorPercent,
			&record.AcceptedDelta,
			&record.RejectedDelta,
			&measuredAt,
			&enteredAt,
			&record.EntryAttemptID,
			&record.ReferenceHash,
		); err != nil {
			return nil, err
		}
		if errorPercent.Valid {
			value := errorPercent.Float64
			record.ErrorPercent = &value
		}
		record.MeasuredAt = storedTime(measuredAt)
		record.EnteredAt = storedTime(enteredAt)
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

const mutationAttemptSelect = `SELECT
	id, mac_addr, kind, reason, from_frequency, from_core_voltage,
	target_frequency, target_core_voltage, intent_created_at, started_at,
	patch_requested_at, configured_verified_at,
	configured_verified_uptime_seconds, restart_requested_at,
	reboot_verified_at, completed_at, first_positive_at,
	mining_resumed_at, failed_at, failure_stage
	FROM mutation_attempts`

func queryMutationAttempt(queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id int64) (MutationAttempt, error) {
	return scanMutationAttempt(
		queryer.QueryRowContext(
			context.Background(),
			mutationAttemptSelect+" WHERE id = ?",
			id,
		),
	)
}

func unfinishedMutationCount(queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, macAddr string) (int, error) {
	var count int
	err := queryer.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM mutation_attempts
		 WHERE mac_addr = ? AND failed_at = 0 AND mining_resumed_at = 0`,
		macAddr,
	).Scan(&count)
	return count, err
}

func queryPendingMutationResume(queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, macAddr string) (MutationAttempt, error) {
	return scanMutationAttempt(
		queryer.QueryRowContext(
			context.Background(),
			mutationAttemptSelect+`
			WHERE mac_addr = ? AND completed_at != 0
				AND mining_resumed_at = 0 AND failed_at = 0
			ORDER BY id DESC LIMIT 1`,
			macAddr,
		),
	)
}

type rowScanner interface {
	Scan(...any) error
}

func scanMutationAttempt(row rowScanner) (MutationAttempt, error) {
	var attempt MutationAttempt
	var kind string
	var reason string
	var failureStage string
	var intentCreatedAt int64
	var startedAt int64
	var patchRequestedAt int64
	var configuredVerifiedAt int64
	var configuredVerifiedUptimeSeconds int
	var restartRequestedAt int64
	var rebootVerifiedAt int64
	var completedAt int64
	var firstPositiveAt int64
	var miningResumedAt int64
	var failedAt int64
	if err := row.Scan(
		&attempt.ID,
		&attempt.MacAddr,
		&kind,
		&reason,
		&attempt.FromFrequency,
		&attempt.FromCoreVoltage,
		&attempt.TargetFrequency,
		&attempt.TargetCoreVoltage,
		&intentCreatedAt,
		&startedAt,
		&patchRequestedAt,
		&configuredVerifiedAt,
		&configuredVerifiedUptimeSeconds,
		&restartRequestedAt,
		&rebootVerifiedAt,
		&completedAt,
		&firstPositiveAt,
		&miningResumedAt,
		&failedAt,
		&failureStage,
	); err != nil {
		return MutationAttempt{}, err
	}
	attempt.Kind = MutationKind(kind)
	attempt.Reason = SafetyReason(reason)
	attempt.IntentCreatedAt = storedTime(intentCreatedAt)
	attempt.StartedAt = storedTime(startedAt)
	attempt.PatchRequestedAt = storedTime(patchRequestedAt)
	attempt.ConfiguredVerifiedAt = storedTime(configuredVerifiedAt)
	attempt.ConfiguredVerifiedUptimeSeconds = configuredVerifiedUptimeSeconds
	attempt.RestartRequestedAt = storedTime(restartRequestedAt)
	attempt.RebootVerifiedAt = storedTime(rebootVerifiedAt)
	attempt.CompletedAt = storedTime(completedAt)
	attempt.FirstPositiveAt = storedTime(firstPositiveAt)
	attempt.MiningResumedAt = storedTime(miningResumedAt)
	attempt.FailedAt = storedTime(failedAt)
	attempt.FailureStage = MutationFailureStage(failureStage)
	return attempt, nil
}

func scanMutationAttempts(rows *sql.Rows) ([]MutationAttempt, error) {
	var attempts []MutationAttempt
	for rows.Next() {
		attempt, err := scanMutationAttempt(rows)
		if err != nil {
			return nil, err
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return attempts, nil
}

func (store *OptimizerStore) Close() error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil
	}
	store.closed = true

	var result error
	if store.conn != nil {
		if err := store.conn.Close(); err != nil {
			result = fmt.Errorf("close optimizer database connection: %w", err)
		}
	}
	if store.db != nil {
		if err := store.db.Close(); err != nil && result == nil {
			result = fmt.Errorf("close optimizer database: %w", err)
		}
	}
	return result
}

func (store *OptimizerStore) ready(operation string) error {
	if store == nil || store.conn == nil || store.closed {
		return fmt.Errorf("%s: store is not initialized", operation)
	}
	return nil
}

func saveMiner(executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, state *MinerState) error {
	return saveMinerWithValidation(executor, state, false, true)
}

func saveMinerAfterMutationCompletion(executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, state *MinerState) error {
	return saveMinerWithValidation(executor, state, true, true)
}

func saveMinerForPassReset(executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, state *MinerState) error {
	return saveMinerWithValidation(executor, state, false, false)
}

func saveMinerWithValidation(
	executor interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	},
	state *MinerState,
	allowUnsettledHold bool,
	preservePassReference bool,
) error {
	if state == nil {
		return fmt.Errorf("save miner state: state is nil")
	}
	if err := validateMinerStateWithTransition(*state, allowUnsettledHold); err != nil {
		return fmt.Errorf("save miner state: %w", err)
	}
	_, err := executor.ExecContext(
		context.Background(),
		`INSERT INTO optimizer_miners (
			mac_addr, hostname, ip, phase, phase_started_at, ramp_until,
			current_frequency, current_core_voltage, best_frequency,
			best_core_voltage, best_hash_rate, fallback_frequency,
			fallback_core_voltage, pending_kind, pending_frequency,
			pending_core_voltage, pending_since, mining_pending,
			observed_frequency, observed_core_voltage, observed_count,
			consecutive_bad_windows, overheat_count, cooldown_until,
			pass_started_at, pass_trigger, pass_reference_hash,
			pass_reference_frequency, pass_reference_core_voltage, pass_reference_settled_at,
			safety_reason,
			hold_reason, settled_at, evidence_deadline_at, accounted_through_at
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?
		)
		ON CONFLICT(mac_addr) DO UPDATE SET
			hostname = excluded.hostname,
			ip = excluded.ip,
			phase = excluded.phase,
			phase_started_at = excluded.phase_started_at,
			ramp_until = excluded.ramp_until,
			current_frequency = excluded.current_frequency,
			current_core_voltage = excluded.current_core_voltage,
			best_frequency = excluded.best_frequency,
			best_core_voltage = excluded.best_core_voltage,
			best_hash_rate = excluded.best_hash_rate,
			fallback_frequency = excluded.fallback_frequency,
			fallback_core_voltage = excluded.fallback_core_voltage,
			pending_kind = excluded.pending_kind,
			pending_frequency = excluded.pending_frequency,
			pending_core_voltage = excluded.pending_core_voltage,
			pending_since = excluded.pending_since,
			mining_pending = excluded.mining_pending,
			observed_frequency = excluded.observed_frequency,
			observed_core_voltage = excluded.observed_core_voltage,
			observed_count = excluded.observed_count,
			consecutive_bad_windows = excluded.consecutive_bad_windows,
			overheat_count = excluded.overheat_count,
			cooldown_until = excluded.cooldown_until,
			pass_started_at = excluded.pass_started_at,
			pass_trigger = excluded.pass_trigger,
			pass_reference_hash = CASE WHEN optimizer_miners.pass_reference_hash > 0 AND ?
				THEN optimizer_miners.pass_reference_hash ELSE excluded.pass_reference_hash END,
			pass_reference_frequency = CASE WHEN ?
				THEN optimizer_miners.pass_reference_frequency ELSE excluded.pass_reference_frequency END,
			pass_reference_core_voltage = CASE WHEN ?
				THEN optimizer_miners.pass_reference_core_voltage ELSE excluded.pass_reference_core_voltage END,
			pass_reference_settled_at = CASE WHEN ?
				THEN optimizer_miners.pass_reference_settled_at ELSE excluded.pass_reference_settled_at END,
			safety_reason = excluded.safety_reason,
		hold_reason = excluded.hold_reason,
		settled_at = excluded.settled_at,
		evidence_deadline_at = excluded.evidence_deadline_at`,
		state.MacAddr,
		state.Hostname,
		state.IP,
		state.Phase,
		timeValue(state.PhaseStartedAt),
		timeValue(state.RampUntil),
		state.CurrentFrequency,
		state.CurrentCoreVoltage,
		state.BestFrequency,
		state.BestCoreVoltage,
		state.BestHashRate,
		state.FallbackFrequency,
		state.FallbackCoreVoltage,
		state.PendingKind,
		state.PendingFrequency,
		state.PendingCoreVoltage,
		timeValue(state.PendingSince),
		state.MiningPending,
		state.ObservedFrequency,
		state.ObservedCoreVoltage,
		state.ObservedCount,
		state.ConsecutiveBadWindows,
		state.OverheatCount,
		timeValue(state.CooldownUntil),
		timeValue(state.PassStartedAt),
		state.PassTrigger,
		state.PassReferenceHash,
		state.PassReferenceFrequency,
		state.PassReferenceCoreVoltage,
		timeValue(state.PassReferenceSettledAt),
		state.SafetyReason,
		state.HoldReason,
		timeValue(state.SettledAt),
		timeValue(state.EvidenceDeadlineAt),
		timeValue(state.AccountedThroughAt),
		preservePassReference,
		preservePassReference,
		preservePassReference,
		preservePassReference,
	)
	if err != nil {
		return fmt.Errorf("save miner state: %w", err)
	}
	return nil
}

const minerSelect = `SELECT
	mac_addr, hostname, ip, phase, phase_started_at, ramp_until,
	current_frequency, current_core_voltage, best_frequency,
	best_core_voltage, best_hash_rate, fallback_frequency,
	fallback_core_voltage, pending_kind, pending_frequency,
	pending_core_voltage, pending_since, mining_pending,
		observed_frequency, observed_core_voltage, observed_count,
		consecutive_bad_windows, overheat_count, cooldown_until,
		pass_started_at, pass_trigger, pass_reference_hash,
		pass_reference_frequency, pass_reference_core_voltage, pass_reference_settled_at,
		safety_reason,
		hold_reason, settled_at, evidence_deadline_at, accounted_through_at
	FROM optimizer_miners WHERE mac_addr = ?`

func queryMiner(queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, macAddr string) (MinerState, error) {
	var state MinerState
	var phase string
	var pendingKind string
	var phaseStartedAt int64
	var rampUntil int64
	var pendingSince int64
	var cooldownUntil int64
	var passStartedAt int64
	var passTrigger string
	var passReferenceSettledAt int64
	var safetyReason string
	var holdReason string
	var settledAt int64
	var evidenceDeadlineAt int64
	var accountedThroughAt int64
	err := queryer.QueryRowContext(context.Background(), minerSelect, macAddr).Scan(
		&state.MacAddr,
		&state.Hostname,
		&state.IP,
		&phase,
		&phaseStartedAt,
		&rampUntil,
		&state.CurrentFrequency,
		&state.CurrentCoreVoltage,
		&state.BestFrequency,
		&state.BestCoreVoltage,
		&state.BestHashRate,
		&state.FallbackFrequency,
		&state.FallbackCoreVoltage,
		&pendingKind,
		&state.PendingFrequency,
		&state.PendingCoreVoltage,
		&pendingSince,
		&state.MiningPending,
		&state.ObservedFrequency,
		&state.ObservedCoreVoltage,
		&state.ObservedCount,
		&state.ConsecutiveBadWindows,
		&state.OverheatCount,
		&cooldownUntil,
		&passStartedAt,
		&passTrigger,
		&state.PassReferenceHash,
		&state.PassReferenceFrequency,
		&state.PassReferenceCoreVoltage,
		&passReferenceSettledAt,
		&safetyReason,
		&holdReason,
		&settledAt,
		&evidenceDeadlineAt,
		&accountedThroughAt,
	)
	if err != nil {
		return MinerState{}, err
	}
	state.Phase = OptimizerPhase(phase)
	state.PendingKind = MutationKind(pendingKind)
	state.PhaseStartedAt = storedTime(phaseStartedAt)
	state.RampUntil = storedTime(rampUntil)
	state.PendingSince = storedTime(pendingSince)
	state.CooldownUntil = storedTime(cooldownUntil)
	state.PassStartedAt = storedTime(passStartedAt)
	state.PassTrigger = PassTrigger(passTrigger)
	state.PassReferenceSettledAt = storedTime(passReferenceSettledAt)
	state.SafetyReason = SafetyReason(safetyReason)
	state.HoldReason = HoldReason(holdReason)
	state.SettledAt = storedTime(settledAt)
	state.EvidenceDeadlineAt = storedTime(evidenceDeadlineAt)
	state.AccountedThroughAt = storedTime(accountedThroughAt)
	return state, nil
}

type schemaColumn struct {
	name    string
	sqlType string
	notNull int
	pk      int
}

var optimizerSchema = map[string][]schemaColumn{
	"mutation_attempts": {
		{name: "id", sqlType: "INTEGER", notNull: 1, pk: 1},
		{name: "mac_addr", sqlType: "TEXT", notNull: 1},
		{name: "kind", sqlType: "TEXT", notNull: 1},
		{name: "reason", sqlType: "TEXT", notNull: 1},
		{name: "from_frequency", sqlType: "INTEGER", notNull: 1},
		{name: "from_core_voltage", sqlType: "INTEGER", notNull: 1},
		{name: "target_frequency", sqlType: "INTEGER", notNull: 1},
		{name: "target_core_voltage", sqlType: "INTEGER", notNull: 1},
		{name: "intent_created_at", sqlType: "INTEGER", notNull: 1},
		{name: "started_at", sqlType: "INTEGER", notNull: 1},
		{name: "patch_requested_at", sqlType: "INTEGER", notNull: 1},
		{name: "configured_verified_at", sqlType: "INTEGER", notNull: 1},
		{name: "configured_verified_uptime_seconds", sqlType: "INTEGER", notNull: 1},
		{name: "restart_requested_at", sqlType: "INTEGER", notNull: 1},
		{name: "reboot_verified_at", sqlType: "INTEGER", notNull: 1},
		{name: "completed_at", sqlType: "INTEGER", notNull: 1},
		{name: "first_positive_at", sqlType: "INTEGER", notNull: 1},
		{name: "mining_resumed_at", sqlType: "INTEGER", notNull: 1},
		{name: "failed_at", sqlType: "INTEGER", notNull: 1},
		{name: "failure_stage", sqlType: "TEXT", notNull: 1},
	},
	"optimizer_miners": {
		{name: "mac_addr", sqlType: "TEXT", notNull: 1, pk: 1},
		{name: "hostname", sqlType: "TEXT", notNull: 1},
		{name: "ip", sqlType: "TEXT", notNull: 1},
		{name: "phase", sqlType: "TEXT", notNull: 1},
		{name: "phase_started_at", sqlType: "INTEGER", notNull: 1},
		{name: "ramp_until", sqlType: "INTEGER", notNull: 1},
		{name: "current_frequency", sqlType: "INTEGER", notNull: 1},
		{name: "current_core_voltage", sqlType: "INTEGER", notNull: 1},
		{name: "best_frequency", sqlType: "INTEGER", notNull: 1},
		{name: "best_core_voltage", sqlType: "INTEGER", notNull: 1},
		{name: "best_hash_rate", sqlType: "REAL", notNull: 1},
		{name: "fallback_frequency", sqlType: "INTEGER", notNull: 1},
		{name: "fallback_core_voltage", sqlType: "INTEGER", notNull: 1},
		{name: "pending_kind", sqlType: "TEXT", notNull: 1},
		{name: "pending_frequency", sqlType: "INTEGER", notNull: 1},
		{name: "pending_core_voltage", sqlType: "INTEGER", notNull: 1},
		{name: "pending_since", sqlType: "INTEGER", notNull: 1},
		{name: "mining_pending", sqlType: "INTEGER", notNull: 1},
		{name: "observed_frequency", sqlType: "INTEGER", notNull: 1},
		{name: "observed_core_voltage", sqlType: "INTEGER", notNull: 1},
		{name: "observed_count", sqlType: "INTEGER", notNull: 1},
		{name: "consecutive_bad_windows", sqlType: "INTEGER", notNull: 1},
		{name: "overheat_count", sqlType: "INTEGER", notNull: 1},
		{name: "cooldown_until", sqlType: "INTEGER", notNull: 1},
		{name: "pass_started_at", sqlType: "INTEGER", notNull: 1},
		{name: "pass_trigger", sqlType: "TEXT", notNull: 1},
		{name: "pass_reference_hash", sqlType: "REAL", notNull: 1},
		{name: "pass_reference_frequency", sqlType: "INTEGER", notNull: 1},
		{name: "pass_reference_core_voltage", sqlType: "INTEGER", notNull: 1},
		{name: "pass_reference_settled_at", sqlType: "INTEGER", notNull: 1},
		{name: "safety_reason", sqlType: "TEXT", notNull: 1},
		{name: "hold_reason", sqlType: "TEXT", notNull: 1},
		{name: "settled_at", sqlType: "INTEGER", notNull: 1},
		{name: "evidence_deadline_at", sqlType: "INTEGER", notNull: 1},
		{name: "accounted_through_at", sqlType: "INTEGER", notNull: 1},
	},
	"operating_points": {
		{name: "mac_addr", sqlType: "TEXT", notNull: 1, pk: 1},
		{name: "frequency", sqlType: "INTEGER", notNull: 1, pk: 2},
		{name: "core_voltage", sqlType: "INTEGER", notNull: 1, pk: 3},
		{name: "status", sqlType: "TEXT", notNull: 1},
		{name: "median_hash", sqlType: "REAL", notNull: 1},
		{name: "expected_hash", sqlType: "REAL", notNull: 1},
		{name: "attainment", sqlType: "REAL", notNull: 1},
		{name: "mean_temp", sqlType: "REAL", notNull: 1},
		{name: "p95_temp", sqlType: "REAL", notNull: 1},
		{name: "p95_vr_temp", sqlType: "REAL", notNull: 1},
		{name: "p95_power", sqlType: "REAL", notNull: 1},
		{name: "error_percent", sqlType: "REAL"},
		{name: "accepted_delta", sqlType: "INTEGER", notNull: 1},
		{name: "rejected_delta", sqlType: "INTEGER", notNull: 1},
		{name: "measured_at", sqlType: "INTEGER", notNull: 1},
		{name: "entered_at", sqlType: "INTEGER", notNull: 1},
		{name: "entry_attempt_id", sqlType: "INTEGER", notNull: 1},
		{name: "reference_hash", sqlType: "REAL", notNull: 1},
	},
	"optimizer_hourly": {
		{name: "mac_addr", sqlType: "TEXT", notNull: 1, pk: 1},
		{name: "hour_started_at", sqlType: "INTEGER", notNull: 1, pk: 2},
		{name: "observed_seconds", sqlType: "REAL", notNull: 1},
		{name: "unknown_gap_seconds", sqlType: "REAL", notNull: 1},
		{name: "actual_hash_seconds", sqlType: "REAL", notNull: 1},
		{name: "trial_actual_hash_seconds", sqlType: "REAL", notNull: 1},
		{name: "incumbent_counterfactual_hash_seconds", sqlType: "REAL", notNull: 1},
		{name: "settled_seconds", sqlType: "REAL", notNull: 1},
		{name: "trial_seconds", sqlType: "REAL", notNull: 1},
	},
}

const createOptimizerSchema = `
CREATE TABLE mutation_attempts (
	id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	mac_addr TEXT NOT NULL,
	kind TEXT NOT NULL,
	reason TEXT NOT NULL,
	from_frequency INTEGER NOT NULL,
	from_core_voltage INTEGER NOT NULL,
	target_frequency INTEGER NOT NULL,
	target_core_voltage INTEGER NOT NULL,
	intent_created_at INTEGER NOT NULL,
	started_at INTEGER NOT NULL,
	patch_requested_at INTEGER NOT NULL,
	configured_verified_at INTEGER NOT NULL,
	configured_verified_uptime_seconds INTEGER NOT NULL,
	restart_requested_at INTEGER NOT NULL,
	reboot_verified_at INTEGER NOT NULL,
	completed_at INTEGER NOT NULL,
	first_positive_at INTEGER NOT NULL,
	mining_resumed_at INTEGER NOT NULL,
	failed_at INTEGER NOT NULL,
	failure_stage TEXT NOT NULL
);
CREATE INDEX mutation_attempts_mac_started
	ON mutation_attempts (mac_addr, started_at);
CREATE UNIQUE INDEX mutation_attempts_one_unfinished
	ON mutation_attempts(mac_addr)
	WHERE failed_at = 0 AND mining_resumed_at = 0;
CREATE TABLE optimizer_miners (
	mac_addr TEXT NOT NULL PRIMARY KEY,
	hostname TEXT NOT NULL,
	ip TEXT NOT NULL,
	phase TEXT NOT NULL,
	phase_started_at INTEGER NOT NULL,
	ramp_until INTEGER NOT NULL,
	current_frequency INTEGER NOT NULL,
	current_core_voltage INTEGER NOT NULL,
	best_frequency INTEGER NOT NULL,
	best_core_voltage INTEGER NOT NULL,
	best_hash_rate REAL NOT NULL,
	fallback_frequency INTEGER NOT NULL,
	fallback_core_voltage INTEGER NOT NULL,
	pending_kind TEXT NOT NULL,
	pending_frequency INTEGER NOT NULL,
	pending_core_voltage INTEGER NOT NULL,
	pending_since INTEGER NOT NULL,
	mining_pending INTEGER NOT NULL,
	observed_frequency INTEGER NOT NULL,
	observed_core_voltage INTEGER NOT NULL,
	observed_count INTEGER NOT NULL,
	consecutive_bad_windows INTEGER NOT NULL,
	overheat_count INTEGER NOT NULL,
	cooldown_until INTEGER NOT NULL,
	pass_started_at INTEGER NOT NULL,
	pass_trigger TEXT NOT NULL,
	pass_reference_hash REAL NOT NULL,
	pass_reference_frequency INTEGER NOT NULL,
	pass_reference_core_voltage INTEGER NOT NULL,
	pass_reference_settled_at INTEGER NOT NULL,
	safety_reason TEXT NOT NULL,
	hold_reason TEXT NOT NULL,
	settled_at INTEGER NOT NULL,
	evidence_deadline_at INTEGER NOT NULL,
	accounted_through_at INTEGER NOT NULL
);
CREATE TABLE operating_points (
	mac_addr TEXT NOT NULL,
	frequency INTEGER NOT NULL,
	core_voltage INTEGER NOT NULL,
	status TEXT NOT NULL,
	median_hash REAL NOT NULL,
	expected_hash REAL NOT NULL,
	attainment REAL NOT NULL,
	mean_temp REAL NOT NULL,
	p95_temp REAL NOT NULL,
	p95_vr_temp REAL NOT NULL,
	p95_power REAL NOT NULL,
	error_percent REAL,
	accepted_delta INTEGER NOT NULL,
	rejected_delta INTEGER NOT NULL,
	measured_at INTEGER NOT NULL,
	entered_at INTEGER NOT NULL,
	entry_attempt_id INTEGER NOT NULL,
	reference_hash REAL NOT NULL,
	PRIMARY KEY (mac_addr, frequency, core_voltage)
);
CREATE UNIQUE INDEX operating_points_one_entry_attempt
	ON operating_points(entry_attempt_id)
	WHERE entry_attempt_id > 0;
CREATE TABLE optimizer_hourly (
	mac_addr TEXT NOT NULL,
	hour_started_at INTEGER NOT NULL,
	observed_seconds REAL NOT NULL,
	unknown_gap_seconds REAL NOT NULL,
	actual_hash_seconds REAL NOT NULL,
	trial_actual_hash_seconds REAL NOT NULL,
	incumbent_counterfactual_hash_seconds REAL NOT NULL,
	settled_seconds REAL NOT NULL,
	trial_seconds REAL NOT NULL,
	PRIMARY KEY (mac_addr, hour_started_at)
);
PRAGMA user_version = 5;
`

func ensureOptimizerSchema(ctx context.Context, conn *sql.Conn) error {
	version, err := schemaVersion(ctx, conn)
	if err != nil {
		return fmt.Errorf("inspect optimizer schema version: %w", err)
	}
	tables, err := applicationTables(ctx, conn)
	if err != nil {
		return fmt.Errorf("inspect optimizer schema tables: %w", err)
	}
	if version == 0 && len(tables) == 0 {
		if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
			return fmt.Errorf("create optimizer schema: begin transaction: %w", err)
		}
		if _, err := conn.ExecContext(ctx, createOptimizerSchema); err != nil {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			return fmt.Errorf("create optimizer schema: %w", err)
		}
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			return fmt.Errorf("create optimizer schema: commit transaction: %w", err)
		}
		if err := validateOptimizerSchema(ctx, conn); err != nil {
			return fmt.Errorf("validate created optimizer schema: %w", err)
		}
		if err := validateOptimizerIndexes(ctx, conn); err != nil {
			return fmt.Errorf("validate created optimizer indexes: %w", err)
		}
		return nil
	}
	if version != optimizerSchemaVersion {
		return incompatibleSchema(version)
	}
	if err := validateOptimizerSchema(ctx, conn); err != nil {
		return fmt.Errorf("%w: %v", incompatibleSchema(version), err)
	}
	if err := validateOptimizerIndexes(ctx, conn); err != nil {
		return fmt.Errorf("%w: %v", incompatibleSchema(version), err)
	}
	return nil
}

func schemaVersion(ctx context.Context, conn *sql.Conn) (int, error) {
	var version int
	if err := conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func applicationTables(ctx context.Context, conn *sql.Conn) ([]string, error) {
	rows, err := conn.QueryContext(
		ctx,
		`SELECT name FROM sqlite_master
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
		ORDER BY name`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, rows.Err()
}

func validateOptimizerSchema(ctx context.Context, conn *sql.Conn) error {
	objects, err := applicationObjects(ctx, conn)
	if err != nil {
		return err
	}
	for _, object := range objects {
		if object.kind != "table" || object.name == "" {
			return fmt.Errorf("unexpected application object %q", object.name)
		}
	}
	tables, err := applicationTables(ctx, conn)
	if err != nil {
		return err
	}
	if len(tables) != len(optimizerSchema) {
		return fmt.Errorf("application table set is incomplete or unexpected")
	}
	for _, table := range tables {
		expected, ok := optimizerSchema[table]
		if !ok {
			return fmt.Errorf("unexpected application table %q", table)
		}
		actual, err := tableColumns(ctx, conn, table)
		if err != nil {
			return err
		}
		if len(actual) != len(expected) {
			return fmt.Errorf("table %q has an incompatible column count", table)
		}
		for index := range expected {
			if actual[index] != expected[index] {
				return fmt.Errorf("table %q has an incompatible column %q", table, actual[index].name)
			}
		}
	}
	return nil
}

type applicationObject struct {
	kind string
	name string
}

func applicationObjects(ctx context.Context, conn *sql.Conn) ([]applicationObject, error) {
	rows, err := conn.QueryContext(ctx, `SELECT type, name FROM sqlite_master
		WHERE name NOT LIKE 'sqlite_%' AND type IN ('table', 'view', 'trigger')
		ORDER BY type, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var objects []applicationObject
	for rows.Next() {
		var object applicationObject
		if err := rows.Scan(&object.kind, &object.name); err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	return objects, rows.Err()
}

func validateOptimizerIndexes(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, `SELECT name, tbl_name, sql
		FROM sqlite_master WHERE type = 'index'
		AND name NOT LIKE 'sqlite_autoindex_%' ORDER BY name`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type indexDefinition struct {
		name  string
		table string
		sql   string
	}
	var actual []indexDefinition
	for rows.Next() {
		var index indexDefinition
		if err := rows.Scan(&index.name, &index.table, &index.sql); err != nil {
			return err
		}
		actual = append(actual, index)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	expected := map[string]struct {
		table string
		sql   string
	}{
		"mutation_attempts_mac_started": {
			table: "mutation_attempts",
			sql:   "CREATE INDEX mutation_attempts_mac_started ON mutation_attempts (mac_addr, started_at)",
		},
		"mutation_attempts_one_unfinished": {
			table: "mutation_attempts",
			sql:   "CREATE UNIQUE INDEX mutation_attempts_one_unfinished ON mutation_attempts(mac_addr) WHERE failed_at = 0 AND mining_resumed_at = 0",
		},
		"operating_points_one_entry_attempt": {
			table: "operating_points",
			sql:   "CREATE UNIQUE INDEX operating_points_one_entry_attempt ON operating_points(entry_attempt_id) WHERE entry_attempt_id > 0",
		},
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("application index set is incomplete or unexpected")
	}
	for _, index := range actual {
		definition, ok := expected[index.name]
		if !ok || definition.table != index.table ||
			normalizeSQL(index.sql) != normalizeSQL(definition.sql) {
			return fmt.Errorf("index %q is incompatible", index.name)
		}
	}
	return nil
}

func normalizeSQL(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(value)), " ")
}

func validateStoredOptimizerData(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(
		ctx,
		"SELECT mac_addr FROM optimizer_miners ORDER BY mac_addr",
	)
	if err != nil {
		return err
	}
	var macAddresses []string
	for rows.Next() {
		var macAddr string
		if err := rows.Scan(&macAddr); err != nil {
			_ = rows.Close()
			return err
		}
		macAddresses = append(macAddresses, macAddr)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	states := make(map[string]MinerState, len(macAddresses))
	for _, macAddr := range macAddresses {
		state, err := queryMiner(conn, macAddr)
		if err != nil {
			return err
		}
		if err := validateMinerState(state); err != nil {
			return fmt.Errorf("miner %s: %w", macAddr, err)
		}
		states[macAddr] = state
	}

	rows, err = conn.QueryContext(
		ctx,
		`SELECT mac_addr, frequency, core_voltage, status, median_hash,
			expected_hash, attainment, mean_temp, p95_temp, p95_vr_temp,
			p95_power, error_percent, accepted_delta, rejected_delta,
			measured_at, entered_at, entry_attempt_id, reference_hash
		FROM operating_points
		ORDER BY mac_addr, frequency, core_voltage`,
	)
	if err != nil {
		return err
	}
	records, err := scanPointRows(rows)
	closeErr := rows.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	for _, record := range records {
		if err := validatePointRecord(record); err != nil {
			return fmt.Errorf(
				"operating point %s/%d/%d: %w",
				record.MacAddr,
				record.Frequency,
				record.CoreVoltage,
				err,
			)
		}
	}

	rows, err = conn.QueryContext(
		ctx,
		mutationAttemptSelect+" ORDER BY id",
	)
	if err != nil {
		return err
	}
	attempts, err := scanMutationAttempts(rows)
	closeErr = rows.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	for _, attempt := range attempts {
		if err := validateMutationAttempt(attempt, true); err != nil {
			return fmt.Errorf("mutation attempt %d: %w", attempt.ID, err)
		}
	}
	if err := validateCrossTableState(ctx, conn, states, records, attempts); err != nil {
		return err
	}
	rows, err = conn.QueryContext(ctx, `SELECT
		mac_addr, hour_started_at, observed_seconds, unknown_gap_seconds,
		actual_hash_seconds, trial_actual_hash_seconds,
		incumbent_counterfactual_hash_seconds, settled_seconds, trial_seconds
		FROM optimizer_hourly ORDER BY mac_addr, hour_started_at`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var aggregate HourlyAggregate
		var hour int64
		if err := rows.Scan(&aggregate.MacAddr, &hour, &aggregate.ObservedSeconds,
			&aggregate.UnknownGapSeconds, &aggregate.ActualHashSeconds,
			&aggregate.TrialActualHashSeconds,
			&aggregate.IncumbentCounterfactualHashSeconds,
			&aggregate.SettledSeconds, &aggregate.TrialSeconds); err != nil {
			_ = rows.Close()
			return err
		}
		aggregate.HourStartedAt = time.Unix(hour, 0).UTC()
		if _, ok := states[aggregate.MacAddr]; !ok {
			_ = rows.Close()
			return fmt.Errorf("hourly row belongs to unknown miner %s", aggregate.MacAddr)
		}
		if err := validateHourlyAggregate(aggregate); err != nil {
			_ = rows.Close()
			return fmt.Errorf("hourly row %s/%s: %w", aggregate.MacAddr, aggregate.HourStartedAt, err)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return nil
}

func validateCrossTableState(
	ctx context.Context,
	conn *sql.Conn,
	states map[string]MinerState,
	records []OperatingPointRecord,
	attempts []MutationAttempt,
) error {
	attemptsByID := make(map[int64]MutationAttempt, len(attempts))
	pointsByAttempt := make(map[int64]OperatingPointRecord)
	for _, record := range records {
		if _, known := states[record.MacAddr]; !known {
			return fmt.Errorf("operating point belongs to unknown miner %s", record.MacAddr)
		}
		if record.EntryAttemptID <= 0 {
			continue
		}
		if _, exists := pointsByAttempt[record.EntryAttemptID]; exists {
			return fmt.Errorf("entry attempt %d is bound to multiple points", record.EntryAttemptID)
		}
		pointsByAttempt[record.EntryAttemptID] = record
	}
	for _, attempt := range attempts {
		if attempt.ID <= 0 {
			return fmt.Errorf("mutation attempt has invalid ID")
		}
		if _, exists := attemptsByID[attempt.ID]; exists {
			return fmt.Errorf("mutation attempt %d is duplicated", attempt.ID)
		}
		attemptsByID[attempt.ID] = attempt
		if attempt.MacAddr == "" {
			return fmt.Errorf("mutation attempt %d has no miner", attempt.ID)
		}
		if _, known := states[attempt.MacAddr]; !known {
			return fmt.Errorf("mutation attempt %d belongs to unknown miner %s", attempt.ID, attempt.MacAddr)
		}
		if attempt.FailureStage == "" && attempt.MiningResumedAt.IsZero() {
			// There is exactly one unfinished row per MAC by the partial index;
			// querying it here also catches databases that were altered outside
			// the application index contract.
			var count int
			if err := conn.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM mutation_attempts
				 WHERE mac_addr = ? AND failed_at = 0 AND mining_resumed_at = 0`,
				attempt.MacAddr).Scan(&count); err != nil {
				return err
			}
			if count != 1 {
				return fmt.Errorf("unfinished mutation attempt count for %s is %d", attempt.MacAddr, count)
			}
		}
		if record, ok := pointsByAttempt[attempt.ID]; ok {
			if record.MacAddr != attempt.MacAddr ||
				record.Point() != attempt.TargetPoint() ||
				record.EnteredAt.IsZero() ||
				!record.EnteredAt.Equal(attempt.IntentCreatedAt) {
				return fmt.Errorf("entry attempt %d does not match its operating point", attempt.ID)
			}
			if attempt.Kind != MutationOperatingPoint {
				return fmt.Errorf("entry attempt %d has mutation kind %q", attempt.ID, attempt.Kind)
			}
		}
	}
	for _, record := range records {
		if record.EntryAttemptID <= 0 {
			if record.ReferenceHash != 0 && record.Status != PointEntered {
				return fmt.Errorf("baseline point %d/%d has a reference hash", record.Frequency, record.CoreVoltage)
			}
			continue
		}
		attempt, ok := attemptsByID[record.EntryAttemptID]
		if !ok {
			return fmt.Errorf("point %s/%d/%d references missing entry attempt %d", record.MacAddr, record.Frequency, record.CoreVoltage, record.EntryAttemptID)
		}
		if attempt.Kind != MutationOperatingPoint || attempt.MacAddr != record.MacAddr || attempt.TargetPoint() != record.Point() ||
			attempt.IntentCreatedAt.IsZero() || !attempt.IntentCreatedAt.Equal(record.EnteredAt) {
			return fmt.Errorf("point %s/%d/%d has an invalid entry-attempt link", record.MacAddr, record.Frequency, record.CoreVoltage)
		}
	}
	for macAddr, state := range states {
		minerRecords := recordsForMiner(records, macAddr)
		baselineCount := 0
		var baseline OperatingPointRecord
		for _, record := range minerRecords {
			if record.EntryAttemptID > 0 {
				if record.ReferenceHash <= 0 {
					return fmt.Errorf("candidate point %d/%d has no positive reference hash", record.Frequency, record.CoreVoltage)
				}
				continue
			}
			if record.ReferenceHash != 0 || !record.EnteredAt.Equal(state.PassStartedAt) {
				return fmt.Errorf("point %d/%d without entry attempt is not the current baseline", record.Frequency, record.CoreVoltage)
			}
			if record.Status == PointEntered && record.Point() != state.CurrentPoint() &&
				!(state.Phase == PhaseHold && state.HoldReason == HoldManual) {
				return fmt.Errorf("entered baseline point %d/%d is not the durable current point", record.Frequency, record.CoreVoltage)
			}
			baselineCount++
			baseline = record
		}
		if baselineCount > 1 {
			return fmt.Errorf("miner %s has multiple baseline rows", macAddr)
		}
		if baselineCount == 0 && len(minerRecords) > 0 {
			return fmt.Errorf("miner %s has point history without exactly one pass baseline", macAddr)
		}
		if baselineCount == 0 && state.Phase != PhaseOverheat &&
			!(state.Phase == PhaseHold && state.HoldReason == HoldBlocked) {
			return fmt.Errorf("miner %s has no pass baseline", macAddr)
		}
		if baselineCount == 1 && baseline.Status == PointEntered && state.Phase == PhaseHold && state.HoldReason != HoldManual {
			// An entered baseline is only valid while the pass is still collecting
			// evidence, or while a pending lifecycle is reconciling it.
			if state.Phase != PhaseBaseline && state.PendingKind == "" {
				return fmt.Errorf("miner %s retains an entered baseline outside active collection", macAddr)
			}
		}
		if err := validateBestPointSummary(state, minerRecords); err != nil {
			return fmt.Errorf("miner %s: %w", macAddr, err)
		}
		if err := validateStoredPhaseShape(state, minerRecords, attemptsByID); err != nil {
			return fmt.Errorf("miner %s: %w", macAddr, err)
		}
	}
	for _, attempt := range attempts {
		if !attempt.FailedAt.IsZero() || !attempt.MiningResumedAt.IsZero() {
			continue
		}
		state, known := states[attempt.MacAddr]
		if !known {
			return fmt.Errorf("unfinished mutation attempt %d belongs to unknown miner %s", attempt.ID, attempt.MacAddr)
		}
		if attempt.CompletedAt.IsZero() {
			if attempt.Kind == MutationMiningConfiguration {
				if !state.MiningPending || state.PendingKind != "" {
					return fmt.Errorf("unfinished mining attempt %d has no durable mining authority", attempt.ID)
				}
			} else {
				if state.PendingKind != attempt.Kind || state.PendingPoint() != attempt.TargetPoint() ||
					!state.PendingSince.Equal(attempt.IntentCreatedAt) {
					return fmt.Errorf("unfinished mutation attempt %d has no durable operating-point authority", attempt.ID)
				}
				if (attempt.Kind == MutationSafetyRollback || attempt.Kind == MutationOverheatRecovery) &&
					state.SafetyReason != attempt.Reason {
					return fmt.Errorf("unfinished safety attempt %d does not match the durable safety reason", attempt.ID)
				}
			}
			continue
		}
		if err := validateCompletedMutationShape(state, attempt); err != nil {
			return fmt.Errorf("completed-but-unresumed attempt %d has invalid state: %w", attempt.ID, err)
		}
		if attempt.Kind == MutationMiningConfiguration {
			if state.MiningPending || state.PendingKind != "" {
				return fmt.Errorf("completed mining attempt %d has a remaining durable obligation", attempt.ID)
			}
		} else if state.PendingKind != "" || state.CurrentPoint() != attempt.TargetPoint() {
			return fmt.Errorf("completed mutation attempt %d has an invalid post-completion authority shape", attempt.ID)
		}
	}
	return nil
}

func recordsForMiner(records []OperatingPointRecord, macAddr string) []OperatingPointRecord {
	filtered := make([]OperatingPointRecord, 0)
	for _, record := range records {
		if record.MacAddr == macAddr {
			filtered = append(filtered, record)
		}
	}
	return filtered
}

func validateBestPointSummary(state MinerState, records []OperatingPointRecord) error {
	var best OperatingPointRecord
	found := false
	for _, record := range records {
		if record.Status != PointValidated {
			continue
		}
		if record.MedianHash <= 0 || !finite(record.MedianHash) {
			return fmt.Errorf("validated point %d/%d has no positive hash rate", record.Frequency, record.CoreVoltage)
		}
		if !found || record.MedianHash > best.MedianHash ||
			(record.MedianHash == best.MedianHash && betterStoredPoint(record, best)) {
			best = record
			found = true
		}
	}
	if !found {
		if state.BestHashRate != 0 || state.BestPoint() != (OperatingPoint{}) {
			return fmt.Errorf("best state exists without a validated point")
		}
		return nil
	}
	if state.BestHashRate != best.MedianHash || state.BestPoint() != best.Point() {
		return fmt.Errorf("best state does not equal pass maximum %.6f at %d/%d", best.MedianHash, best.Frequency, best.CoreVoltage)
	}
	return nil
}

func betterStoredPoint(left, right OperatingPointRecord) bool {
	for _, comparison := range []func(OperatingPointRecord) float64{
		func(record OperatingPointRecord) float64 { return record.P95Temp },
		func(record OperatingPointRecord) float64 { return record.P95Power },
		func(record OperatingPointRecord) float64 { return record.P95VRTemp },
		func(record OperatingPointRecord) float64 { return float64(record.CoreVoltage) },
		func(record OperatingPointRecord) float64 { return float64(record.Frequency) },
	} {
		leftValue, rightValue := comparison(left), comparison(right)
		if leftValue != rightValue {
			return leftValue < rightValue
		}
	}
	return false
}

func validateStoredPhaseShape(
	state MinerState,
	records []OperatingPointRecord,
	attempts map[int64]MutationAttempt,
) error {
	completedUnresumed := false
	for _, attempt := range attempts {
		if attempt.MacAddr == state.MacAddr && !attempt.CompletedAt.IsZero() &&
			attempt.MiningResumedAt.IsZero() && attempt.FailedAt.IsZero() {
			completedUnresumed = true
			break
		}
	}
	if state.PendingKind != "" && state.EvidenceDeadlineAt.IsZero() == false {
		return fmt.Errorf("pending mutation retains an evidence deadline")
	}
	if !state.EvidenceDeadlineAt.IsZero() {
		if state.RampUntil.IsZero() {
			return fmt.Errorf("evidence deadline exists without a ramp")
		}
		if state.EvidenceDeadlineAt.Before(state.RampUntil) {
			return fmt.Errorf("evidence deadline precedes ramp")
		}
	}
	if state.MiningPending && state.PendingKind == MutationOperatingPoint {
		return fmt.Errorf("mining and operating-point obligations overlap")
	}
	if state.Phase == PhaseOverheat && !state.EvidenceDeadlineAt.IsZero() {
		return fmt.Errorf("overheat phase retains an evidence deadline")
	}
	if state.Phase == PhaseHold && state.HoldReason == HoldBlocked && !state.EvidenceDeadlineAt.IsZero() {
		return fmt.Errorf("blocked hold retains an evidence deadline")
	}
	if state.Phase == PhaseHold && state.HoldReason == HoldOptimized &&
		state.SettledAt.IsZero() && state.PendingKind == "" && state.EvidenceDeadlineAt.IsZero() && !completedUnresumed {
		return fmt.Errorf("active hold validation has no evidence deadline")
	}
	if (state.Phase == PhaseBaseline || state.Phase == PhaseUndervolt ||
		state.Phase == PhaseFrequencyTest || state.Phase == PhaseVoltageTest) &&
		state.PendingKind == "" && state.EvidenceDeadlineAt.IsZero() && !completedUnresumed {
		return fmt.Errorf("active optimization phase has no evidence deadline")
	}
	if state.Phase == PhaseCooldown && state.PendingKind == "" &&
		state.EvidenceDeadlineAt.IsZero() && !completedUnresumed &&
		state.SafetyReason == "" {
		return fmt.Errorf("active cooldown validation has no evidence deadline")
	}
	if state.Phase == PhaseHold && state.HoldReason == HoldOptimized && state.PendingKind != "" &&
		(state.FallbackPoint() != (OperatingPoint{}) || state.PendingKind != MutationOperatingPoint) {
		return fmt.Errorf("final placement has an invalid fallback or mutation kind")
	}
	if state.Phase == PhaseUndervolt || state.Phase == PhaseFrequencyTest || state.Phase == PhaseVoltageTest {
		fallback := state.FallbackPoint()
		if !validStoredPoint(fallback) || !validStoredPoint(state.CurrentPoint()) {
			return fmt.Errorf("trial phase has invalid current or fallback")
		}
		current, found := findStoredPoint(records, state.CurrentPoint())
		if !found {
			return fmt.Errorf("trial phase current point has no point record")
		}
		fallbackRecord, fallbackFound := findStoredPoint(records, fallback)
		if !fallbackFound {
			return fmt.Errorf("trial phase fallback point has no point record")
		}
		if state.CurrentPoint() == fallback {
			if state.PendingKind != MutationOperatingPoint || state.PendingPoint() == fallback {
				return fmt.Errorf("trial entry shape is invalid")
			}
			candidate, found := findStoredPoint(records, state.PendingPoint())
			if !found || candidate.Status != PointEntered || candidate.EntryAttemptID <= 0 {
				return fmt.Errorf("trial entry target is not an entered candidate")
			}
			if _, ok := attempts[candidate.EntryAttemptID]; !ok {
				return fmt.Errorf("trial entry target attempt is missing")
			}
		} else if current.EntryAttemptID <= 0 {
			return fmt.Errorf("trial incumbent point has no entry authority")
		} else if state.PendingKind != "" && state.PendingKind != MutationOperatingPoint {
			return fmt.Errorf("trial phase has invalid pending kind")
		} else if state.PendingKind == MutationOperatingPoint && state.PendingPoint() != fallback {
			return fmt.Errorf("trial return target is not the reserved fallback")
		}
		if fallbackRecord.EntryAttemptID > 0 && fallbackRecord.Status == PointEntered {
			return fmt.Errorf("trial fallback point is not terminal")
		}
	}
	for _, attempt := range attempts {
		if attempt.MacAddr != state.MacAddr || attempt.CompletedAt.IsZero() || !attempt.MiningResumedAt.IsZero() || !attempt.FailedAt.IsZero() {
			continue
		}
		if attempt.Kind == MutationMiningConfiguration {
			if state.MiningPending {
				return fmt.Errorf("completed mining attempt still has a mining obligation")
			}
			continue
		}
		if state.PendingKind != "" || state.CurrentPoint() != attempt.TargetPoint() {
			return fmt.Errorf("completed mutation has not-resumed state shape")
		}
		if !state.EvidenceDeadlineAt.IsZero() {
			return fmt.Errorf("completed mutation has an evidence deadline before healthy resume")
		}
	}
	return nil
}

func findStoredPoint(records []OperatingPointRecord, point OperatingPoint) (OperatingPointRecord, bool) {
	for _, record := range records {
		if record.Point() == point {
			return record, true
		}
	}
	return OperatingPointRecord{}, false
}

func tableColumns(ctx context.Context, conn *sql.Conn, table string) ([]schemaColumn, error) {
	rows, err := conn.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []schemaColumn
	for rows.Next() {
		var position int
		var column schemaColumn
		var defaultValue sql.NullString
		if err := rows.Scan(
			&position,
			&column.name,
			&column.sqlType,
			&column.notNull,
			&defaultValue,
			&column.pk,
		); err != nil {
			return nil, err
		}
		column.sqlType = strings.ToUpper(column.sqlType)
		columns = append(columns, column)
	}
	return columns, rows.Err()
}

func incompatibleSchema(version int) error {
	return fmt.Errorf(
		"optimizer database schema version %d is incompatible; move aside or remove the runtime database and start from a fresh baseline",
		version,
	)
}

func sqliteBusy(err error) bool {
	var sqliteError sqlite3.Error
	return errors.As(err, &sqliteError) &&
		(sqliteError.Code == sqlite3.ErrBusy || sqliteError.Code == sqlite3.ErrLocked)
}

func validateMinerState(state MinerState) error {
	return validateMinerStateWithTransition(state, false)
}

func validateMinerStateWithTransition(state MinerState, allowUnsettledHold bool) error {
	normalizedMAC, macErr := normalizeMAC(state.MacAddr)
	switch {
	case macErr != nil || normalizedMAC != state.MacAddr:
		return fmt.Errorf("MAC address is invalid or non-canonical")
	case strings.TrimSpace(state.Hostname) == "" ||
		strings.TrimSpace(state.Hostname) != state.Hostname ||
		hasControl(state.Hostname):
		return fmt.Errorf("hostname is invalid")
	case net.ParseIP(state.IP) == nil || net.ParseIP(state.IP).To4() == nil:
		return fmt.Errorf("IP is not an IPv4 address")
	case state.Phase == "":
		return fmt.Errorf("optimizer phase is empty")
	case !validOptimizerPhase(state.Phase):
		return fmt.Errorf("optimizer phase %q is invalid", state.Phase)
	case !validOptionalPoint(state.CurrentPoint()):
		return fmt.Errorf("current operating point is invalid")
	case !validOptionalPoint(state.BestPoint()):
		return fmt.Errorf("best operating point is invalid")
	case !validCanonicalOptionalPoint(state.BestPoint()):
		return fmt.Errorf("best operating point is not on the supported automation grid")
	case !validOptionalPoint(state.FallbackPoint()):
		return fmt.Errorf("fallback operating point is invalid")
	case !validCanonicalOptionalPoint(state.FallbackPoint()):
		return fmt.Errorf("fallback operating point is not on the supported automation grid")
	case state.CurrentFrequency == 50:
		return fmt.Errorf("firmware emergency sentinel cannot be durable current")
	case state.PendingKind == "" &&
		(state.PendingFrequency != 0 ||
			state.PendingCoreVoltage != 0 ||
			!state.PendingSince.IsZero()):
		return fmt.Errorf("pending mutation fields exist without a mutation kind")
	case state.PendingKind != "" &&
		state.PendingKind != MutationOperatingPoint &&
		state.PendingKind != MutationSafetyRollback &&
		state.PendingKind != MutationOverheatRecovery:
		return fmt.Errorf("pending mutation kind %q is invalid", state.PendingKind)
	case state.PendingKind != "" && !validStoredPoint(state.PendingPoint()):
		return fmt.Errorf("pending operating point is invalid")
	case state.PendingKind != "" && !validCanonicalPoint(state.PendingPoint()):
		return fmt.Errorf("pending operating point is not on the supported automation grid")
	case state.PendingKind != "" && state.PendingSince.IsZero():
		return fmt.Errorf("pending mutation has no timestamp")
	case state.MiningPending && state.PendingKind == MutationOperatingPoint:
		return fmt.Errorf("mining and operating-point obligations overlap")
	case state.PendingKind == MutationSafetyRollback &&
		state.Phase != PhaseCooldown &&
		state.Phase != PhaseOverheat:
		return fmt.Errorf("safety rollback requires cooldown or overheat phase")
	case state.PendingKind == MutationOverheatRecovery &&
		state.Phase != PhaseOverheat:
		return fmt.Errorf("overheat recovery requires overheat phase")
	case state.Phase == PhaseOverheat &&
		state.PendingKind != "" &&
		state.PendingKind != MutationSafetyRollback &&
		state.PendingKind != MutationOverheatRecovery:
		return fmt.Errorf("overheat phase has invalid pending mutation kind %q", state.PendingKind)
	case state.PendingKind != "" && state.EvidenceDeadlineAt.IsZero() == false:
		return fmt.Errorf("pending mutation retains an evidence deadline")
	case !state.EvidenceDeadlineAt.IsZero() && state.RampUntil.IsZero():
		return fmt.Errorf("evidence deadline exists without a ramp")
	case !state.EvidenceDeadlineAt.IsZero() && state.EvidenceDeadlineAt.Before(state.RampUntil):
		return fmt.Errorf("evidence deadline precedes ramp")
	case state.Phase == PhaseOverheat && !state.EvidenceDeadlineAt.IsZero():
		return fmt.Errorf("overheat phase retains an evidence deadline")
	case state.ObservedCount == 0 &&
		(state.ObservedFrequency != 0 || state.ObservedCoreVoltage != 0):
		return fmt.Errorf("observed operating point exists without confirmations")
	case state.ObservedCount > 0 && !validStoredPoint(OperatingPoint{
		Frequency:   state.ObservedFrequency,
		CoreVoltage: state.ObservedCoreVoltage,
	}):
		return fmt.Errorf("observed operating point is invalid")
	case state.ObservedCount < 0:
		return fmt.Errorf("observed count %d is invalid", state.ObservedCount)
	case state.ConsecutiveBadWindows < 0:
		return fmt.Errorf("bad-window count %d is invalid", state.ConsecutiveBadWindows)
	case state.OverheatCount < 0:
		return fmt.Errorf("overheat count %d is invalid", state.OverheatCount)
	case !finite(state.BestHashRate) || state.BestHashRate < 0:
		return fmt.Errorf("best hash rate %.2f is invalid", state.BestHashRate)
	case state.PassStartedAt.IsZero():
		return fmt.Errorf("optimization pass has no start time")
	case !validPassTrigger(state.PassTrigger):
		return fmt.Errorf("optimization pass trigger %q is invalid", state.PassTrigger)
	case !finite(state.PassReferenceHash) || state.PassReferenceHash < 0:
		return fmt.Errorf("pass reference hash %.2f is invalid", state.PassReferenceHash)
	case !validSafetyReason(state.SafetyReason):
		return fmt.Errorf("safety reason %q is invalid", state.SafetyReason)
	case state.SafetyReason != "" && state.Phase != PhaseCooldown && state.Phase != PhaseOverheat &&
		!(state.Phase == PhaseHold && state.HoldReason == HoldSafety):
		return fmt.Errorf("safety reason exists outside a safety-owned phase")
	case state.Phase == PhaseHold && !validHoldReason(state.HoldReason):
		return fmt.Errorf("hold reason %q is invalid", state.HoldReason)
	case state.Phase != PhaseHold && state.HoldReason != "":
		return fmt.Errorf("hold reason exists outside HOLD")
	case state.Phase == PhaseHold && state.HoldReason == HoldBlocked && !state.SettledAt.IsZero():
		return fmt.Errorf("blocked hold is settled")
	case state.Phase != PhaseHold && !state.SettledAt.IsZero():
		return fmt.Errorf("settlement exists outside HOLD")
	case state.Phase == PhaseHold &&
		(state.HoldReason == HoldOptimized || state.HoldReason == HoldSafety || state.HoldReason == HoldManual) &&
		!state.SettledAt.IsZero() && state.EvidenceDeadlineAt.IsZero() == false:
		return fmt.Errorf("settled HOLD retains an evidence deadline")
	case state.Phase == PhaseHold && state.HoldReason == HoldBlocked && !state.EvidenceDeadlineAt.IsZero():
		return fmt.Errorf("blocked HOLD retains an evidence deadline")
	case state.Phase == PhaseHold && state.HoldReason == HoldOptimized &&
		state.SettledAt.IsZero() && state.PendingKind == "" && state.EvidenceDeadlineAt.IsZero() &&
		!allowUnsettledHold:
		return fmt.Errorf("active HOLD validation has no evidence deadline")
	case state.Phase == PhaseHold && state.HoldReason == HoldOptimized && state.PendingKind != "" &&
		(state.PendingKind != MutationOperatingPoint || state.FallbackPoint() != (OperatingPoint{})):
		return fmt.Errorf("final placement has an invalid mutation shape")
	case (state.Phase == PhaseUndervolt || state.Phase == PhaseFrequencyTest || state.Phase == PhaseVoltageTest) &&
		(!validStoredPoint(state.CurrentPoint()) || !validStoredPoint(state.FallbackPoint())):
		return fmt.Errorf("trial phase has an invalid current or fallback point")
	case (state.Phase == PhaseUndervolt || state.Phase == PhaseFrequencyTest || state.Phase == PhaseVoltageTest) &&
		state.PendingKind != "" && state.PendingKind != MutationOperatingPoint:
		return fmt.Errorf("trial phase has an invalid pending mutation")
	case (state.Phase == PhaseUndervolt || state.Phase == PhaseFrequencyTest || state.Phase == PhaseVoltageTest) &&
		state.CurrentPoint() == state.FallbackPoint() &&
		(state.PendingKind != MutationOperatingPoint || state.PendingPoint() == state.FallbackPoint()):
		return fmt.Errorf("trial entry shape is invalid")
	case (state.Phase == PhaseUndervolt || state.Phase == PhaseFrequencyTest || state.Phase == PhaseVoltageTest) &&
		state.CurrentPoint() != state.FallbackPoint() && state.PendingKind == MutationOperatingPoint &&
		state.PendingPoint() != state.FallbackPoint():
		return fmt.Errorf("trial return shape is invalid")
	case state.AccountedThroughAt.IsZero():
		return fmt.Errorf("hourly accounting cursor is empty")
	default:
		return validatePassReferenceSnapshot(state)
	}
}

func validatePassReferenceSnapshot(state MinerState) error {
	point := OperatingPoint{
		Frequency:   state.PassReferenceFrequency,
		CoreVoltage: state.PassReferenceCoreVoltage,
	}
	hasPoint := point.Frequency != 0 || point.CoreVoltage != 0
	hasSettlement := !state.PassReferenceSettledAt.IsZero()
	if !hasPoint && !hasSettlement {
		if state.PassTrigger == PassOperator && state.PassReferenceHash > 0 {
			return fmt.Errorf("operator pass reference hash has no complete boundary snapshot")
		}
		return nil
	}
	if !hasPoint || !hasSettlement || !IsCanonicalOperatingPoint(point) ||
		point.Frequency == 50 || !finite(state.PassReferenceHash) || state.PassReferenceHash <= 0 {
		return fmt.Errorf("pass reference boundary snapshot is incomplete or invalid")
	}
	if state.PassReferenceSettledAt.UnixNano() <= 0 ||
		state.PassReferenceSettledAt.After(state.PassStartedAt) {
		return fmt.Errorf("pass reference settlement is outside the pass boundary")
	}
	return nil
}

func validatePointRecord(record OperatingPointRecord) error {
	normalizedMAC, macErr := normalizeMAC(record.MacAddr)
	switch {
	case macErr != nil || normalizedMAC != record.MacAddr:
		return fmt.Errorf("MAC address is invalid or non-canonical")
	case !validStoredPoint(record.Point()):
		return fmt.Errorf("operating point is invalid")
	case !validCanonicalPoint(record.Point()):
		return fmt.Errorf("operating point is not on the supported automation grid")
	case strings.TrimSpace(record.Status) == "":
		return fmt.Errorf("status is empty")
	case !validPointStatus(record.Status):
		return fmt.Errorf("status %q is invalid", record.Status)
	case !finite(record.MedianHash) || record.MedianHash < 0:
		return fmt.Errorf("median hash rate %.2f is invalid", record.MedianHash)
	case !finite(record.ExpectedHash) || record.ExpectedHash < 0:
		return fmt.Errorf("expected hash rate %.2f is invalid", record.ExpectedHash)
	case !finite(record.Attainment) || record.Attainment < 0:
		return fmt.Errorf("hash attainment %.4f is invalid", record.Attainment)
	case !finite(record.MeanTemp) ||
		!finite(record.P95Temp) ||
		!finite(record.P95VRTemp) ||
		!finite(record.P95Power):
		return fmt.Errorf("operating point contains non-finite safety telemetry")
	case record.MeanTemp < 0 ||
		record.P95Temp < 0 ||
		record.P95VRTemp < 0 ||
		record.P95Power < 0:
		return fmt.Errorf("operating point contains negative safety telemetry")
	case record.ErrorPercent != nil &&
		(!finite(*record.ErrorPercent) ||
			*record.ErrorPercent < 0 ||
			*record.ErrorPercent > 100):
		return fmt.Errorf("error percentage is invalid")
	case record.EntryAttemptID < 0:
		return fmt.Errorf("entry attempt ID is negative")
	case !finite(record.ReferenceHash) || record.ReferenceHash < 0:
		return fmt.Errorf("reference hash %.2f is invalid", record.ReferenceHash)
	case record.Status == PointEntered && record.EnteredAt.IsZero():
		return fmt.Errorf("entered point has no entry timestamp")
	case record.Status == PointEntered && !record.MeasuredAt.IsZero():
		return fmt.Errorf("entered point has measurement evidence")
	case record.Status == PointEntered &&
		(record.MedianHash != 0 || record.ExpectedHash != 0 || record.Attainment != 0 ||
			record.MeanTemp != 0 || record.P95Temp != 0 || record.P95VRTemp != 0 ||
			record.P95Power != 0 || record.ErrorPercent != nil ||
			record.AcceptedDelta != 0 || record.RejectedDelta != 0):
		return fmt.Errorf("entered point has terminal evidence")
	case record.Status != PointEntered && record.EnteredAt.IsZero():
		return fmt.Errorf("terminal point has no entry timestamp")
	case record.Status != PointEntered && record.EntryAttemptID == 0 && record.ReferenceHash != 0:
		return fmt.Errorf("terminal baseline point has a reference hash")
	case record.EntryAttemptID > 0 && record.ReferenceHash <= 0:
		return fmt.Errorf("candidate point has no positive reference hash")
	case record.Status != PointEntered && record.MeasuredAt.Before(record.EnteredAt):
		return fmt.Errorf("point measurement precedes entry")
	default:
		return nil
	}
}

func validateMutationAttempt(attempt MutationAttempt, requireID bool) error {
	normalizedMAC, macErr := normalizeMAC(attempt.MacAddr)
	target := attempt.TargetPoint()
	switch {
	case requireID && attempt.ID <= 0:
		return fmt.Errorf("ID must be positive")
	case !requireID && attempt.ID != 0:
		return fmt.Errorf("ID must be zero before insertion")
	case macErr != nil || normalizedMAC != attempt.MacAddr:
		return fmt.Errorf("MAC address is invalid or non-canonical")
	case !validMutationKind(attempt.Kind):
		return fmt.Errorf("mutation kind %q is invalid", attempt.Kind)
	case !validMutationReason(attempt.Kind, attempt.Reason):
		return fmt.Errorf("mutation reason %q is invalid for kind %q", attempt.Reason, attempt.Kind)
	case !validStoredPoint(attempt.FromPoint()):
		return fmt.Errorf("source operating point is invalid")
	case attempt.FromFrequency == 50:
		return fmt.Errorf("firmware emergency sentinel cannot be mutation source")
	case attempt.Kind == MutationMiningConfiguration &&
		target != (OperatingPoint{}):
		return fmt.Errorf("mining mutation has an operating-point target")
	case attempt.Kind != MutationMiningConfiguration &&
		!validStoredPoint(target):
		return fmt.Errorf("target operating point is invalid")
	case attempt.Kind != MutationMiningConfiguration &&
		!validCanonicalPoint(target):
		return fmt.Errorf("target operating point is not on the supported automation grid")
	case attempt.IntentCreatedAt.IsZero():
		return fmt.Errorf("intent timestamp is required")
	case attempt.StartedAt.IsZero():
		return fmt.Errorf("start timestamp is required")
	case attempt.IntentCreatedAt.After(attempt.StartedAt):
		return fmt.Errorf("intent timestamp is after attempt start")
	case !orderedOptionalTime(attempt.StartedAt, attempt.PatchRequestedAt):
		return fmt.Errorf("PATCH timestamp is before attempt start")
	case !attempt.ConfiguredVerifiedAt.IsZero() && attempt.PatchRequestedAt.IsZero():
		return fmt.Errorf("configured verification exists without PATCH timestamp")
	case !orderedOptionalTime(attempt.PatchRequestedAt, attempt.ConfiguredVerifiedAt):
		return fmt.Errorf("configured verification is before PATCH timestamp")
	case attempt.ConfiguredVerifiedAt.IsZero() && attempt.ConfiguredVerifiedUptimeSeconds != -1:
		return fmt.Errorf("configured verification uptime sentinel is invalid")
	case !attempt.ConfiguredVerifiedAt.IsZero() && attempt.ConfiguredVerifiedUptimeSeconds < 0:
		return fmt.Errorf("configured verification uptime is invalid")
	case !attempt.RestartRequestedAt.IsZero() &&
		attempt.ConfiguredVerifiedAt.IsZero():
		return fmt.Errorf("restart timestamp exists without configured verification")
	case !orderedOptionalTime(
		attempt.ConfiguredVerifiedAt,
		attempt.RestartRequestedAt,
	):
		return fmt.Errorf("restart timestamp is before configured verification")
	case !attempt.RebootVerifiedAt.IsZero() &&
		attempt.RestartRequestedAt.IsZero():
		return fmt.Errorf("reboot proof exists without restart timestamp")
	case !orderedOptionalTime(
		attempt.RestartRequestedAt,
		attempt.RebootVerifiedAt,
	):
		return fmt.Errorf("reboot proof is before restart timestamp")
	case !attempt.CompletedAt.IsZero() && attempt.RebootVerifiedAt.IsZero():
		return fmt.Errorf("completion exists without reboot proof")
	case !orderedOptionalTime(attempt.RebootVerifiedAt, attempt.CompletedAt):
		return fmt.Errorf("completion is before reboot proof")
	case !attempt.FirstPositiveAt.IsZero() && attempt.CompletedAt.IsZero():
		return fmt.Errorf("first positive observation exists without completion")
	case !orderedOptionalTime(attempt.CompletedAt, attempt.FirstPositiveAt):
		return fmt.Errorf("first positive observation is before completion")
	case !attempt.MiningResumedAt.IsZero() && attempt.CompletedAt.IsZero():
		return fmt.Errorf("mining resume exists without completion")
	case !attempt.MiningResumedAt.IsZero() && attempt.FirstPositiveAt.IsZero():
		return fmt.Errorf("mining resume exists without first positive observation")
	case !orderedOptionalTime(attempt.FirstPositiveAt, attempt.MiningResumedAt):
		return fmt.Errorf("mining resume is before first positive observation")
	case attempt.FailedAt.IsZero() && attempt.FailureStage != "":
		return fmt.Errorf("failure stage exists without failure timestamp")
	case !attempt.FailedAt.IsZero() && attempt.FailureStage == "":
		return fmt.Errorf("failure timestamp exists without failure stage")
	case attempt.FailureStage != "" &&
		!validMutationFailureStage(attempt.FailureStage):
		return fmt.Errorf("failure stage %q is invalid", attempt.FailureStage)
	case attempt.FailureStage != "" && !validMutationFailureShape(attempt):
		return fmt.Errorf("failure stage %q does not match mutation milestones", attempt.FailureStage)
	case !attempt.FailedAt.IsZero() &&
		attempt.FailedAt.Before(attempt.StartedAt):
		return fmt.Errorf("failure timestamp is before attempt start")
	case !attempt.FailedAt.IsZero() &&
		attempt.FailedAt.Before(latestMutationProgress(attempt)):
		return fmt.Errorf("failure timestamp is before the latest mutation milestone")
	case !attempt.MiningResumedAt.IsZero() && !attempt.FailedAt.IsZero():
		return fmt.Errorf("resumed mutation attempt is also failed")
	default:
		return nil
	}
}

func latestMutationProgress(attempt MutationAttempt) time.Time {
	latest := attempt.StartedAt
	for _, candidate := range []time.Time{
		attempt.PatchRequestedAt,
		attempt.ConfiguredVerifiedAt,
		attempt.RestartRequestedAt,
		attempt.RebootVerifiedAt,
		attempt.CompletedAt,
		attempt.FirstPositiveAt,
	} {
		if candidate.After(latest) {
			latest = candidate
		}
	}
	return latest
}

func validMutationFailureShape(attempt MutationAttempt) bool {
	switch attempt.FailureStage {
	case MutationFailurePreflight:
		return attempt.PatchRequestedAt.IsZero() && attempt.ConfiguredVerifiedAt.IsZero() &&
			attempt.RestartRequestedAt.IsZero() && attempt.RebootVerifiedAt.IsZero() &&
			attempt.CompletedAt.IsZero() && attempt.FirstPositiveAt.IsZero() &&
			attempt.MiningResumedAt.IsZero()
	case MutationFailureConfiguredVerification:
		return !attempt.PatchRequestedAt.IsZero() && attempt.RestartRequestedAt.IsZero() &&
			attempt.RebootVerifiedAt.IsZero() && attempt.CompletedAt.IsZero() &&
			attempt.FirstPositiveAt.IsZero() && attempt.MiningResumedAt.IsZero()
	case MutationFailureRebootVerification:
		return !attempt.RestartRequestedAt.IsZero() && attempt.RebootVerifiedAt.IsZero() &&
			attempt.CompletedAt.IsZero() && attempt.FirstPositiveAt.IsZero() &&
			attempt.MiningResumedAt.IsZero()
	case MutationFailureMiningResume:
		return !attempt.CompletedAt.IsZero() && !attempt.RebootVerifiedAt.IsZero() &&
			attempt.MiningResumedAt.IsZero()
	case MutationFailureSafetySuperseded:
		// Safety arbitration may supersede an attempt before PATCH, after
		// configured verification, or while healthy resumption is still pending.
		return attempt.MiningResumedAt.IsZero()
	default:
		return false
	}
}

func orderedOptionalTime(previous time.Time, current time.Time) bool {
	return current.IsZero() || (!previous.IsZero() && !current.Before(previous))
}

func validMutationKind(kind MutationKind) bool {
	switch kind {
	case MutationOperatingPoint,
		MutationSafetyRollback,
		MutationOverheatRecovery,
		MutationMiningConfiguration:
		return true
	default:
		return false
	}
}

func validMutationFailureStage(stage MutationFailureStage) bool {
	switch stage {
	case MutationFailurePreflight,
		MutationFailureConfiguredVerification,
		MutationFailureRebootVerification,
		MutationFailureMiningResume,
		MutationFailureSafetySuperseded:
		return true
	default:
		return false
	}
}

func validOptimizerPhase(phase OptimizerPhase) bool {
	switch phase {
	case PhaseBaseline,
		PhaseUndervolt,
		PhaseFrequencyTest,
		PhaseVoltageTest,
		PhaseHold,
		PhaseCooldown,
		PhaseOverheat:
		return true
	default:
		return false
	}
}

func validPointStatus(status string) bool {
	switch status {
	case PointEntered,
		PointValidated,
		PointUnstable,
		PointNoGain,
		PointUnobservable,
		PointThermal,
		PointPower,
		PointVRHot:
		return true
	default:
		return false
	}
}

func validPassTrigger(trigger PassTrigger) bool {
	return trigger == PassInitial || trigger == PassOperator
}

func validHoldReason(reason HoldReason) bool {
	switch reason {
	case HoldOptimized, HoldSafety, HoldManual, HoldBlocked:
		return true
	default:
		return false
	}
}

func validSafetyReason(reason SafetyReason) bool {
	if reason == "" {
		return true
	}
	switch reason {
	case SafetyReasonASICLimit,
		SafetyReasonHostCutoff,
		SafetyReasonFirmwareOverheat,
		SafetyReasonFirmwareTrip,
		SafetyReasonPowerLimit,
		SafetyReasonVRLimit,
		SafetyReasonMutationUncertain:
		return true
	default:
		return false
	}
}

func validMutationReason(kind MutationKind, reason SafetyReason) bool {
	switch kind {
	case MutationOperatingPoint, MutationMiningConfiguration:
		return reason == ""
	case MutationSafetyRollback, MutationOverheatRecovery:
		return validSafetyReason(reason) && reason != ""
	default:
		return false
	}
}

func pointFromInfo(info Info) OperatingPoint {
	return OperatingPoint{
		Frequency:   info.Frequency,
		CoreVoltage: info.CoreVoltage,
	}
}

func validOptionalPoint(point OperatingPoint) bool {
	if point.Frequency == 0 && point.CoreVoltage == 0 {
		return true
	}
	return validStoredPoint(point)
}

func validCanonicalOptionalPoint(point OperatingPoint) bool {
	return point == (OperatingPoint{}) || validCanonicalPoint(point)
}

func validCanonicalPoint(point OperatingPoint) bool {
	return validStoredPoint(point) && point.Frequency != 50 && IsCanonicalOperatingPoint(point)
}

func validStoredPoint(point OperatingPoint) bool {
	return point.Frequency > 0 && point.Frequency <= 10_000 &&
		validCoreVoltage(point.CoreVoltage)
}

func validCoreVoltage(voltage int) bool {
	return voltage >= 500 && voltage <= 2000
}

func timeValue(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixNano()
}

func storedTime(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(0, value).UTC()
}

func nullableFloat(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}
