package lib

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
)

const optimizerSchemaVersion = 1

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
	PointValidated = "validated"
	PointUnstable  = "unstable"
	PointNoGain    = "no_gain"
	PointThermal   = "thermal"
	PointPower     = "power"
	PointVRHot     = "vr_hot"
)

// MutationKind identifies the durable hardware mutation represented by a
// pending operating-point pair.
type MutationKind string

const (
	MutationOperatingPoint      MutationKind = "operating_point"
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
	OverheatPending       bool
	OverheatCount         int
	CooldownUntil         time.Time
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
	MacAddr       string
	Frequency     int
	CoreVoltage   int
	Status        string
	MedianHash    float64
	ExpectedHash  float64
	Attainment    float64
	MeanTemp      float64
	P95Temp       float64
	P95VRTemp     float64
	P95Power      float64
	ErrorPercent  *float64
	AcceptedDelta uint64
	RejectedDelta uint64
	MeasuredAt    time.Time
	RetryAfter    time.Time
}

func (record OperatingPointRecord) Point() OperatingPoint {
	return OperatingPoint{
		Frequency:   record.Frequency,
		CoreVoltage: record.CoreVoltage,
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
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("open optimizer database: path cannot be empty")
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("open optimizer database: resolve path: %w", err)
	}

	database, err := sql.Open("sqlite3", optimizerSQLiteDSN(absolutePath))
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

	if err := ensureOptimizerSchema(ctx, conn); err != nil {
		return nil, err
	}
	if err := validateStoredOptimizerData(ctx, conn); err != nil {
		return nil, fmt.Errorf("validate optimizer database contents: %w", err)
	}

	closeConnection = false
	closeDatabase = false
	return &OptimizerStore{db: database, conn: conn}, nil
}

func optimizerSQLiteDSN(path string) string {
	return (&url.URL{
		Scheme:   "file",
		Path:     path,
		RawQuery: "_busy_timeout=1000",
	}).String()
}

func (store *OptimizerStore) LoadOrCreate(
	info Info,
	ip string,
	now time.Time,
) (MinerState, bool, error) {
	if strings.TrimSpace(info.Hostname) == "" || strings.TrimSpace(info.MacAddr) == "" {
		return MinerState{}, false, fmt.Errorf(
			"load miner state: hostname and MAC address are required",
		)
	}
	if strings.TrimSpace(ip) == "" {
		return MinerState{}, false, fmt.Errorf("load miner state: IP cannot be empty")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("load miner state"); err != nil {
		return MinerState{}, false, err
	}

	state, err := queryMiner(store.conn, info.MacAddr)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return MinerState{}, false, fmt.Errorf("load miner state: %w", err)
	}
	if errors.Is(err, sql.ErrNoRows) {
		state = MinerState{
			MacAddr:        info.MacAddr,
			Hostname:       info.Hostname,
			IP:             ip,
			Phase:          PhaseBaseline,
			PhaseStartedAt: now,
		}
		if info.OverHeatMode != 0 {
			state.Phase = PhaseOverheat
			state.OverheatPending = true
			state.OverheatCount = 1
		} else if point := pointFromInfo(info); validStoredPoint(point) {
			state.SetCurrentPoint(point)
		}
		if err := saveMiner(store.conn, &state); err != nil {
			return MinerState{}, false, err
		}
		return state, true, nil
	}

	changed := false
	if state.Hostname != info.Hostname {
		state.Hostname = info.Hostname
		changed = true
	}
	if state.IP != ip {
		state.IP = ip
		changed = true
	}
	if changed {
		if err := saveMiner(store.conn, &state); err != nil {
			return MinerState{}, false, err
		}
	} else if err := validateMinerState(state); err != nil {
		return MinerState{}, false, fmt.Errorf("load miner state: %w", err)
	}
	return state, false, nil
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

func (store *OptimizerStore) SavePoint(record *OperatingPointRecord) error {
	if record == nil {
		return fmt.Errorf("save operating point: record is nil")
	}
	if err := validatePointRecord(*record); err != nil {
		return fmt.Errorf("save operating point: %w", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("save operating point"); err != nil {
		return err
	}
	_, err := store.conn.ExecContext(
		context.Background(),
		`INSERT INTO operating_points (
			mac_addr, frequency, core_voltage, status, median_hash, expected_hash,
			attainment, mean_temp, p95_temp, p95_vr_temp, p95_power, error_percent,
			accepted_delta, rejected_delta, measured_at, retry_after
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(mac_addr, frequency, core_voltage) DO UPDATE SET
			status = excluded.status,
			median_hash = excluded.median_hash,
			expected_hash = excluded.expected_hash,
			attainment = excluded.attainment,
			mean_temp = excluded.mean_temp,
			p95_temp = excluded.p95_temp,
			p95_vr_temp = excluded.p95_vr_temp,
			p95_power = excluded.p95_power,
			error_percent = excluded.error_percent,
			accepted_delta = excluded.accepted_delta,
			rejected_delta = excluded.rejected_delta,
			measured_at = excluded.measured_at,
			retry_after = excluded.retry_after`,
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
		timeValue(record.RetryAfter),
	)
	if err != nil {
		return fmt.Errorf("save operating point: %w", err)
	}
	return nil
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
			measured_at, retry_after
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

func scanPointRows(rows *sql.Rows) ([]OperatingPointRecord, error) {
	var records []OperatingPointRecord
	for rows.Next() {
		var record OperatingPointRecord
		var errorPercent sql.NullFloat64
		var measuredAt int64
		var retryAfter int64
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
			&retryAfter,
		); err != nil {
			return nil, err
		}
		if errorPercent.Valid {
			value := errorPercent.Float64
			record.ErrorPercent = &value
		}
		record.MeasuredAt = storedTime(measuredAt)
		record.RetryAfter = storedTime(retryAfter)
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
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
	if err := validateMinerState(*state); err != nil {
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
			consecutive_bad_windows, overheat_pending, overheat_count,
			cooldown_until
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
			overheat_pending = excluded.overheat_pending,
			overheat_count = excluded.overheat_count,
			cooldown_until = excluded.cooldown_until`,
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
		state.OverheatPending,
		state.OverheatCount,
		timeValue(state.CooldownUntil),
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
	consecutive_bad_windows, overheat_pending, overheat_count,
	cooldown_until
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
		&state.OverheatPending,
		&state.OverheatCount,
		&cooldownUntil,
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
	return state, nil
}

type schemaColumn struct {
	name    string
	sqlType string
	notNull int
	pk      int
}

var optimizerSchema = map[string][]schemaColumn{
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
		{name: "overheat_pending", sqlType: "INTEGER", notNull: 1},
		{name: "overheat_count", sqlType: "INTEGER", notNull: 1},
		{name: "cooldown_until", sqlType: "INTEGER", notNull: 1},
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
		{name: "retry_after", sqlType: "INTEGER", notNull: 1},
	},
}

const createOptimizerSchema = `
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
	overheat_pending INTEGER NOT NULL,
	overheat_count INTEGER NOT NULL,
	cooldown_until INTEGER NOT NULL
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
	retry_after INTEGER NOT NULL,
	PRIMARY KEY (mac_addr, frequency, core_voltage)
);
PRAGMA user_version = 1;
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
		return nil
	}
	if version != optimizerSchemaVersion {
		return incompatibleSchema(version)
	}
	if err := validateOptimizerSchema(ctx, conn); err != nil {
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
	for _, macAddr := range macAddresses {
		state, err := queryMiner(conn, macAddr)
		if err != nil {
			return err
		}
		if err := validateMinerState(state); err != nil {
			return fmt.Errorf("miner %s: %w", macAddr, err)
		}
	}

	rows, err = conn.QueryContext(
		ctx,
		`SELECT mac_addr, frequency, core_voltage, status, median_hash,
			expected_hash, attainment, mean_temp, p95_temp, p95_vr_temp,
			p95_power, error_percent, accepted_delta, rejected_delta,
			measured_at, retry_after
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
	return nil
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
	case !validOptionalPoint(state.FallbackPoint()):
		return fmt.Errorf("fallback operating point is invalid")
	case state.PendingKind == "" &&
		(state.PendingFrequency != 0 ||
			state.PendingCoreVoltage != 0 ||
			!state.PendingSince.IsZero()):
		return fmt.Errorf("pending mutation fields exist without a mutation kind")
	case state.PendingKind != "" &&
		state.PendingKind != MutationOperatingPoint &&
		state.PendingKind != MutationOverheatRecovery:
		return fmt.Errorf("pending mutation kind %q is invalid", state.PendingKind)
	case state.PendingKind != "" && !validStoredPoint(state.PendingPoint()):
		return fmt.Errorf("pending operating point is invalid")
	case state.PendingKind != "" && state.PendingSince.IsZero():
		return fmt.Errorf("pending mutation has no timestamp")
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
	default:
		return nil
	}
}

func validatePointRecord(record OperatingPointRecord) error {
	normalizedMAC, macErr := normalizeMAC(record.MacAddr)
	switch {
	case macErr != nil || normalizedMAC != record.MacAddr:
		return fmt.Errorf("MAC address is invalid or non-canonical")
	case !validStoredPoint(record.Point()):
		return fmt.Errorf("operating point is invalid")
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
	default:
		return nil
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
	case PointValidated,
		PointUnstable,
		PointNoGain,
		PointThermal,
		PointPower,
		PointVRHot:
		return true
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
