package lib

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

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

// MinerState contains only durable optimizer control state. Telemetry samples
// remain in memory and are intentionally restarted after a process restart.
type MinerState struct {
	MacAddr  string `gorm:"primaryKey"`
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

	PendingFrequency   int
	PendingCoreVoltage int
	PendingSince       time.Time
	PendingRecovery    bool

	ObservedFrequency   int
	ObservedCoreVoltage int
	ObservedCount       int

	ConsecutiveBadWindows int
	OverheatPending       bool
	OverheatCount         int
	CooldownUntil         time.Time
}

func (MinerState) TableName() string {
	return "optimizer_miners"
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

func (state *MinerState) SetPendingPoint(point OperatingPoint) {
	state.PendingFrequency = point.Frequency
	state.PendingCoreVoltage = point.CoreVoltage
}

func (state *MinerState) ClearPendingPoint() {
	state.PendingFrequency = 0
	state.PendingCoreVoltage = 0
	state.PendingSince = time.Time{}
	state.PendingRecovery = false
}

type OperatingPointRecord struct {
	MacAddr       string `gorm:"primaryKey"`
	Frequency     int    `gorm:"primaryKey"`
	CoreVoltage   int    `gorm:"primaryKey"`
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

func (OperatingPointRecord) TableName() string {
	return "operating_points"
}

func (record OperatingPointRecord) Point() OperatingPoint {
	return OperatingPoint{
		Frequency:   record.Frequency,
		CoreVoltage: record.CoreVoltage,
	}
}

type OptimizerStore struct {
	mu sync.Mutex
	db *gorm.DB
}

func OpenOptimizerStore(path string) (*OptimizerStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("open optimizer database: path cannot be empty")
	}

	customLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true,
		},
	)
	database, err := gorm.Open(sqlite.Open(path), &gorm.Config{Logger: customLogger})
	if err != nil {
		return nil, fmt.Errorf("open optimizer database: %w", err)
	}
	sqlDatabase, err := database.DB()
	if err != nil {
		return nil, fmt.Errorf("access optimizer database connection: %w", err)
	}
	sqlDatabase.SetMaxOpenConns(1)
	sqlDatabase.SetMaxIdleConns(1)

	if err := database.AutoMigrate(&MinerState{}, &OperatingPointRecord{}); err != nil {
		return nil, fmt.Errorf("create optimizer schema: %w", err)
	}
	return &OptimizerStore{db: database}, nil
}

func (store *OptimizerStore) LoadOrCreate(
	info Info,
	ip string,
	now time.Time,
) (MinerState, bool, error) {
	if store == nil || store.db == nil {
		return MinerState{}, false, fmt.Errorf("load miner state: store is not initialized")
	}
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

	var state MinerState
	err := store.db.Where("mac_addr = ?", info.MacAddr).First(&state).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return MinerState{}, false, fmt.Errorf("load miner state: %w", err)
	}
	if err == gorm.ErrRecordNotFound {
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
		if err := store.saveMiner(&state); err != nil {
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
		if err := store.saveMiner(&state); err != nil {
			return MinerState{}, false, err
		}
	} else if err := validateMinerState(state); err != nil {
		return MinerState{}, false, fmt.Errorf("load miner state: %w", err)
	}
	return state, false, nil
}

func (store *OptimizerStore) SaveMiner(state *MinerState) error {
	if store == nil || store.db == nil {
		return fmt.Errorf("save miner state: store is not initialized")
	}
	if state == nil {
		return fmt.Errorf("save miner state: state is nil")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.saveMiner(state)
}

func (store *OptimizerStore) SavePoint(record *OperatingPointRecord) error {
	if store == nil || store.db == nil {
		return fmt.Errorf("save operating point: store is not initialized")
	}
	if record == nil {
		return fmt.Errorf("save operating point: record is nil")
	}
	if err := validatePointRecord(*record); err != nil {
		return fmt.Errorf("save operating point: %w", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.db.Save(record).Error; err != nil {
		return fmt.Errorf("save operating point: %w", err)
	}
	return nil
}

func (store *OptimizerStore) ListPoints(macAddr string) ([]OperatingPointRecord, error) {
	if store == nil || store.db == nil {
		return nil, fmt.Errorf("list operating points: store is not initialized")
	}
	if strings.TrimSpace(macAddr) == "" {
		return nil, fmt.Errorf("list operating points: MAC address is empty")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	var records []OperatingPointRecord
	if err := store.db.
		Where("mac_addr = ?", macAddr).
		Order("frequency ASC, core_voltage ASC").
		Find(&records).Error; err != nil {
		return nil, fmt.Errorf("list operating points: %w", err)
	}
	return records, nil
}

func (store *OptimizerStore) Close() error {
	if store == nil || store.db == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	sqlDatabase, err := store.db.DB()
	if err != nil {
		return fmt.Errorf("close optimizer database: %w", err)
	}
	if err := sqlDatabase.Close(); err != nil {
		return fmt.Errorf("close optimizer database: %w", err)
	}
	return nil
}

func (store *OptimizerStore) saveMiner(state *MinerState) error {
	if err := validateMinerState(*state); err != nil {
		return fmt.Errorf("save miner state: %w", err)
	}
	if err := store.db.Save(state).Error; err != nil {
		return fmt.Errorf("save miner state: %w", err)
	}
	return nil
}

func validateMinerState(state MinerState) error {
	switch {
	case strings.TrimSpace(state.MacAddr) == "":
		return fmt.Errorf("MAC address is empty")
	case strings.TrimSpace(state.Hostname) == "":
		return fmt.Errorf("hostname is empty")
	case strings.TrimSpace(state.IP) == "":
		return fmt.Errorf("IP is empty")
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
	case !validOptionalPoint(state.PendingPoint()):
		return fmt.Errorf("pending operating point is invalid")
	case state.PendingFrequency == 0 && !state.PendingSince.IsZero():
		return fmt.Errorf("pending timestamp exists without a pending operating point")
	case state.PendingFrequency != 0 && state.PendingSince.IsZero():
		return fmt.Errorf("pending operating point has no timestamp")
	case state.PendingRecovery && state.PendingFrequency == 0:
		return fmt.Errorf("pending recovery has no operating point")
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
	switch {
	case strings.TrimSpace(record.MacAddr) == "":
		return fmt.Errorf("MAC address is empty")
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
