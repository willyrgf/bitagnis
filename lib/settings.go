package lib

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"
)

const (
	defaultRecoveryTemp         = 61
	defaultTargetTemp           = 65
	defaultTempLimit            = 66
	defaultTempCutoff           = 70
	defaultMaxPower             = 24
	defaultVRTempHigh           = 97
	defaultMaxErrorPercentage   = 5
	defaultMetricsInterval      = 10
	defaultRampUpSeconds        = 60
	defaultEvaluationWindowMins = 5
	defaultOverheatCooldownMins = 120
	maxPasswordFileBytes        = 64 * 1024
)

type Settings struct {
	Skip                    bool           `yaml:"skip"`
	RecoveryTemp            float64        `yaml:"recoveryTemp"`
	TargetTemp              float64        `yaml:"targetTemp"`
	TempLimit               float64        `yaml:"tempLimit"`
	TempCutoff              float64        `yaml:"tempCutoff"`
	MaxPower                float64        `yaml:"maxPower"`
	VRTempHigh              float64        `yaml:"vrTempHigh"`
	MaxErrorPercentage      float64        `yaml:"maxErrorPercentage"`
	MetricsInterval         int            `yaml:"metricsInterval"`
	RampUpSeconds           int            `yaml:"rampUpSeconds"`
	EvaluationWindowMinutes int            `yaml:"evaluationWindowMinutes"`
	OverheatCooldownMins    int            `yaml:"overheatCooldownMinutes"`
	Mining                  MiningSettings `yaml:"mining"`

	MetricsTime          time.Duration `yaml:"-"`
	RampUpTime           time.Duration `yaml:"-"`
	EvaluationWindowTime time.Duration `yaml:"-"`
}

// MiningSettings is the complete desired primary and fallback Stratum
// configuration for one effective hostname.
type MiningSettings struct {
	Enabled  bool         `yaml:"enabled"`
	Primary  PoolSettings `yaml:"primary"`
	Fallback PoolSettings `yaml:"fallback"`
}

// PoolSettings identifies one desired Stratum endpoint and the dotenv entry
// that supplies its write-only password.
type PoolSettings struct {
	Host        string `yaml:"host"`
	Port        int    `yaml:"port"`
	User        string `yaml:"user"`
	PasswordEnv string `yaml:"passwordEnv"`
}

// SettingsOverride uses pointers so false and an omitted setting remain
// distinguishable. Explicit zero values are rejected after merging.
type SettingsOverride struct {
	Skip                    *bool                   `yaml:"skip"`
	RecoveryTemp            *float64                `yaml:"recoveryTemp"`
	TargetTemp              *float64                `yaml:"targetTemp"`
	TempLimit               *float64                `yaml:"tempLimit"`
	TempCutoff              *float64                `yaml:"tempCutoff"`
	MaxPower                *float64                `yaml:"maxPower"`
	VRTempHigh              *float64                `yaml:"vrTempHigh"`
	MaxErrorPercentage      *float64                `yaml:"maxErrorPercentage"`
	RampUpSeconds           *int                    `yaml:"rampUpSeconds"`
	EvaluationWindowMinutes *int                    `yaml:"evaluationWindowMinutes"`
	OverheatCooldownMins    *int                    `yaml:"overheatCooldownMinutes"`
	Mining                  *MiningSettingsOverride `yaml:"mining"`
}

// MiningSettingsOverride retains omission semantics while merging nested
// hostname overrides.
type MiningSettingsOverride struct {
	Enabled  *bool                 `yaml:"enabled"`
	Primary  *PoolSettingsOverride `yaml:"primary"`
	Fallback *PoolSettingsOverride `yaml:"fallback"`
}

// PoolSettingsOverride retains omission semantics for individual pool fields.
type PoolSettingsOverride struct {
	Host        *string `yaml:"host"`
	Port        *int    `yaml:"port"`
	User        *string `yaml:"user"`
	PasswordEnv *string `yaml:"passwordEnv"`
}

type SettingsFile struct {
	Defaults  Settings                    `yaml:"defaults"`
	Overrides map[string]SettingsOverride `yaml:"overrides"`
}

func LoadSettings(path string) (SettingsFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return SettingsFile{}, fmt.Errorf("read settings %q: %w", path, err)
	}

	var settingsFile SettingsFile
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&settingsFile); err != nil {
		return SettingsFile{}, fmt.Errorf("decode settings %q: %w", path, err)
	}
	var extraDocument any
	if err := decoder.Decode(&extraDocument); err != io.EOF {
		if err == nil {
			return SettingsFile{}, fmt.Errorf(
				"decode settings %q: multiple YAML documents are not allowed",
				path,
			)
		}
		return SettingsFile{}, fmt.Errorf("decode settings %q: %w", path, err)
	}

	settingsFile.Defaults = withDefaults(settingsFile.Defaults)
	if err := validateSettings(settingsFile.Defaults); err != nil {
		return SettingsFile{}, fmt.Errorf("settings defaults: %w", err)
	}

	hostnames := make([]string, 0, len(settingsFile.Overrides))
	for hostname := range settingsFile.Overrides {
		hostnames = append(hostnames, hostname)
	}
	sort.Strings(hostnames)
	for _, hostname := range hostnames {
		if strings.TrimSpace(hostname) == "" || strings.TrimSpace(hostname) != hostname {
			return SettingsFile{}, fmt.Errorf(
				"settings overrides: hostname %q is empty or has surrounding whitespace",
				hostname,
			)
		}
		if _, err := settingsFile.ForHost(hostname); err != nil {
			return SettingsFile{}, err
		}
	}
	return settingsFile, nil
}

// LoadMiningPasswords reads the primary and fallback passwords named by
// settings from one dotenv file snapshot.
func LoadMiningPasswords(
	path string,
	settings MiningSettings,
) (string, string, error) {
	if !settings.Enabled {
		return "", "", fmt.Errorf("mining is not enabled")
	}
	if err := validateMiningSettings(settings); err != nil {
		return "", "", err
	}

	file, err := os.Open(path)
	if err != nil {
		return "", "", fmt.Errorf("open password file %q: %w", path, err)
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, maxPasswordFileBytes+1))
	if err != nil {
		return "", "", fmt.Errorf("read password file %q: %w", path, err)
	}
	if len(data) > maxPasswordFileBytes {
		return "", "", fmt.Errorf(
			"read password file %q: file exceeds %d bytes",
			path,
			maxPasswordFileBytes,
		)
	}
	values, err := parseDotEnv(data)
	if err != nil {
		return "", "", fmt.Errorf("parse password file %q: %w", path, err)
	}

	primary, primaryExists := values[settings.Primary.PasswordEnv]
	if !primaryExists {
		return "", "", fmt.Errorf("primary password entry is unavailable")
	}
	fallback, fallbackExists := values[settings.Fallback.PasswordEnv]
	if !fallbackExists {
		return "", "", fmt.Errorf("fallback password entry is unavailable")
	}
	if err := validateResolvedPassword("primary", primary); err != nil {
		return "", "", fmt.Errorf("primary password entry is invalid")
	}
	if err := validateResolvedPassword("fallback", fallback); err != nil {
		return "", "", fmt.Errorf("fallback password entry is invalid")
	}
	return primary, fallback, nil
}

func parseDotEnv(data []byte) (map[string]string, error) {
	values := make(map[string]string)
	for index, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if strings.TrimSpace(line) == "" ||
			strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		name, value, found := strings.Cut(line, "=")
		if !found {
			return nil, fmt.Errorf("line %d must use NAME=VALUE", index+1)
		}
		if !validDotEnvName(name) {
			return nil, fmt.Errorf("line %d has an invalid name", index+1)
		}
		if _, exists := values[name]; exists {
			return nil, fmt.Errorf("line %d duplicates an entry", index+1)
		}
		value, err := parseDotEnvValue(value)
		if err != nil {
			return nil, fmt.Errorf("line %d has an invalid value", index+1)
		}
		values[name] = value
	}
	return values, nil
}

func parseDotEnvValue(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	switch value[0] {
	case '\'':
		if len(value) < 2 || value[len(value)-1] != '\'' {
			return "", fmt.Errorf("unterminated single-quoted value")
		}
		return value[1 : len(value)-1], nil
	case '"':
		unquoted, err := strconv.Unquote(value)
		if err != nil {
			return "", fmt.Errorf("invalid double-quoted value")
		}
		return unquoted, nil
	default:
		return value, nil
	}
}

func (settingsFile SettingsFile) ForHost(hostname string) (Settings, error) {
	settings := withDefaults(settingsFile.Defaults)
	if override, ok := settingsFile.Overrides[hostname]; hostname != "" && ok {
		settings = mergeSettings(settings, override)
	}
	settings = withDurations(settings)
	if err := validateSettings(settings); err != nil {
		if hostname == "" {
			return Settings{}, fmt.Errorf("settings defaults: %w", err)
		}
		return Settings{}, fmt.Errorf("settings override for %q: %w", hostname, err)
	}
	return settings, nil
}

func withDefaults(settings Settings) Settings {
	if settings.RecoveryTemp == 0 {
		settings.RecoveryTemp = defaultRecoveryTemp
	}
	if settings.TargetTemp == 0 {
		settings.TargetTemp = defaultTargetTemp
	}
	if settings.TempLimit == 0 {
		settings.TempLimit = defaultTempLimit
	}
	if settings.TempCutoff == 0 {
		settings.TempCutoff = defaultTempCutoff
	}
	if settings.MaxPower == 0 {
		settings.MaxPower = defaultMaxPower
	}
	if settings.VRTempHigh == 0 {
		settings.VRTempHigh = defaultVRTempHigh
	}
	if settings.MaxErrorPercentage == 0 {
		settings.MaxErrorPercentage = defaultMaxErrorPercentage
	}
	if settings.MetricsInterval == 0 {
		settings.MetricsInterval = defaultMetricsInterval
	}
	if settings.RampUpSeconds == 0 {
		settings.RampUpSeconds = defaultRampUpSeconds
	}
	if settings.EvaluationWindowMinutes == 0 {
		settings.EvaluationWindowMinutes = defaultEvaluationWindowMins
	}
	if settings.OverheatCooldownMins == 0 {
		settings.OverheatCooldownMins = defaultOverheatCooldownMins
	}
	return withDurations(settings)
}

func withDurations(settings Settings) Settings {
	settings.MetricsTime = time.Duration(settings.MetricsInterval) * time.Second
	settings.RampUpTime = time.Duration(settings.RampUpSeconds) * time.Second
	settings.EvaluationWindowTime =
		time.Duration(settings.EvaluationWindowMinutes) * time.Minute
	return settings
}

func validateSettings(settings Settings) error {
	switch {
	case settings.RecoveryTemp <= 0 || settings.RecoveryTemp >= settings.TargetTemp:
		return fmt.Errorf("recoveryTemp must be greater than 0 and below targetTemp")
	case settings.TargetTemp <= 0 || settings.TargetTemp >= settings.TempLimit:
		return fmt.Errorf("targetTemp must be greater than 0 and below tempLimit")
	case settings.TempLimit <= 0 || settings.TempLimit > 110:
		return fmt.Errorf("tempLimit must be between 1 and 110")
	case settings.TempCutoff <= settings.TempLimit || settings.TempCutoff > 120:
		return fmt.Errorf("tempCutoff must be greater than tempLimit and no more than 120")
	case settings.MaxPower <= 1 || settings.MaxPower > 1000:
		return fmt.Errorf("maxPower must be greater than 1 and no more than 1000")
	case settings.VRTempHigh <= 0 || settings.VRTempHigh > 150:
		return fmt.Errorf("vrTempHigh must be between 1 and 150")
	case settings.MaxErrorPercentage <= 0 || settings.MaxErrorPercentage > 100:
		return fmt.Errorf("maxErrorPercentage must be greater than 0 and no more than 100")
	case settings.MetricsInterval < 2 || settings.MetricsInterval > 60:
		return fmt.Errorf("metricsInterval must be between 2 and 60 seconds")
	case settings.RampUpSeconds < 0 || settings.RampUpSeconds > 30*60:
		return fmt.Errorf("rampUpSeconds must be between 0 and 1800")
	case settings.EvaluationWindowMinutes <= 0 || settings.EvaluationWindowMinutes > 24*60:
		return fmt.Errorf("evaluationWindowMinutes must be between 1 and 1440")
	case settings.OverheatCooldownMins <= 0 || settings.OverheatCooldownMins > 24*60:
		return fmt.Errorf("overheatCooldownMinutes must be between 1 and 1440")
	}
	if err := validateMiningSettings(settings.Mining); err != nil {
		return err
	}
	return nil
}

func mergeSettings(settings Settings, override SettingsOverride) Settings {
	if override.Skip != nil {
		settings.Skip = *override.Skip
	}
	if override.RecoveryTemp != nil {
		settings.RecoveryTemp = *override.RecoveryTemp
	}
	if override.TargetTemp != nil {
		settings.TargetTemp = *override.TargetTemp
	}
	if override.TempLimit != nil {
		settings.TempLimit = *override.TempLimit
	}
	if override.TempCutoff != nil {
		settings.TempCutoff = *override.TempCutoff
	}
	if override.MaxPower != nil {
		settings.MaxPower = *override.MaxPower
	}
	if override.VRTempHigh != nil {
		settings.VRTempHigh = *override.VRTempHigh
	}
	if override.MaxErrorPercentage != nil {
		settings.MaxErrorPercentage = *override.MaxErrorPercentage
	}
	if override.RampUpSeconds != nil {
		settings.RampUpSeconds = *override.RampUpSeconds
	}
	if override.EvaluationWindowMinutes != nil {
		settings.EvaluationWindowMinutes = *override.EvaluationWindowMinutes
	}
	if override.OverheatCooldownMins != nil {
		settings.OverheatCooldownMins = *override.OverheatCooldownMins
	}
	if override.Mining != nil {
		settings.Mining = mergeMiningSettings(settings.Mining, *override.Mining)
	}
	return withDurations(settings)
}

func mergeMiningSettings(
	settings MiningSettings,
	override MiningSettingsOverride,
) MiningSettings {
	if override.Enabled != nil {
		settings.Enabled = *override.Enabled
	}
	if override.Primary != nil {
		settings.Primary = mergePoolSettings(settings.Primary, *override.Primary)
	}
	if override.Fallback != nil {
		settings.Fallback = mergePoolSettings(settings.Fallback, *override.Fallback)
	}
	return settings
}

func mergePoolSettings(settings PoolSettings, override PoolSettingsOverride) PoolSettings {
	if override.Host != nil {
		settings.Host = *override.Host
	}
	if override.Port != nil {
		settings.Port = *override.Port
	}
	if override.User != nil {
		settings.User = *override.User
	}
	if override.PasswordEnv != nil {
		settings.PasswordEnv = *override.PasswordEnv
	}
	return settings
}

func validateMiningSettings(settings MiningSettings) error {
	if !settings.Enabled {
		return nil
	}
	if err := validatePoolSettings("primary", settings.Primary); err != nil {
		return err
	}
	if err := validatePoolSettings("fallback", settings.Fallback); err != nil {
		return err
	}
	return nil
}

func validatePoolSettings(name string, settings PoolSettings) error {
	switch {
	case !validPoolHost(settings.Host):
		return fmt.Errorf("mining %s host is not a bare DNS host or IPv4 address", name)
	case settings.Port < 1 || settings.Port > 65535:
		return fmt.Errorf("mining %s port must be between 1 and 65535", name)
	case !validPoolText(settings.User):
		return fmt.Errorf("mining %s user must be non-empty, at most 255 bytes, and have no surrounding whitespace or control characters", name)
	case !validDotEnvName(settings.PasswordEnv):
		return fmt.Errorf("mining %s passwordEnv is invalid", name)
	default:
		return nil
	}
}

func validPoolHost(host string) bool {
	if host == "" ||
		len([]byte(host)) > 255 ||
		strings.TrimSpace(host) != host ||
		hasControl(host) ||
		strings.ContainsAny(host, ":/\\?#") {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.To4() != nil
	}
	if strings.HasSuffix(host, ".") || len(host) > 253 {
		return false
	}
	labels := strings.Split(host, ".")
	for _, label := range labels {
		if label == "" || len(label) > 63 ||
			label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') &&
				(character < 'A' || character > 'Z') &&
				(character < '0' || character > '9') &&
				character != '-' {
				return false
			}
		}
	}
	return true
}

func validPoolText(value string) bool {
	return value != "" &&
		len([]byte(value)) <= 255 &&
		strings.TrimSpace(value) == value &&
		!hasControl(value)
}

func validDotEnvName(value string) bool {
	if value == "" ||
		len([]byte(value)) > 255 ||
		strings.TrimSpace(value) != value ||
		hasControl(value) {
		return false
	}
	for index, character := range value {
		if index == 0 {
			if character != '_' &&
				(character < 'a' || character > 'z') &&
				(character < 'A' || character > 'Z') {
				return false
			}
			continue
		}
		if character != '_' &&
			(character < 'a' || character > 'z') &&
			(character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func hasControl(value string) bool {
	for _, character := range value {
		if unicode.IsControl(character) {
			return true
		}
	}
	return false
}
