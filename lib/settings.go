package lib

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

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
)

type Settings struct {
	Skip                    bool    `yaml:"skip"`
	RecoveryTemp            float64 `yaml:"recoveryTemp"`
	TargetTemp              float64 `yaml:"targetTemp"`
	TempLimit               float64 `yaml:"tempLimit"`
	TempCutoff              float64 `yaml:"tempCutoff"`
	MaxPower                float64 `yaml:"maxPower"`
	VRTempHigh              float64 `yaml:"vrTempHigh"`
	MaxErrorPercentage      float64 `yaml:"maxErrorPercentage"`
	MetricsInterval         int     `yaml:"metricsInterval"`
	RampUpSeconds           int     `yaml:"rampUpSeconds"`
	EvaluationWindowMinutes int     `yaml:"evaluationWindowMinutes"`
	OverheatCooldownMins    int     `yaml:"overheatCooldownMinutes"`

	MetricsTime          time.Duration `yaml:"-"`
	RampUpTime           time.Duration `yaml:"-"`
	EvaluationWindowTime time.Duration `yaml:"-"`
}

// SettingsOverride uses pointers so false and an omitted setting remain
// distinguishable. Explicit zero values are rejected after merging.
type SettingsOverride struct {
	Skip                    *bool    `yaml:"skip"`
	RecoveryTemp            *float64 `yaml:"recoveryTemp"`
	TargetTemp              *float64 `yaml:"targetTemp"`
	TempLimit               *float64 `yaml:"tempLimit"`
	TempCutoff              *float64 `yaml:"tempCutoff"`
	MaxPower                *float64 `yaml:"maxPower"`
	VRTempHigh              *float64 `yaml:"vrTempHigh"`
	MaxErrorPercentage      *float64 `yaml:"maxErrorPercentage"`
	RampUpSeconds           *int     `yaml:"rampUpSeconds"`
	EvaluationWindowMinutes *int     `yaml:"evaluationWindowMinutes"`
	OverheatCooldownMins    *int     `yaml:"overheatCooldownMinutes"`
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
	return withDurations(settings)
}
