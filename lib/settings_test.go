package lib

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func loadTestSettings(t *testing.T, contents string) (SettingsFile, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "settings.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write settings: %v", err)
	}
	return LoadSettings(path)
}

func TestLoadSettingsAppliesOptimizerDefaults(t *testing.T) {
	settingsFile, err := loadTestSettings(t, "defaults: {}\n")
	if err != nil {
		t.Fatalf("LoadSettings returned an error: %v", err)
	}
	defaults, err := settingsFile.ForHost("")
	if err != nil {
		t.Fatalf("resolve defaults: %v", err)
	}

	if defaults.RecoveryTemp != 61 ||
		defaults.TargetTemp != 65 ||
		defaults.TempLimit != 66 ||
		defaults.TempCutoff != 70 ||
		defaults.MaxPower != 24 ||
		defaults.VRTempHigh != 97 {
		t.Fatalf("safety defaults = %+v", defaults)
	}
	if defaults.MaxErrorPercentage != 5 ||
		defaults.OverheatCooldownMins != 120 {
		t.Fatalf("optimizer defaults = %+v", defaults)
	}
	if defaults.MetricsTime != 10*time.Second ||
		defaults.RampUpTime != time.Minute ||
		defaults.EvaluationWindowTime != 5*time.Minute {
		t.Fatalf("default intervals = %+v", defaults)
	}
}

func TestExampleSettingsLoads(t *testing.T) {
	settingsFile, err := LoadSettings(filepath.Join("..", "settings.example.yaml"))
	if err != nil {
		t.Fatalf("load settings.example.yaml: %v", err)
	}
	if _, err := settingsFile.ForHost("bitaxe-example"); err != nil {
		t.Fatalf("resolve example override: %v", err)
	}
}

func TestLoadSettingsMergesPerHostOptimizerOverrides(t *testing.T) {
	settingsFile, err := loadTestSettings(t, `defaults:
  recoveryTemp: 61
  targetTemp: 65
  tempLimit: 66
overrides:
  mineira:
    targetTemp: 64
    tempLimit: 65
    rampUpSeconds: 90
    evaluationWindowMinutes: 10
`)
	if err != nil {
		t.Fatalf("LoadSettings returned an error: %v", err)
	}
	mineira, err := settingsFile.ForHost("mineira")
	if err != nil {
		t.Fatalf("resolve mineira settings: %v", err)
	}
	if mineira.TargetTemp != 64 || mineira.TempLimit != 65 {
		t.Fatalf("mineira optimizer overrides = %+v", mineira)
	}
	if mineira.RampUpTime != 90*time.Second ||
		mineira.EvaluationWindowTime != 10*time.Minute {
		t.Fatalf("mineira intervals = %+v", mineira)
	}
}

func TestLoadSettingsCanOverrideSkipWithFalse(t *testing.T) {
	settingsFile, err := loadTestSettings(t, `defaults:
  skip: true
overrides:
  mineira:
    skip: false
`)
	if err != nil {
		t.Fatalf("LoadSettings returned an error: %v", err)
	}
	mineira, err := settingsFile.ForHost("mineira")
	if err != nil {
		t.Fatalf("resolve mineira settings: %v", err)
	}
	if mineira.Skip {
		t.Fatal("mineira remained skipped, want explicit false override")
	}
}

func TestLoadSettingsRejectsUnknownKeys(t *testing.T) {
	const key = "unknownSetting"
	_, err := loadTestSettings(t, "defaults:\n  "+key+": 25\n")
	if err == nil || !strings.Contains(err.Error(), key) {
		t.Fatalf("error = %v, want rejection of %q", err, key)
	}
}

func TestLoadSettingsRejectsUnsafeThresholds(t *testing.T) {
	tests := []struct {
		name     string
		settings string
		want     string
	}{
		{
			name: "recovery above target",
			settings: `defaults:
  recoveryTemp: 65
  targetTemp: 65
`,
			want: "recoveryTemp",
		},
		{
			name: "target not below limit",
			settings: `defaults:
  targetTemp: 66
  tempLimit: 66
`,
			want: "targetTemp",
		},
		{
			name: "cutoff not above limit",
			settings: `defaults:
  tempLimit: 70
  tempCutoff: 70
`,
			want: "tempCutoff",
		},
		{
			name: "zero override",
			settings: `defaults: {}
overrides:
  mineira:
    evaluationWindowMinutes: 0
`,
			want: `override for "mineira"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadTestSettings(t, test.settings)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}
