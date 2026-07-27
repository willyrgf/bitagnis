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

func TestLoadMiningPasswordsReadsOneDotEnvSnapshot(t *testing.T) {
	settings := enabledMiningSettings()
	path := filepath.Join(t.TempDir(), ".env")
	contents := `# Synthetic test credentials.
BITAGNIS_PRIMARY_PASSWORD='synthetic-primary=#value'
BITAGNIS_FALLBACK_PASSWORD="synthetic-fallback=\"quoted\""
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	primary, fallback, err := LoadMiningPasswords(path, settings)
	if err != nil {
		t.Fatalf("LoadMiningPasswords returned an error: %v", err)
	}
	if primary != "synthetic-primary=#value" ||
		fallback != `synthetic-fallback="quoted"` {
		t.Fatalf("passwords were not parsed as expected")
	}
}

func TestLoadMiningPasswordsDoesNotFallBackToProcessEnvironment(t *testing.T) {
	settings := enabledMiningSettings()
	t.Setenv(settings.Primary.PasswordEnv, "synthetic-process-primary")
	t.Setenv(settings.Fallback.PasswordEnv, "synthetic-process-fallback")
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	_, _, err := LoadMiningPasswords(path, settings)
	if err == nil || !strings.Contains(err.Error(), "primary password entry is unavailable") {
		t.Fatalf("error = %v, want missing .env entry", err)
	}
}

func TestLoadMiningPasswordsRejectsMalformedFileWithoutExposingSecret(t *testing.T) {
	const secret = "synthetic-secret-sentinel"
	settings := enabledMiningSettings()
	path := filepath.Join(t.TempDir(), ".env")
	contents := settings.Primary.PasswordEnv + "=\"" + secret + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	_, _, err := LoadMiningPasswords(path, settings)
	if err == nil || !strings.Contains(err.Error(), "line 1 has an invalid value") {
		t.Fatalf("error = %v, want malformed value rejection", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("password file error exposed the secret")
	}
}

func TestLoadMiningPasswordsRejectsDuplicateEntries(t *testing.T) {
	settings := enabledMiningSettings()
	path := filepath.Join(t.TempDir(), ".env")
	contents := settings.Primary.PasswordEnv + "=synthetic-first\n" +
		settings.Primary.PasswordEnv + "=synthetic-second\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	_, _, err := LoadMiningPasswords(path, settings)
	if err == nil || !strings.Contains(err.Error(), "line 2 duplicates an entry") {
		t.Fatalf("error = %v, want duplicate rejection", err)
	}
}

func TestLoadSettingsMergesPerHostOptimizerOverrides(t *testing.T) {
	settingsFile, err := loadTestSettings(t, `defaults:
  recoveryTemp: 61
  targetTemp: 65
  tempLimit: 66
overrides:
  bitaxe-alpha:
    targetTemp: 64
    tempLimit: 65
    rampUpSeconds: 90
    evaluationWindowMinutes: 10
`)
	if err != nil {
		t.Fatalf("LoadSettings returned an error: %v", err)
	}
	bitaxeAlpha, err := settingsFile.ForHost("bitaxe-alpha")
	if err != nil {
		t.Fatalf("resolve bitaxe-alpha settings: %v", err)
	}
	if bitaxeAlpha.TargetTemp != 64 || bitaxeAlpha.TempLimit != 65 {
		t.Fatalf("bitaxe-alpha optimizer overrides = %+v", bitaxeAlpha)
	}
	if bitaxeAlpha.RampUpTime != 90*time.Second ||
		bitaxeAlpha.EvaluationWindowTime != 10*time.Minute {
		t.Fatalf("bitaxe-alpha intervals = %+v", bitaxeAlpha)
	}
}

func TestLoadSettingsCanOverrideSkipWithFalse(t *testing.T) {
	settingsFile, err := loadTestSettings(t, `defaults:
  skip: true
overrides:
  bitaxe-alpha:
    skip: false
`)
	if err != nil {
		t.Fatalf("LoadSettings returned an error: %v", err)
	}
	bitaxeAlpha, err := settingsFile.ForHost("bitaxe-alpha")
	if err != nil {
		t.Fatalf("resolve bitaxe-alpha settings: %v", err)
	}
	if bitaxeAlpha.Skip {
		t.Fatal("bitaxe-alpha remained skipped, want explicit false override")
	}
}

func TestLoadSettingsMergesNestedMiningSettingsPerHost(t *testing.T) {
	settingsFile, err := loadTestSettings(t, `defaults:
  mining:
    enabled: false
    primary:
      host: pool.example.net
      port: 3333
      user: common-worker
      passwordEnv: BITAGNIS_PRIMARY_PASSWORD
    fallback:
      host: fallback.example.net
      port: 4444
      user: common-worker
      passwordEnv: BITAGNIS_FALLBACK_PASSWORD
overrides:
  bitaxe-alpha:
    mining:
      enabled: true
      primary:
        user: worker-bitaxe-alpha
      fallback:
        user: worker-bitaxe-alpha
`)
	if err != nil {
		t.Fatalf("LoadSettings returned an error: %v", err)
	}
	defaults, err := settingsFile.ForHost("")
	if err != nil {
		t.Fatalf("resolve defaults: %v", err)
	}
	if defaults.Mining.Enabled {
		t.Fatal("mining was not disabled by default")
	}
	bitaxeAlpha, err := settingsFile.ForHost("bitaxe-alpha")
	if err != nil {
		t.Fatalf("resolve bitaxe-alpha: %v", err)
	}
	if !bitaxeAlpha.Mining.Enabled ||
		bitaxeAlpha.Mining.Primary.Host != "pool.example.net" ||
		bitaxeAlpha.Mining.Primary.User != "worker-bitaxe-alpha" ||
		bitaxeAlpha.Mining.Fallback.Port != 4444 ||
		bitaxeAlpha.Mining.Fallback.User != "worker-bitaxe-alpha" {
		t.Fatalf("merged mining settings = %+v", bitaxeAlpha.Mining)
	}
}

func TestLoadSettingsRejectsLiteralMiningPassword(t *testing.T) {
	_, err := loadTestSettings(t, `defaults:
  mining:
    primary:
      password: synthetic-secret
`)
	if err == nil || !strings.Contains(err.Error(), "password") {
		t.Fatalf("error = %v, want literal password rejection", err)
	}
}

func TestLoadSettingsRequiresCompleteEnabledMiningConfiguration(t *testing.T) {
	_, err := loadTestSettings(t, `defaults:
  mining:
    enabled: false
    primary:
      host: pool.example.net
      port: 3333
      user: worker
      passwordEnv: BITAGNIS_PRIMARY_PASSWORD
overrides:
  bitaxe-alpha:
    mining:
      enabled: true
`)
	if err == nil || !strings.Contains(err.Error(), "fallback") {
		t.Fatalf("error = %v, want incomplete fallback rejection", err)
	}
}

func TestLoadSettingsAllowsDefaultMiningEnablement(t *testing.T) {
	settingsFile, err := loadTestSettings(t, `defaults:
  mining:
    enabled: true
    primary:
      host: pool.example.net
      port: 3333
      user: worker
      passwordEnv: BITAGNIS_PRIMARY_PASSWORD
    fallback:
      host: fallback.example.net
      port: 3333
      user: worker
      passwordEnv: BITAGNIS_FALLBACK_PASSWORD
overrides:
  bitaxe-alpha:
    mining:
      enabled: false
`)
	if err != nil {
		t.Fatalf("LoadSettings returned an error: %v", err)
	}
	defaults, err := settingsFile.ForHost("")
	if err != nil {
		t.Fatalf("resolve defaults: %v", err)
	}
	bitaxeAlpha, err := settingsFile.ForHost("bitaxe-alpha")
	if err != nil {
		t.Fatalf("resolve bitaxe-alpha: %v", err)
	}
	if !defaults.Mining.Enabled || bitaxeAlpha.Mining.Enabled {
		t.Fatalf(
			"default/override mining enabled = %v/%v, want true/false",
			defaults.Mining.Enabled,
			bitaxeAlpha.Mining.Enabled,
		)
	}
}

func TestMiningValidationBoundaries(t *testing.T) {
	valid := enabledMiningSettings()
	tests := []struct {
		name   string
		mutate func(*MiningSettings)
		want   string
	}{
		{
			name: "scheme",
			mutate: func(settings *MiningSettings) {
				settings.Primary.Host = "stratum+tcp://pool.example.net"
			},
			want: "primary host",
		},
		{
			name: "host port",
			mutate: func(settings *MiningSettings) {
				settings.Primary.Host = "pool.example.net:3333"
			},
			want: "primary host",
		},
		{
			name: "zero port",
			mutate: func(settings *MiningSettings) {
				settings.Primary.Port = 0
			},
			want: "primary port",
		},
		{
			name: "surrounding user whitespace",
			mutate: func(settings *MiningSettings) {
				settings.Primary.User = " worker"
			},
			want: "primary user",
		},
		{
			name: "nonportable dotenv entry",
			mutate: func(settings *MiningSettings) {
				settings.Primary.PasswordEnv = "PRIMARY-PASSWORD"
			},
			want: "passwordEnv",
		},
		{
			name: "oversized UTF-8 user",
			mutate: func(settings *MiningSettings) {
				settings.Primary.User = strings.Repeat("é", 128)
			},
			want: "primary user",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			settings := valid
			test.mutate(&settings)
			err := validateMiningSettings(settings)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want %q", err, test.want)
			}
		})
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
  bitaxe-alpha:
    evaluationWindowMinutes: 0
`,
			want: `override for "bitaxe-alpha"`,
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
