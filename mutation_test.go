package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/willyrgf/bitagnis/lib"
)

func mutationSettings(miningEnabled bool) lib.Settings {
	settings := optimizerSettings()
	settings.RampUpSeconds = 1
	settings.RampUpTime = time.Second
	settings.Mining = lib.MiningSettings{
		Enabled: miningEnabled,
		Primary: lib.PoolSettings{
			Host:        "pool.example.net",
			Port:        3333,
			User:        "worker-primary",
			PasswordEnv: "BITAGNIS_PRIMARY_PASSWORD",
		},
		Fallback: lib.PoolSettings{
			Host:        "fallback.example.net",
			Port:        4444,
			User:        "worker-fallback",
			PasswordEnv: "BITAGNIS_FALLBACK_PASSWORD",
		},
	}
	return settings
}

func matchingMiningInfo() lib.Info {
	info := healthyInfo()
	settings := mutationSettings(true).Mining
	info.StratumURL = settings.Primary.Host
	info.StratumPort = settings.Primary.Port
	info.StratumUser = settings.Primary.User
	info.FallbackStratumURL = settings.Fallback.Host
	info.FallbackStratumPort = settings.Fallback.Port
	info.FallbackStratumUser = settings.Fallback.User
	info.IsUsingFallbackStratum = 0
	return info
}

func observation(
	info lib.Info,
	state lib.MinerState,
	settings lib.Settings,
) *minerObservation {
	return &minerObservation{
		miner:    discovered(info, state.IP),
		info:     info,
		asic:     gammaASIC(),
		settings: settings,
		state:    state,
	}
}

func testMutationCoordinator(
	devices deviceAPI,
	states optimizerStateStore,
	settings lib.Settings,
	info lib.Info,
	reapply map[string]bool,
	discover mutationDiscovery,
	resolvePasswords miningPasswordResolver,
	logger *log.Logger,
) *mutationCoordinator {
	coordinator := newMutationCoordinator(
		devices,
		states,
		lib.SettingsFile{Defaults: settings},
		[]lib.DiscoveredMiner{discovered(info, "192.0.2.10")},
		reapply,
		discover,
		resolvePasswords,
		logger,
		func(string) {},
	)
	coordinator.rediscoveryDelay = 0
	return coordinator
}

func fixedMiningPasswords(password string) miningPasswordResolver {
	return func(lib.MiningSettings) (string, string, error) {
		return password, password, nil
	}
}

func waitForMutation(
	t *testing.T,
	coordinator *mutationCoordinator,
	observations func() map[string]*minerObservation,
	now time.Time,
	condition func() bool,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for mutation")
		}
		if _, err := coordinator.Advance(
			context.Background(),
			observations(),
			now,
		); err != nil {
			t.Fatalf("advance mutation: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}

func verifiedMutationAttempt(
	t *testing.T,
	states *memoryOptimizerStore,
	state lib.MinerState,
	kind lib.MutationKind,
	target lib.OperatingPoint,
	now time.Time,
) int64 {
	t.Helper()
	if kind == lib.MutationMiningConfiguration {
		target = lib.OperatingPoint{}
	}
	attempt := lib.MutationAttempt{
		MacAddr:           state.MacAddr,
		Kind:              kind,
		FromFrequency:     state.CurrentFrequency,
		FromCoreVoltage:   state.CurrentCoreVoltage,
		TargetFrequency:   target.Frequency,
		TargetCoreVoltage: target.CoreVoltage,
		IntentCreatedAt:   now.Add(-time.Minute),
		StartedAt:         now.Add(-30 * time.Second),
	}
	id, err := states.StartMutationAttempt(&attempt)
	if err != nil {
		t.Fatalf("start verified mutation attempt: %v", err)
	}
	for _, milestone := range []lib.MutationMilestone{
		lib.MutationMilestonePatchRequested,
		lib.MutationMilestoneRestartRequested,
		lib.MutationMilestoneRebootVerified,
	} {
		if err := states.AdvanceMutationAttempt(id, milestone, now); err != nil {
			t.Fatalf("advance verified mutation attempt: %v", err)
		}
	}
	return id
}

func TestOperatingPointMutationPersistsBeforePatchAndRequiresVerifiedRestart(t *testing.T) {
	now := time.Now().Round(time.Second)
	info := healthyInfo()
	state := optimizerState(now)
	target := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}
	state.SetPendingMutation(lib.MutationOperatingPoint, target, now)
	state.Phase = lib.PhaseUndervolt
	states := newMemoryOptimizerStore()
	states.putState(state)

	persistedBeforePatch := false
	historyBeforePatch := false
	devices := &fakeDeviceAPI{
		getInfo: func(context.Context, string) (lib.Info, error) {
			return info, nil
		},
		asicSettings: gammaASIC(),
		patchHook: func(kind lib.MutationKind, point lib.OperatingPoint) {
			got := states.getState(state.MacAddr)
			attempts := states.mutationAttempts(state.MacAddr)
			persistedBeforePatch = kind == lib.MutationOperatingPoint &&
				got.PendingKind == kind &&
				got.PendingPoint() == point
			historyBeforePatch = len(attempts) == 1 &&
				attempts[0].Kind == kind &&
				attempts[0].TargetPoint() == point &&
				!attempts[0].PatchRequestedAt.IsZero() &&
				attempts[0].RestartRequestedAt.IsZero()
		},
	}
	post := info
	post.CoreVoltage = target.CoreVoltage
	post.UpTimeSeconds = 1
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(false),
		info,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return discovered(post, "192.0.2.44"), nil
		},
		nil,
		nil,
	)
	coordinator.now = func() time.Time { return now }

	observations := func() map[string]*minerObservation {
		return map[string]*minerObservation{
			state.MacAddr: observation(info, states.getState(state.MacAddr), mutationSettings(false)),
		}
	}
	waitForMutation(t, coordinator, observations, now, func() bool {
		return states.getState(state.MacAddr).PendingKind == ""
	})
	got := states.getState(state.MacAddr)
	if !persistedBeforePatch ||
		!historyBeforePatch ||
		got.CurrentPoint() != target ||
		got.IP != "192.0.2.44" ||
		!got.RampUntil.After(now) {
		t.Fatalf(
			"completed state = %+v, durable intent/history before patch = %v/%v",
			got,
			persistedBeforePatch,
			historyBeforePatch,
		)
	}
	if len(devices.operatingRequests()) != 1 ||
		len(devices.restartRequests()) != 1 {
		t.Fatalf(
			"device requests = %+v, restarts = %+v",
			devices.operatingRequests(),
			devices.restartRequests(),
		)
	}
	attempts := states.mutationAttempts(state.MacAddr)
	if len(attempts) != 1 ||
		attempts[0].Kind != lib.MutationOperatingPoint ||
		attempts[0].FromPoint() != operatingPointFromInfo(info) ||
		attempts[0].TargetPoint() != target ||
		attempts[0].PatchRequestedAt.IsZero() ||
		attempts[0].RestartRequestedAt.IsZero() ||
		attempts[0].RebootVerifiedAt.IsZero() ||
		attempts[0].CompletedAt.IsZero() ||
		!attempts[0].MiningResumedAt.IsZero() ||
		!attempts[0].FailedAt.IsZero() {
		t.Fatalf("completed mutation history = %+v", attempts)
	}

	for index, healthyAt := range []time.Time{
		now.Add(10 * time.Second),
		now.Add(20 * time.Second),
	} {
		if _, err := coordinator.Advance(
			context.Background(),
			map[string]*minerObservation{
				state.MacAddr: observation(
					post,
					states.getState(state.MacAddr),
					mutationSettings(false),
				),
			},
			healthyAt,
		); err != nil {
			t.Fatalf("advance healthy mining poll %d: %v", index+1, err)
		}
	}
	attempts = states.mutationAttempts(state.MacAddr)
	if !attempts[0].MiningResumedAt.Equal(now.Add(20 * time.Second)) {
		t.Fatalf("mining resume history = %+v", attempts[0])
	}
}

func TestMutationHistoryRecordsPatchFailureWithoutRestart(t *testing.T) {
	now := time.Now().Round(time.Second)
	info := healthyInfo()
	state := optimizerState(now)
	target := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}
	state.SetPendingMutation(lib.MutationOperatingPoint, target, now)
	state.Phase = lib.PhaseUndervolt
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return info, nil },
		asicSettings: gammaASIC(),
		setError:     errors.New("synthetic PATCH failure"),
	}
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(false),
		info,
		nil,
		nil,
		nil,
		nil,
	)
	coordinator.now = func() time.Time { return now }
	observations := func() map[string]*minerObservation {
		return map[string]*minerObservation{
			state.MacAddr: observation(
				info,
				states.getState(state.MacAddr),
				mutationSettings(false),
			),
		}
	}
	waitForMutation(t, coordinator, observations, now, func() bool {
		attempts := states.mutationAttempts(state.MacAddr)
		return len(attempts) == 1 && !attempts[0].FailedAt.IsZero()
	})
	attempts := states.mutationAttempts(state.MacAddr)
	if attempts[0].FailureStage != lib.MutationFailurePatch ||
		attempts[0].PatchRequestedAt.IsZero() ||
		!attempts[0].RestartRequestedAt.IsZero() ||
		!attempts[0].RebootVerifiedAt.IsZero() ||
		!attempts[0].CompletedAt.IsZero() ||
		len(devices.operatingRequests()) != 1 ||
		len(devices.restartRequests()) != 0 ||
		states.getState(state.MacAddr).PendingKind != lib.MutationOperatingPoint {
		t.Fatalf("PATCH failure history/state = %+v / %+v", attempts[0], states.getState(state.MacAddr))
	}
}

func TestSafetyMutationsRunThroughRestartAndCompleteWithResidualHeat(t *testing.T) {
	tests := []struct {
		name      string
		phase     lib.OptimizerPhase
		temp      float64
		wantPhase lib.OptimizerPhase
	}{
		{
			name:      "ordinary hard-limit rollback",
			phase:     lib.PhaseCooldown,
			temp:      optimizerSettings().TempLimit + 1,
			wantPhase: lib.PhaseCooldown,
		},
		{
			name:      "host cutoff containment",
			phase:     lib.PhaseOverheat,
			temp:      optimizerSettings().TempCutoff,
			wantPhase: lib.PhaseOverheat,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().Round(time.Second)
			target := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
			info := healthyInfo()
			info.Frequency = 490
			info.CoreVoltage = 1100
			info.Temp = test.temp
			state := optimizerState(now.Add(-time.Hour))
			state.SetCurrentPoint(operatingPointFromInfo(info))
			state.Phase = test.phase
			state.PhaseStartedAt = now.Add(-30 * time.Minute)
			state.SetPendingMutation(
				lib.MutationSafetyRollback,
				target,
				now.Add(-20*time.Minute),
			)
			states := newMemoryOptimizerStore()
			states.putState(state)
			devices := &fakeDeviceAPI{
				getInfo: func(context.Context, string) (lib.Info, error) {
					return info, nil
				},
				asicSettings: gammaASIC(),
			}
			post := info
			post.Frequency = target.Frequency
			post.CoreVoltage = target.CoreVoltage
			post.UpTimeSeconds = 1
			coordinator := testMutationCoordinator(
				devices,
				states,
				mutationSettings(false),
				info,
				nil,
				func(context.Context, string) (lib.DiscoveredMiner, error) {
					return discovered(post, "192.0.2.44"), nil
				},
				nil,
				nil,
			)
			coordinator.now = func() time.Time { return now }
			observations := func() map[string]*minerObservation {
				return map[string]*minerObservation{
					state.MacAddr: observation(
						info,
						states.getState(state.MacAddr),
						mutationSettings(false),
					),
				}
			}
			waitForMutation(t, coordinator, observations, now, func() bool {
				return states.getState(state.MacAddr).PendingKind == ""
			})
			got := states.getState(state.MacAddr)
			if got.CurrentPoint() != target ||
				got.Phase != test.wantPhase ||
				len(devices.operatingRequests()) != 1 ||
				devices.operatingRequests()[0].recovery ||
				len(devices.restartRequests()) != 1 {
				t.Fatalf(
					"completed state/requests/restarts = %+v/%+v/%+v",
					got,
					devices.operatingRequests(),
					devices.restartRequests(),
				)
			}
		})
	}
}

func TestFirmwareOverheatRecoveryUsesFlagClearingVerifiedLifecycle(t *testing.T) {
	now := time.Now().Round(time.Second)
	settings := mutationSettings(false)
	target := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	info := healthyInfo()
	info.Frequency = 50
	info.CoreVoltage = 1000
	info.OverHeatMode = 1
	info.Temp = settings.RecoveryTemp
	info.Power = settings.MaxPower - powerHeadroom
	info.VRTemp = settings.VRTempHigh * vrExplorationFactor
	state := optimizerState(now.Add(-time.Hour))
	originalBest := state.BestPoint()
	originalBestHash := state.BestHashRate
	state.Phase = lib.PhaseOverheat
	state.PhaseStartedAt = now.Add(-30 * time.Minute)
	state.SetPendingMutation(
		lib.MutationOverheatRecovery,
		target,
		now.Add(-20*time.Minute),
	)
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{
		getInfo: func(context.Context, string) (lib.Info, error) {
			return info, nil
		},
		asicSettings: gammaASIC(),
	}
	post := info
	post.Frequency = target.Frequency
	post.CoreVoltage = target.CoreVoltage
	post.OverHeatMode = 0
	post.UpTimeSeconds = 1
	coordinator := testMutationCoordinator(
		devices,
		states,
		settings,
		info,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return discovered(post, "192.0.2.44"), nil
		},
		nil,
		nil,
	)
	coordinator.now = func() time.Time { return now }
	observations := func() map[string]*minerObservation {
		return map[string]*minerObservation{
			state.MacAddr: observation(
				info,
				states.getState(state.MacAddr),
				settings,
			),
		}
	}
	waitForMutation(t, coordinator, observations, now, func() bool {
		return states.getState(state.MacAddr).PendingKind == ""
	})
	got := states.getState(state.MacAddr)
	requests := devices.operatingRequests()
	if got.CurrentPoint() != target ||
		got.Phase != lib.PhaseCooldown ||
		got.BestPoint() != originalBest ||
		got.BestHashRate != originalBestHash ||
		len(requests) != 1 ||
		!requests[0].recovery ||
		len(devices.restartRequests()) != 1 {
		t.Fatalf(
			"recovery state/requests/restarts = %+v/%+v/%+v",
			got,
			requests,
			devices.restartRequests(),
		)
	}
}

func TestMutationRetainsIntentWhenRebootIsNotProven(t *testing.T) {
	tests := []struct {
		name       string
		rediscover func(lib.Info, *time.Time) mutationDiscovery
	}{
		{
			name: "offline only",
			rediscover: func(_ lib.Info, clock *time.Time) mutationDiscovery {
				return func(context.Context, string) (lib.DiscoveredMiner, error) {
					*clock = clock.Add(2 * time.Second)
					return lib.DiscoveredMiner{}, errMinerNotFound
				}
			},
		},
		{
			name: "continuous uptime",
			rediscover: func(info lib.Info, clock *time.Time) mutationDiscovery {
				return func(context.Context, string) (lib.DiscoveredMiner, error) {
					*clock = clock.Add(2 * time.Second)
					info.UpTimeSeconds += 2
					info.CoreVoltage = 1060
					return discovered(info, "192.0.2.10"), nil
				}
			},
		},
		{
			name: "post-restart readback mismatch",
			rediscover: func(info lib.Info, clock *time.Time) mutationDiscovery {
				return func(context.Context, string) (lib.DiscoveredMiner, error) {
					*clock = clock.Add(2 * time.Second)
					info.UpTimeSeconds = 1
					return discovered(info, "192.0.2.10"), nil
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().Round(time.Second)
			clock := now
			info := healthyInfo()
			state := optimizerState(now)
			state.SetPendingMutation(
				lib.MutationOperatingPoint,
				lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060},
				now,
			)
			state.Phase = lib.PhaseUndervolt
			states := newMemoryOptimizerStore()
			states.putState(state)
			devices := &fakeDeviceAPI{
				getInfo: func(context.Context, string) (lib.Info, error) {
					return info, nil
				},
				asicSettings: gammaASIC(),
			}
			coordinator := testMutationCoordinator(
				devices,
				states,
				mutationSettings(false),
				info,
				nil,
				test.rediscover(info, &clock),
				nil,
				nil,
			)
			coordinator.now = func() time.Time { return clock }
			coordinator.rebootDeadline = time.Second

			waitForMutation(t, coordinator, func() map[string]*minerObservation {
				return map[string]*minerObservation{
					state.MacAddr: observation(
						info,
						states.getState(state.MacAddr),
						mutationSettings(false),
					),
				}
			}, now, func() bool {
				coordinator.mu.Lock()
				defer coordinator.mu.Unlock()
				return len(coordinator.active) == 0 &&
					len(devices.restartRequests()) == 1
			})
			if got := states.getState(state.MacAddr); got.PendingKind == "" {
				t.Fatalf("ambiguous restart cleared durable intent: %+v", got)
			}
		})
	}
}

func TestMutationWaitsForCompletePostRestartTelemetry(t *testing.T) {
	now := time.Now()
	info := healthyInfo()
	target := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	state := optimizerState(now)
	state.SetPendingMutation(lib.MutationOperatingPoint, target, now)
	state.Phase = lib.PhaseCooldown
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return info, nil },
		asicSettings: gammaASIC(),
	}
	post := info
	post.CoreVoltage = target.CoreVoltage
	post.UpTimeSeconds = 1
	incomplete := post
	incomplete.Temp = 0
	var rediscoveryCalls atomic.Int32
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(false),
		info,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			if rediscoveryCalls.Add(1) == 1 {
				return discovered(incomplete, state.IP), nil
			}
			return discovered(post, state.IP), nil
		},
		nil,
		nil,
	)
	coordinator.now = func() time.Time { return now }
	observations := func() map[string]*minerObservation {
		return map[string]*minerObservation{
			state.MacAddr: observation(
				info,
				states.getState(state.MacAddr),
				mutationSettings(false),
			),
		}
	}
	waitForMutation(t, coordinator, observations, now, func() bool {
		return states.getState(state.MacAddr).PendingKind == ""
	})
	if rediscoveryCalls.Load() < 2 ||
		len(devices.operatingRequests()) != 1 ||
		len(devices.restartRequests()) != 1 {
		t.Fatalf(
			"rediscovery/PATCH/restart = %d/%d/%d, want at least 2/1/1",
			rediscoveryCalls.Load(),
			len(devices.operatingRequests()),
			len(devices.restartRequests()),
		)
	}
}

func TestWrongMACPreflightNeverReachesPatch(t *testing.T) {
	now := time.Now()
	info := healthyInfo()
	state := optimizerState(now)
	state.SetPendingMutation(
		lib.MutationOperatingPoint,
		lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060},
		now,
	)
	state.Phase = lib.PhaseUndervolt
	states := newMemoryOptimizerStore()
	states.putState(state)
	wrong := info
	wrong.MacAddr = "00:11:22:33:44:55"
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return wrong, nil },
		asicSettings: gammaASIC(),
	}
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(false),
		info,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		nil,
		nil,
	)
	observations := func() map[string]*minerObservation {
		return map[string]*minerObservation{
			state.MacAddr: observation(
				info,
				states.getState(state.MacAddr),
				mutationSettings(false),
			),
		}
	}
	if _, err := coordinator.Advance(context.Background(), observations(), now); err != nil {
		t.Fatalf("start mutation: %v", err)
	}
	waitForMutation(t, coordinator, observations, now, func() bool {
		coordinator.mu.Lock()
		defer coordinator.mu.Unlock()
		return len(coordinator.active) == 0
	})
	if len(devices.operatingRequests()) != 0 ||
		len(devices.restartRequests()) != 0 ||
		states.getState(state.MacAddr).PendingKind == "" {
		t.Fatal("wrong-MAC preflight touched the device or cleared intent")
	}
}

func TestNamedHostnameChangeBlocksNormalMutation(t *testing.T) {
	now := time.Now()
	info := healthyInfo()
	state := optimizerState(now)
	state.SetPendingMutation(
		lib.MutationOperatingPoint,
		lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060},
		now,
	)
	state.Phase = lib.PhaseUndervolt
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return info, nil },
		asicSettings: gammaASIC(),
	}
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(false),
		info,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		nil,
		nil,
	)
	coordinator.RequireHostnames(map[string]string{state.MacAddr: state.Hostname})
	renamed := info
	renamed.Hostname = "unexpected-name"
	if _, err := coordinator.Advance(
		context.Background(),
		map[string]*minerObservation{
			state.MacAddr: observation(
				renamed,
				state,
				mutationSettings(false),
			),
		},
		now,
	); err != nil {
		t.Fatalf("advance renamed miner: %v", err)
	}
	if len(devices.operatingRequests()) != 0 {
		t.Fatal("normal mutation started after selected hostname changed")
	}
	coordinator.mu.Lock()
	blocked := coordinator.startupBlocked
	coordinator.mu.Unlock()
	if blocked != state.Hostname {
		t.Fatalf("blocked hostname = %q, want %q", blocked, state.Hostname)
	}
}

func TestOfflinePendingMutationBlocksOtherNormalWork(t *testing.T) {
	tests := []struct {
		name          string
		phase         lib.OptimizerPhase
		kind          lib.MutationKind
		miningPending bool
	}{
		{
			name:  "offline safety",
			phase: lib.PhaseCooldown,
			kind:  lib.MutationSafetyRollback,
		},
		{
			name:  "offline overheat recovery",
			phase: lib.PhaseOverheat,
			kind:  lib.MutationOverheatRecovery,
		},
		{
			name:  "offline normal",
			phase: lib.PhaseUndervolt,
			kind:  lib.MutationOperatingPoint,
		},
		{
			name:          "offline mining",
			phase:         lib.PhaseBaseline,
			miningPending: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now()
			firstInfo := healthyInfo()
			firstInfo.MacAddr = "00:00:00:00:00:01"
			firstInfo.Hostname = "miner-one"
			secondInfo := healthyInfo()
			secondInfo.MacAddr = "00:00:00:00:00:02"
			secondInfo.Hostname = "miner-two"
			firstState := optimizerState(now)
			firstState.MacAddr = firstInfo.MacAddr
			firstState.Hostname = firstInfo.Hostname
			if test.miningPending {
				firstState.MiningPending = true
			} else {
				firstState.SetPendingMutation(
					test.kind,
					lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060},
					now,
				)
			}
			firstState.Phase = test.phase
			secondState := optimizerState(now)
			secondState.MacAddr = secondInfo.MacAddr
			secondState.Hostname = secondInfo.Hostname
			secondState.IP = "192.0.2.11"
			secondState.SetPendingMutation(
				lib.MutationOperatingPoint,
				lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060},
				now,
			)
			secondState.Phase = lib.PhaseUndervolt
			states := newMemoryOptimizerStore()
			states.putState(firstState)
			states.putState(secondState)
			devices := &fakeDeviceAPI{
				getInfo:      func(context.Context, string) (lib.Info, error) { return secondInfo, nil },
				asicSettings: gammaASIC(),
			}
			coordinator := newMutationCoordinator(
				devices,
				states,
				lib.SettingsFile{Defaults: mutationSettings(false)},
				[]lib.DiscoveredMiner{
					discovered(firstInfo, firstState.IP),
					discovered(secondInfo, secondState.IP),
				},
				nil,
				func(context.Context, string) (lib.DiscoveredMiner, error) {
					return lib.DiscoveredMiner{}, errMinerNotFound
				},
				nil,
				nil,
				func(string) {},
			)
			coordinator.gateOpen = true
			if _, err := coordinator.Advance(
				context.Background(),
				map[string]*minerObservation{
					secondState.MacAddr: observation(
						secondInfo,
						secondState,
						mutationSettings(false),
					),
				},
				now,
			); err != nil {
				t.Fatalf("advance coordinator: %v", err)
			}
			coordinator.mu.Lock()
			active := len(coordinator.active)
			coordinator.mu.Unlock()
			if active != 0 || len(devices.operatingRequests()) != 0 {
				t.Fatal("normal mutation started while earlier durable work was offline")
			}
		})
	}
}

func TestOperatingPointIntentIsRecheckedAfterASICRead(t *testing.T) {
	now := time.Now()
	info := healthyInfo()
	state := optimizerState(now)
	original := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}
	replacement := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	state.SetPendingMutation(lib.MutationOperatingPoint, original, now)
	state.Phase = lib.PhaseUndervolt
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return info, nil },
		asicSettings: gammaASIC(),
		asicHook: func() {
			updated := states.getState(state.MacAddr)
			updated.SetPendingMutation(lib.MutationOperatingPoint, replacement, now)
			states.putState(updated)
		},
	}
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(false),
		info,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		nil,
		nil,
	)
	observations := func() map[string]*minerObservation {
		return map[string]*minerObservation{
			state.MacAddr: observation(
				info,
				states.getState(state.MacAddr),
				mutationSettings(false),
			),
		}
	}
	if _, err := coordinator.Advance(context.Background(), observations(), now); err != nil {
		t.Fatalf("start mutation: %v", err)
	}
	waitForMutation(t, coordinator, observations, now, func() bool {
		coordinator.mu.Lock()
		defer coordinator.mu.Unlock()
		return len(coordinator.active) == 0
	})
	if len(devices.operatingRequests()) != 0 ||
		states.getState(state.MacAddr).PendingPoint() != replacement {
		t.Fatal("superseded operating-point intent reached PATCH")
	}
}

func TestMatchingDefaultMiningConfigurationDoesNotResolveSecretsOrMutate(t *testing.T) {
	now := time.Now()
	info := matchingMiningInfo()
	state := optimizerState(now)
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return info, nil },
		asicSettings: gammaASIC(),
	}
	var passwordReads atomic.Int32
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(true),
		info,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		func(lib.MiningSettings) (string, string, error) {
			passwordReads.Add(1)
			return "must-not-be-read", "must-not-be-read", nil
		},
		nil,
	)
	coordinator.now = func() time.Time { return now }
	observations := map[string]*minerObservation{
		state.MacAddr: observation(info, state, mutationSettings(true)),
	}

	if _, err := coordinator.Advance(context.Background(), observations, now); err != nil {
		t.Fatalf("first startup health poll: %v", err)
	}
	if coordinator.GateOpen() {
		t.Fatal("startup gate opened after only one health poll")
	}
	if _, err := coordinator.Advance(context.Background(), observations, now); err != nil {
		t.Fatalf("second startup health poll: %v", err)
	}
	if _, err := coordinator.Advance(context.Background(), observations, now); err != nil {
		t.Fatalf("open startup gate: %v", err)
	}
	if !coordinator.GateOpen() ||
		passwordReads.Load() != 0 ||
		len(devices.miningRequests()) != 0 ||
		len(devices.restartRequests()) != 0 {
		t.Fatalf(
			"gate/passwords/mining/restarts = %v/%d/%d/%d",
			coordinator.GateOpen(),
			passwordReads.Load(),
			len(devices.miningRequests()),
			len(devices.restartRequests()),
		)
	}
}

func TestPasswordFileFailureBlocksMiningMutationButNotSafetyPolling(t *testing.T) {
	now := time.Now()
	info := matchingMiningInfo()
	info.StratumUser = "old-worker"
	state := optimizerState(now)
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return info, nil },
		asicSettings: gammaASIC(),
	}
	var passwordReads atomic.Int32
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(true),
		info,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		func(lib.MiningSettings) (string, string, error) {
			passwordReads.Add(1)
			return "", "", errors.New("password file is unavailable")
		},
		nil,
	)
	observations := map[string]*minerObservation{
		state.MacAddr: observation(info, state, mutationSettings(true)),
	}

	if _, err := coordinator.Advance(context.Background(), observations, now); err != nil {
		t.Fatalf("Advance returned a password-file error: %v", err)
	}
	if passwordReads.Load() != 1 ||
		len(devices.miningRequests()) != 0 ||
		len(devices.restartRequests()) != 0 {
		t.Fatalf(
			"passwords/mining/restarts = %d/%d/%d",
			passwordReads.Load(),
			len(devices.miningRequests()),
			len(devices.restartRequests()),
		)
	}
	if !states.getState(state.MacAddr).MiningPending {
		t.Fatal("mining obligation was not preserved after password-file failure")
	}
}

func TestNamedReapplyMutatesMatchingEnabledMiningConfiguration(t *testing.T) {
	now := time.Now()
	info := matchingMiningInfo()
	state := optimizerState(now)
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return info, nil },
		asicSettings: gammaASIC(),
		restartError: errors.New("synthetic restart ambiguity"),
	}
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(true),
		info,
		map[string]bool{info.Hostname: true},
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		fixedMiningPasswords("synthetic-password"),
		nil,
	)
	waitForMutation(t, coordinator, func() map[string]*minerObservation {
		return map[string]*minerObservation{
			state.MacAddr: observation(
				info,
				states.getState(state.MacAddr),
				mutationSettings(true),
			),
		}
	}, now, func() bool {
		return len(devices.miningRequests()) == 1
	})
	if !states.getState(state.MacAddr).MiningPending {
		t.Fatal("named reapply did not create a durable mining obligation")
	}
}

func TestExistingMiningObligationConsumesReapplyWithoutDuplicateMutation(t *testing.T) {
	now := time.Now()
	pre := matchingMiningInfo()
	post := pre
	post.UpTimeSeconds = 1
	state := optimizerState(now)
	state.MiningPending = true
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return pre, nil },
		asicSettings: gammaASIC(),
	}
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(true),
		pre,
		map[string]bool{pre.Hostname: true},
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return discovered(post, state.IP), nil
		},
		fixedMiningPasswords("synthetic-password"),
		nil,
	)
	coordinator.now = func() time.Time { return now }
	observations := func() map[string]*minerObservation {
		return map[string]*minerObservation{
			state.MacAddr: observation(
				pre,
				states.getState(state.MacAddr),
				mutationSettings(true),
			),
		}
	}
	waitForMutation(t, coordinator, observations, now, func() bool {
		return !states.getState(state.MacAddr).MiningPending
	})
	for range 3 {
		if _, err := coordinator.Advance(context.Background(), observations(), now); err != nil {
			t.Fatalf("advance completed reapply: %v", err)
		}
	}
	if got := len(devices.miningRequests()); got != 1 {
		t.Fatalf("mining mutation count = %d, want 1", got)
	}
}

func TestDisabledMiningCannotAbandonDurableObligation(t *testing.T) {
	now := time.Now()
	info := healthyInfo()
	state := optimizerState(now)
	state.MiningPending = true
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return info, nil },
		asicSettings: gammaASIC(),
	}
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(false),
		info,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		nil,
		nil,
	)
	if _, err := coordinator.Advance(
		context.Background(),
		map[string]*minerObservation{
			state.MacAddr: observation(
				info,
				state,
				mutationSettings(false),
			),
		},
		now,
	); err != nil {
		t.Fatalf("advance disabled mining obligation: %v", err)
	}
	coordinator.mu.Lock()
	blocked := coordinator.startupBlocked
	coordinator.mu.Unlock()
	if blocked != info.Hostname ||
		coordinator.GateOpen() ||
		!states.getState(state.MacAddr).MiningPending ||
		len(devices.miningRequests()) != 0 {
		t.Fatal("disabled mining abandoned or applied a durable obligation")
	}
}

func TestMiningDriftUsesCompleteRestartVerifiedFlowAndPrimaryHealth(t *testing.T) {
	now := time.Now().Round(time.Second)
	pre := matchingMiningInfo()
	pre.StratumUser = "old-worker"
	post := matchingMiningInfo()
	post.UpTimeSeconds = 1
	state := optimizerState(now)
	states := newMemoryOptimizerStore()
	states.putState(state)

	devices := &fakeDeviceAPI{
		getInfo: func(context.Context, string) (lib.Info, error) {
			return pre, nil
		},
		asicSettings: gammaASIC(),
	}
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(true),
		pre,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return discovered(post, "192.0.2.77"), nil
		},
		func(settings lib.MiningSettings) (string, string, error) {
			return "synthetic-" + settings.Primary.PasswordEnv,
				"synthetic-" + settings.Fallback.PasswordEnv,
				nil
		},
		nil,
	)
	coordinator.now = func() time.Time { return now }
	preObservations := func() map[string]*minerObservation {
		return map[string]*minerObservation{
			state.MacAddr: observation(pre, states.getState(state.MacAddr), mutationSettings(true)),
		}
	}
	waitForMutation(t, coordinator, preObservations, now, func() bool {
		return len(devices.miningRequests()) == 1
	})
	if !states.getState(state.MacAddr).MiningPending {
		t.Fatal("mining obligation was not durable before PATCH")
	}
	waitForMutation(t, coordinator, preObservations, now, func() bool {
		return !states.getState(state.MacAddr).MiningPending
	})
	requests := devices.miningRequests()
	if len(requests) != 1 ||
		requests[0].settings != mutationSettings(true).Mining ||
		requests[0].primaryPassword == "" ||
		requests[0].fallbackPassword == "" ||
		len(devices.restartRequests()) != 1 {
		t.Fatalf("mining requests = %+v, restarts = %+v", requests, devices.restartRequests())
	}

	healthyAt := now.Add(2 * time.Second)
	postState := states.getState(state.MacAddr)
	postObservations := map[string]*minerObservation{
		state.MacAddr: observation(post, postState, mutationSettings(true)),
	}
	if _, err := coordinator.Advance(context.Background(), postObservations, healthyAt); err != nil {
		t.Fatalf("first primary health poll: %v", err)
	}
	if coordinator.GateOpen() {
		t.Fatal("gate opened after one positive primary-health poll")
	}
	if _, err := coordinator.Advance(context.Background(), postObservations, healthyAt); err != nil {
		t.Fatalf("second primary health poll: %v", err)
	}
	if _, err := coordinator.Advance(context.Background(), postObservations, healthyAt); err != nil {
		t.Fatalf("open gate: %v", err)
	}
	if !coordinator.GateOpen() {
		t.Fatal("gate did not open after two safe positive primary-health polls")
	}
}

func TestMiningHealthRejectsFallbackAndZeroHash(t *testing.T) {
	settings := mutationSettings(true)
	info := matchingMiningInfo()
	info.HashRate = 0
	if startupHealthy(info, settings) {
		t.Fatal("zero hash rate passed startup health")
	}
	info.HashRate = 800
	info.IsUsingFallbackStratum = 1
	if startupHealthy(info, settings) {
		t.Fatal("fallback Stratum passed primary health")
	}
	info.IsUsingFallbackStratum = 0
	if !startupHealthy(info, settings) {
		t.Fatal("safe positive primary telemetry failed startup health")
	}
}

func TestStartupHealthFailureIsBounded(t *testing.T) {
	now := time.Now()
	info := matchingMiningInfo()
	info.HashRate = 0
	state := optimizerState(now)
	states := newMemoryOptimizerStore()
	states.putState(state)
	coordinator := testMutationCoordinator(
		&fakeDeviceAPI{},
		states,
		mutationSettings(true),
		info,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		nil,
		nil,
	)
	coordinator.healthDeadline = time.Second
	observations := map[string]*minerObservation{
		state.MacAddr: observation(info, state, mutationSettings(true)),
	}
	if _, err := coordinator.Advance(context.Background(), observations, now); err != nil {
		t.Fatalf("start health deadline: %v", err)
	}
	if _, err := coordinator.Advance(
		context.Background(),
		observations,
		now.Add(2*time.Second),
	); err != nil {
		t.Fatalf("expire health deadline: %v", err)
	}
	coordinator.mu.Lock()
	blocked := coordinator.startupBlocked
	coordinator.mu.Unlock()
	if blocked != info.Hostname || coordinator.GateOpen() {
		t.Fatalf("blocked hostname/gate = %q/%v", blocked, coordinator.GateOpen())
	}
}

func TestStartupHealthWaitsForManualPointAdoptionAndRamp(t *testing.T) {
	now := time.Now()
	info := healthyInfo()
	info.Frequency = 490
	info.CoreVoltage = 1000
	state := optimizerState(now)
	state.RampUntil = time.Time{}
	states := newMemoryOptimizerStore()
	states.putState(state)
	coordinator := testMutationCoordinator(
		&fakeDeviceAPI{},
		states,
		mutationSettings(false),
		info,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		nil,
		nil,
	)
	advance := func(observedState lib.MinerState, at time.Time) {
		t.Helper()
		if _, err := coordinator.Advance(
			context.Background(),
			map[string]*minerObservation{
				state.MacAddr: observation(
					info,
					observedState,
					mutationSettings(false),
				),
			},
			at,
		); err != nil {
			t.Fatalf("advance startup: %v", err)
		}
	}

	state.ObservedFrequency = info.Frequency
	state.ObservedCoreVoltage = info.CoreVoltage
	state.ObservedCount = 1
	advance(state, now)
	advance(state, now.Add(10*time.Second))
	if coordinator.startupHealth[state.MacAddr] != 0 ||
		coordinator.startupIndex != 0 {
		t.Fatal("startup health counted an unadopted manual operating point")
	}

	state.SetCurrentPoint(operatingPointFromInfo(info))
	state.ObservedFrequency = 0
	state.ObservedCoreVoltage = 0
	state.ObservedCount = 0
	state.RampUntil = now.Add(time.Minute)
	states.putState(state)
	advance(state, now.Add(20*time.Second))
	if coordinator.startupHealth[state.MacAddr] != 0 {
		t.Fatal("startup health counted during the manual-point ramp")
	}

	state.RampUntil = now.Add(30 * time.Second)
	states.putState(state)
	advance(state, now.Add(30*time.Second))
	advance(state, now.Add(40*time.Second))
	if coordinator.startupIndex != 1 || coordinator.GateOpen() {
		t.Fatal("two post-ramp health polls did not finish the selected miner")
	}
	advance(state, now.Add(50*time.Second))
	if !coordinator.GateOpen() {
		t.Fatal("startup gate did not open after adoption, ramp, and health")
	}
}

func TestFirstMinerMiningFailureStopsSecondAndRedactsSecrets(t *testing.T) {
	now := time.Now()
	first := matchingMiningInfo()
	first.MacAddr = "00:00:00:00:00:01"
	first.Hostname = "miner-one"
	first.StratumUser = "old-one"
	second := matchingMiningInfo()
	second.MacAddr = "00:00:00:00:00:02"
	second.Hostname = "miner-two"
	second.StratumUser = "old-two"
	firstState := optimizerState(now)
	firstState.MacAddr = first.MacAddr
	firstState.Hostname = first.Hostname
	secondState := optimizerState(now)
	secondState.MacAddr = second.MacAddr
	secondState.Hostname = second.Hostname
	secondState.IP = "192.0.2.11"
	states := newMemoryOptimizerStore()
	states.putState(firstState)
	states.putState(secondState)
	const secret = "synthetic-secret-sentinel"
	devices := &fakeDeviceAPI{
		getInfo: func(_ context.Context, ip string) (lib.Info, error) {
			if ip == secondState.IP {
				return second, nil
			}
			return first, nil
		},
		asicSettings: gammaASIC(),
		miningError:  errors.New("rejected " + secret),
	}
	var logs bytes.Buffer
	coordinator := newMutationCoordinator(
		devices,
		states,
		lib.SettingsFile{Defaults: mutationSettings(true)},
		[]lib.DiscoveredMiner{
			discovered(first, firstState.IP),
			discovered(second, secondState.IP),
		},
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		fixedMiningPasswords(secret),
		log.New(&logs, "", 0),
		func(string) {},
	)
	coordinator.rediscoveryDelay = 0
	observations := func() map[string]*minerObservation {
		return map[string]*minerObservation{
			first.MacAddr: observation(first, states.getState(first.MacAddr), mutationSettings(true)),
			second.MacAddr: observation(
				second,
				states.getState(second.MacAddr),
				mutationSettings(true),
			),
		}
	}
	waitForMutation(t, coordinator, observations, now, func() bool {
		coordinator.mu.Lock()
		defer coordinator.mu.Unlock()
		return coordinator.startupBlocked == first.Hostname
	})
	for range 3 {
		if _, err := coordinator.Advance(context.Background(), observations(), now); err != nil {
			t.Fatalf("advance blocked rollout: %v", err)
		}
	}
	requests := devices.miningRequests()
	if len(requests) != 1 || requests[0].ip != firstState.IP {
		t.Fatalf("mining rollout requests = %+v", requests)
	}
	if strings.Contains(logs.String(), secret) {
		t.Fatalf("secret escaped into logs: %s", logs.String())
	}
	if !states.getState(first.MacAddr).MiningPending {
		t.Fatal("failed first-miner obligation was cleared")
	}
}

func TestResolvedSecretsNeverEnterSQLiteOrErrors(t *testing.T) {
	const secret = "synthetic-sqlite-secret-sentinel"
	now := time.Now()
	info := matchingMiningInfo()
	info.StratumUser = "drifted-user"
	path := filepath.Join(t.TempDir(), "optimizer.db")
	store, err := lib.OpenOptimizerStore(path)
	if err != nil {
		t.Fatalf("open optimizer store: %v", err)
	}
	state, _, err := store.LoadOrCreate(info, "192.0.2.10", now)
	if err != nil {
		_ = store.Close()
		t.Fatalf("create state: %v", err)
	}
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return info, nil },
		asicSettings: gammaASIC(),
		miningError:  errors.New("device echoed " + secret),
	}
	var logs bytes.Buffer
	coordinator := testMutationCoordinator(
		devices,
		store,
		mutationSettings(true),
		info,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		fixedMiningPasswords(secret),
		log.New(&logs, "", 0),
	)
	waitForMutation(t, coordinator, func() map[string]*minerObservation {
		current, loadErr := store.LoadMiner(state.MacAddr)
		if loadErr != nil {
			t.Fatalf("load state: %v", loadErr)
		}
		return map[string]*minerObservation{
			state.MacAddr: observation(info, current, mutationSettings(true)),
		}
	}, now, func() bool {
		coordinator.mu.Lock()
		defer coordinator.mu.Unlock()
		return coordinator.startupBlocked == info.Hostname
	})
	if strings.Contains(logs.String(), secret) {
		t.Fatalf("secret escaped into mutation logs: %s", logs.String())
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close optimizer store: %v", err)
	}
	databaseBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read optimizer database: %v", err)
	}
	if bytes.Contains(databaseBytes, []byte(secret)) {
		t.Fatal("resolved secret was persisted in SQLite")
	}
}

func TestShortSecretRedactionDoesNotCorruptErrorText(t *testing.T) {
	err := redactMutationError(
		errors.New("post-restart verification deadline expired"),
		"x",
	)
	if err == nil ||
		err.Error() != "mutation failed after resolving secret-bearing configuration" {
		t.Fatalf("short-secret error = %v", err)
	}
}

func TestOverheatPreemptionPreservesMiningObligation(t *testing.T) {
	now := time.Now()
	state := optimizerState(now)
	state.MiningPending = true
	states := newMemoryOptimizerStore()
	states.putState(state)
	minerController := testController(&fakeDeviceAPI{}, states, nil)
	info := healthyInfo()
	info.OverHeatMode = 1
	info.Temp = 60

	handled, err := minerController.enforceMinerSafety(
		context.Background(),
		&state,
		info,
		gammaASIC(),
		optimizerSettings(),
		now,
	)
	if err != nil || !handled {
		t.Fatalf("enforce overheat safety = %v, %v", handled, err)
	}
	got := states.getState(state.MacAddr)
	if got.PendingKind != lib.MutationOverheatRecovery || !got.MiningPending {
		t.Fatalf("preempted durable state = %+v", got)
	}
}

func TestMutationKindSafetyAuthorization(t *testing.T) {
	settings := mutationSettings(false)
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	live := healthyInfo()
	live.Frequency = 490
	live.CoreVoltage = 1100
	hardLimit := live
	hardLimit.Temp = settings.TempLimit + 0.1
	tests := []struct {
		name    string
		info    lib.Info
		state   lib.MinerState
		wantErr bool
	}{
		{
			name: "ordinary point blocked above hard limit",
			info: hardLimit,
			state: func() lib.MinerState {
				state := optimizerState(time.Now())
				state.SetPendingMutation(
					lib.MutationOperatingPoint,
					minimum,
					time.Now(),
				)
				state.Phase = lib.PhaseUndervolt
				return state
			}(),
			wantErr: true,
		},
		{
			name: "mining blocked above hard limit",
			info: hardLimit,
			state: func() lib.MinerState {
				state := optimizerState(time.Now())
				state.MiningPending = true
				return state
			}(),
			wantErr: true,
		},
		{
			name: "rollback allowed above ordinary hard limit",
			info: hardLimit,
			state: func() lib.MinerState {
				state := optimizerState(time.Now())
				state.SetPendingMutation(
					lib.MutationSafetyRollback,
					minimum,
					time.Now(),
				)
				state.Phase = lib.PhaseCooldown
				return state
			}(),
		},
		{
			name: "rollback allowed at power limit",
			info: func() lib.Info {
				info := live
				info.Power = settings.MaxPower
				return info
			}(),
			state: func() lib.MinerState {
				state := optimizerState(time.Now())
				state.SetPendingMutation(
					lib.MutationSafetyRollback,
					minimum,
					time.Now(),
				)
				state.Phase = lib.PhaseCooldown
				return state
			}(),
		},
		{
			name: "rollback allowed at VR limit",
			info: func() lib.Info {
				info := live
				info.VRTemp = settings.VRTempHigh
				return info
			}(),
			state: func() lib.MinerState {
				state := optimizerState(time.Now())
				state.SetPendingMutation(
					lib.MutationSafetyRollback,
					minimum,
					time.Now(),
				)
				state.Phase = lib.PhaseCooldown
				return state
			}(),
		},
		{
			name: "rollback blocked at host cutoff",
			info: func() lib.Info {
				info := live
				info.Temp = settings.TempCutoff
				return info
			}(),
			state: func() lib.MinerState {
				state := optimizerState(time.Now())
				state.SetPendingMutation(
					lib.MutationSafetyRollback,
					minimum,
					time.Now(),
				)
				state.Phase = lib.PhaseCooldown
				return state
			}(),
			wantErr: true,
		},
		{
			name: "rollback blocked by firmware overheat",
			info: func() lib.Info {
				info := hardLimit
				info.OverHeatMode = 1
				return info
			}(),
			state: func() lib.MinerState {
				state := optimizerState(time.Now())
				state.SetPendingMutation(
					lib.MutationSafetyRollback,
					minimum,
					time.Now(),
				)
				state.Phase = lib.PhaseCooldown
				return state
			}(),
			wantErr: true,
		},
		{
			name: "host containment allowed at cutoff",
			info: func() lib.Info {
				info := live
				info.Temp = settings.TempCutoff
				return info
			}(),
			state: func() lib.MinerState {
				state := optimizerState(time.Now())
				state.SetPendingMutation(
					lib.MutationSafetyRollback,
					minimum,
					time.Now(),
				)
				state.Phase = lib.PhaseOverheat
				return state
			}(),
		},
		{
			name: "host containment rejects non-minimum target",
			info: func() lib.Info {
				info := live
				info.Temp = settings.TempCutoff
				return info
			}(),
			state: func() lib.MinerState {
				state := optimizerState(time.Now())
				state.SetPendingMutation(
					lib.MutationSafetyRollback,
					lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060},
					time.Now(),
				)
				state.Phase = lib.PhaseOverheat
				return state
			}(),
			wantErr: true,
		},
		{
			name: "host containment rejects already active minimum",
			info: func() lib.Info {
				info := live
				info.Frequency = minimum.Frequency
				info.CoreVoltage = minimum.CoreVoltage
				info.Temp = settings.TempCutoff
				return info
			}(),
			state: func() lib.MinerState {
				state := optimizerState(time.Now())
				state.SetPendingMutation(
					lib.MutationSafetyRollback,
					minimum,
					time.Now(),
				)
				state.Phase = lib.PhaseOverheat
				return state
			}(),
			wantErr: true,
		},
		{
			name: "containment blocked beyond firmware trip",
			info: func() lib.Info {
				info := live
				info.Temp = axeOSASICTripTemp + 0.1
				return info
			}(),
			state: func() lib.MinerState {
				state := optimizerState(time.Now())
				state.SetPendingMutation(
					lib.MutationSafetyRollback,
					minimum,
					time.Now(),
				)
				state.Phase = lib.PhaseOverheat
				return state
			}(),
			wantErr: true,
		},
		{
			name: "containment blocked by firmware overheat",
			info: func() lib.Info {
				info := live
				info.Temp = settings.TempCutoff
				info.OverHeatMode = 1
				return info
			}(),
			state: func() lib.MinerState {
				state := optimizerState(time.Now())
				state.SetPendingMutation(
					lib.MutationSafetyRollback,
					minimum,
					time.Now(),
				)
				state.Phase = lib.PhaseOverheat
				return state
			}(),
			wantErr: true,
		},
		{
			name: "firmware recovery allowed at every recovery boundary",
			info: func() lib.Info {
				info := live
				info.OverHeatMode = 1
				info.Temp = settings.RecoveryTemp
				info.Power = settings.MaxPower - powerHeadroom
				info.VRTemp = settings.VRTempHigh * vrExplorationFactor
				return info
			}(),
			state: func() lib.MinerState {
				state := optimizerState(time.Now())
				state.SetPendingMutation(
					lib.MutationOverheatRecovery,
					minimum,
					time.Now(),
				)
				state.Phase = lib.PhaseOverheat
				return state
			}(),
		},
		{
			name: "firmware recovery waits above a recovery boundary",
			info: func() lib.Info {
				info := live
				info.OverHeatMode = 1
				info.Temp = settings.RecoveryTemp + 0.1
				info.Power = settings.MaxPower - powerHeadroom
				info.VRTemp = settings.VRTempHigh * vrExplorationFactor
				return info
			}(),
			state: func() lib.MinerState {
				state := optimizerState(time.Now())
				state.SetPendingMutation(
					lib.MutationOverheatRecovery,
					minimum,
					time.Now(),
				)
				state.Phase = lib.PhaseOverheat
				return state
			}(),
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateKindSafety(test.info, settings, test.state, minimum)
			if (err != nil) != test.wantErr {
				t.Fatalf("authorization error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestSafetyRollbackPreflightRevalidatesHistoricalEvidence(t *testing.T) {
	now := time.Now()
	info := healthyInfo()
	info.Frequency = 600
	info.CoreVoltage = 1150
	target := lib.OperatingPoint{Frequency: 550, CoreVoltage: 1100}
	state := optimizerState(now)
	state.SetCurrentPoint(operatingPointFromInfo(info))
	state.SetPendingMutation(lib.MutationSafetyRollback, target, now)
	state.Phase = lib.PhaseCooldown
	states := newMemoryOptimizerStore()
	states.putState(state)
	record := lib.OperatingPointRecord{
		MacAddr:     state.MacAddr,
		Frequency:   target.Frequency,
		CoreVoltage: target.CoreVoltage,
		Status:      lib.PointValidated,
		MedianHash:  900,
		P95Temp:     62,
		P95VRTemp:   50,
		P95Power:    20,
	}
	states.putRecord(record)
	coordinator := testMutationCoordinator(
		&fakeDeviceAPI{},
		states,
		mutationSettings(false),
		info,
		nil,
		nil,
		nil,
		nil,
	)
	if err := coordinator.validateMutationPreflight(
		info,
		mutationSettings(false),
		state,
		gammaASIC(),
	); err != nil {
		t.Fatalf("valid rollback evidence was rejected: %v", err)
	}

	record.P95VRTemp = 0
	states.putRecord(record)
	if err := coordinator.validateMutationPreflight(
		info,
		mutationSettings(false),
		state,
		gammaASIC(),
	); err == nil {
		t.Fatal("rollback without complete VR evidence was authorized")
	}
	state.SetPendingMutation(
		lib.MutationSafetyRollback,
		lib.OperatingPoint{Frequency: 625, CoreVoltage: 1000},
		now,
	)
	crossPair := record
	crossPair.Frequency = 625
	crossPair.CoreVoltage = 1000
	crossPair.P95VRTemp = 50
	states.putRecord(crossPair)
	if err := coordinator.validateMutationPreflight(
		info,
		mutationSettings(false),
		state,
		gammaASIC(),
	); err == nil {
		t.Fatal("cross-pair rollback that raised frequency was authorized")
	}
}

func TestPreflightEmergencyDurablySupersedesWeakerAuthority(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	tests := []struct {
		name      string
		preflight func(lib.Info) lib.Info
		kind      lib.MutationKind
		phase     lib.OptimizerPhase
		target    lib.OperatingPoint
		wantPhase lib.OptimizerPhase
		wantKind  lib.MutationKind
	}{
		{
			name: "firmware flag replaces ordinary point with recovery",
			preflight: func(info lib.Info) lib.Info {
				info.OverHeatMode = 1
				info.Temp = optimizerSettings().RecoveryTemp
				return info
			},
			kind:      lib.MutationOperatingPoint,
			phase:     lib.PhaseUndervolt,
			target:    lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060},
			wantPhase: lib.PhaseOverheat,
			wantKind:  lib.MutationOverheatRecovery,
		},
		{
			name: "unlatched firmware trip replaces ordinary point with hold",
			preflight: func(info lib.Info) lib.Info {
				info.Temp = axeOSASICTripTemp + 0.1
				return info
			},
			kind:      lib.MutationOperatingPoint,
			phase:     lib.PhaseUndervolt,
			target:    lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060},
			wantPhase: lib.PhaseOverheat,
		},
		{
			name: "host cutoff retains rollback for fleet replacement",
			preflight: func(info lib.Info) lib.Info {
				info.Temp = optimizerSettings().TempCutoff
				return info
			},
			kind:      lib.MutationSafetyRollback,
			phase:     lib.PhaseCooldown,
			target:    minimum,
			wantPhase: lib.PhaseCooldown,
			wantKind:  lib.MutationSafetyRollback,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fleetInfo := healthyInfo()
			fleetInfo.Frequency = 490
			fleetInfo.CoreVoltage = 1100
			preflight := test.preflight(fleetInfo)
			state := optimizerState(now.Add(-time.Hour))
			state.SetCurrentPoint(operatingPointFromInfo(fleetInfo))
			state.SetPendingMutation(test.kind, test.target, now.Add(-time.Minute))
			state.Phase = test.phase
			states := newMemoryOptimizerStore()
			states.putState(state)
			devices := &fakeDeviceAPI{
				getInfo: func(context.Context, string) (lib.Info, error) {
					return preflight, nil
				},
				asicSettings: gammaASIC(),
			}
			coordinator := testMutationCoordinator(
				devices,
				states,
				mutationSettings(false),
				fleetInfo,
				nil,
				nil,
				nil,
				nil,
			)
			coordinator.now = func() time.Time { return now }
			result := coordinator.execute(context.Background(), mutationRequest{
				macAddr:  state.MacAddr,
				hostname: state.Hostname,
				ip:       state.IP,
				kind:     test.kind,
				point:    test.target,
				info:     fleetInfo,
				settings: mutationSettings(false),
			})
			if result.err == nil {
				t.Fatal("unsafe preflight unexpectedly reached a successful result")
			}
			got := states.getState(state.MacAddr)
			if got.Phase != test.wantPhase ||
				got.PendingKind != test.wantKind ||
				len(devices.operatingRequests()) != 0 ||
				len(devices.restartRequests()) != 0 {
				t.Fatalf(
					"superseded state/requests/restarts = %+v/%+v/%+v",
					got,
					devices.operatingRequests(),
					devices.restartRequests(),
				)
			}
			if test.wantKind == lib.MutationOverheatRecovery &&
				got.PendingPoint() != minimum {
				t.Fatalf("firmware recovery target = %+v", got.PendingPoint())
			}
		})
	}
}

func TestSafetyCompletionSeparatesReadbackFromResidualHeat(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	minimum := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000}
	info := healthyInfo()
	info.Frequency = minimum.Frequency
	info.CoreVoltage = minimum.CoreVoltage
	info.Temp = optimizerSettings().TempLimit + 1
	info.Power = optimizerSettings().MaxPower
	info.VRTemp = optimizerSettings().VRTempHigh
	request := mutationRequest{
		kind:     lib.MutationSafetyRollback,
		point:    minimum,
		info:     healthyInfo(),
		settings: mutationSettings(false),
	}
	if err := verifyMutationReadback(request, info); err != nil {
		t.Fatalf("exact safety readback rejected residual heat: %v", err)
	}

	tests := []struct {
		name      string
		phase     lib.OptimizerPhase
		kind      lib.MutationKind
		wantPhase lib.OptimizerPhase
	}{
		{
			name:      "ordinary rollback retains cooldown",
			phase:     lib.PhaseCooldown,
			kind:      lib.MutationSafetyRollback,
			wantPhase: lib.PhaseCooldown,
		},
		{
			name:      "host containment retains emergency",
			phase:     lib.PhaseOverheat,
			kind:      lib.MutationSafetyRollback,
			wantPhase: lib.PhaseOverheat,
		},
		{
			name:      "firmware recovery enters cooldown",
			phase:     lib.PhaseOverheat,
			kind:      lib.MutationOverheatRecovery,
			wantPhase: lib.PhaseCooldown,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := optimizerState(now.Add(-time.Hour))
			state.Phase = test.phase
			state.PhaseStartedAt = now.Add(-30 * time.Minute)
			state.SetCurrentPoint(lib.OperatingPoint{Frequency: 490, CoreVoltage: 1100})
			state.SetPendingMutation(test.kind, minimum, now.Add(-20*time.Minute))
			originalBest := state.BestPoint()
			originalBestHash := state.BestHashRate
			states := newMemoryOptimizerStore()
			states.putState(state)
			coordinator := testMutationCoordinator(
				&fakeDeviceAPI{},
				states,
				mutationSettings(false),
				healthyInfo(),
				nil,
				nil,
				nil,
				nil,
			)
			coordinator.now = func() time.Time { return now }
			attemptID := verifiedMutationAttempt(
				t,
				states,
				state,
				test.kind,
				minimum,
				now,
			)
			result := mutationResult{
				attemptID: attemptID,
				macAddr:   state.MacAddr,
				hostname:  state.Hostname,
				kind:      test.kind,
				point:     minimum,
				miner:     discovered(info, state.IP),
			}
			if err := coordinator.completeMutationLocked(result); err != nil {
				t.Fatalf("complete safety mutation: %v", err)
			}
			got := states.getState(state.MacAddr)
			if got.PendingKind != "" ||
				got.CurrentPoint() != minimum ||
				got.Phase != test.wantPhase {
				t.Fatalf("completed state = %+v", got)
			}
			if test.kind == lib.MutationSafetyRollback &&
				!got.PhaseStartedAt.Equal(state.PhaseStartedAt) {
				t.Fatalf("safety completion refreshed durable episode age: %+v", got)
			}
			if got.BestPoint() != originalBest ||
				got.BestHashRate != originalBestHash {
				t.Fatalf("safety completion deleted evaluated history: %+v", got)
			}
		})
	}
}

func TestMutationFreeEmergencyBlocksOptimizationAndNormalWork(t *testing.T) {
	now := time.Now()
	firstInfo := healthyInfo()
	firstInfo.MacAddr = "00:00:00:00:00:01"
	firstInfo.Hostname = "miner-one"
	secondInfo := healthyInfo()
	secondInfo.MacAddr = "00:00:00:00:00:02"
	secondInfo.Hostname = "miner-two"
	firstState := optimizerState(now)
	firstState.MacAddr = firstInfo.MacAddr
	firstState.Hostname = firstInfo.Hostname
	firstState.Phase = lib.PhaseOverheat
	firstState.ClearPendingMutation()
	secondState := optimizerState(now)
	secondState.MacAddr = secondInfo.MacAddr
	secondState.Hostname = secondInfo.Hostname
	secondState.IP = "192.0.2.11"
	secondState.SetPendingMutation(
		lib.MutationOperatingPoint,
		lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060},
		now,
	)
	secondState.Phase = lib.PhaseUndervolt
	states := newMemoryOptimizerStore()
	states.putState(firstState)
	states.putState(secondState)
	devices := &fakeDeviceAPI{}
	coordinator := newMutationCoordinator(
		devices,
		states,
		lib.SettingsFile{Defaults: mutationSettings(false)},
		[]lib.DiscoveredMiner{
			discovered(firstInfo, firstState.IP),
			discovered(secondInfo, secondState.IP),
		},
		nil,
		nil,
		nil,
		nil,
		func(string) {},
	)
	coordinator.gateOpen = true
	allowOptimization, err := coordinator.Advance(
		context.Background(),
		map[string]*minerObservation{
			secondState.MacAddr: observation(
				secondInfo,
				secondState,
				mutationSettings(false),
			),
		},
		now,
	)
	if err != nil {
		t.Fatalf("advance coordinator: %v", err)
	}
	if allowOptimization ||
		len(devices.operatingRequests()) != 0 {
		t.Fatalf(
			"emergency hold allowed optimization/work: %v/%d",
			allowOptimization,
			len(devices.operatingRequests()),
		)
	}
}

func TestSafetyIntentCancelsActiveMiningWithoutBlockingReplay(t *testing.T) {
	now := time.Now()
	info := matchingMiningInfo()
	info.StratumUser = "drifted-user"
	state := optimizerState(now)
	state.MiningPending = true
	states := newMemoryOptimizerStore()
	states.putState(state)
	preflightStarted := make(chan struct{})
	var informationReads atomic.Int32
	devices := &fakeDeviceAPI{
		getInfo: func(ctx context.Context, _ string) (lib.Info, error) {
			if informationReads.Add(1) == 1 {
				close(preflightStarted)
				<-ctx.Done()
				return lib.Info{}, ctx.Err()
			}
			return info, nil
		},
		asicSettings: gammaASIC(),
	}
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(true),
		info,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		fixedMiningPasswords("synthetic-password"),
		nil,
	)
	observations := func() map[string]*minerObservation {
		return map[string]*minerObservation{
			state.MacAddr: observation(
				info,
				states.getState(state.MacAddr),
				mutationSettings(true),
			),
		}
	}
	if _, err := coordinator.Advance(context.Background(), observations(), now); err != nil {
		t.Fatalf("start mining mutation: %v", err)
	}
	select {
	case <-preflightStarted:
	case <-time.After(time.Second):
		t.Fatal("mining preflight did not start")
	}
	safetyState := states.getState(state.MacAddr)
	safetyState.SetPendingMutation(
		lib.MutationSafetyRollback,
		lib.OperatingPoint{Frequency: 400, CoreVoltage: 1000},
		now,
	)
	safetyState.Phase = lib.PhaseCooldown
	if err := states.SaveMiner(&safetyState); err != nil {
		t.Fatalf("record safety intent: %v", err)
	}
	if _, err := coordinator.Advance(context.Background(), observations(), now); err != nil {
		t.Fatalf("preempt mining mutation: %v", err)
	}
	waitForMutation(t, coordinator, observations, now, func() bool {
		coordinator.mu.Lock()
		defer coordinator.mu.Unlock()
		return len(coordinator.active) == 0
	})
	got := states.getState(state.MacAddr)
	coordinator.mu.Lock()
	blocked := coordinator.startupBlocked
	coordinator.mu.Unlock()
	if blocked != "" ||
		!got.MiningPending ||
		got.PendingKind != lib.MutationSafetyRollback {
		t.Fatalf("preempted state/block = %+v/%q", got, blocked)
	}
}

func TestMutationWaitDoesNotBlockFleetSafetyPolling(t *testing.T) {
	now := time.Now()
	firstInfo := healthyInfo()
	firstInfo.MacAddr = "00:00:00:00:00:01"
	firstInfo.Hostname = "miner-one"
	secondInfo := healthyInfo()
	secondInfo.MacAddr = "00:00:00:00:00:02"
	secondInfo.Hostname = "miner-two"
	firstState := optimizerState(now)
	firstState.MacAddr = firstInfo.MacAddr
	firstState.Hostname = firstInfo.Hostname
	firstState.SetPendingMutation(
		lib.MutationOperatingPoint,
		lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060},
		now,
	)
	firstState.Phase = lib.PhaseUndervolt
	secondState := optimizerState(now)
	secondState.MacAddr = secondInfo.MacAddr
	secondState.Hostname = secondInfo.Hostname
	secondState.IP = "192.0.2.11"
	states := newMemoryOptimizerStore()
	states.putState(firstState)
	states.putState(secondState)

	var secondPolls atomic.Int32
	patchStarted := make(chan struct{})
	releasePatch := make(chan struct{})
	devices := &fakeDeviceAPI{
		getInfo: func(_ context.Context, ip string) (lib.Info, error) {
			if ip == secondState.IP {
				secondPolls.Add(1)
				return secondInfo, nil
			}
			return firstInfo, nil
		},
		asicSettings: gammaASIC(),
		patchHook: func(lib.MutationKind, lib.OperatingPoint) {
			close(patchStarted)
			<-releasePatch
		},
	}
	minerController := testController(devices, states, nil)
	miners := []lib.DiscoveredMiner{
		discovered(firstInfo, firstState.IP),
		discovered(secondInfo, secondState.IP),
	}
	minerController.mutations = newMutationCoordinator(
		devices,
		states,
		lib.SettingsFile{Defaults: mutationSettings(false)},
		miners,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		nil,
		nil,
		minerController.resetRuntime,
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	minerController.pollMiners(ctx, miners, now)
	select {
	case <-patchStarted:
	case <-time.After(time.Second):
		t.Fatal("mutation did not start")
	}

	returned := make(chan struct{})
	go func() {
		minerController.pollMiners(ctx, miners, now.Add(10*time.Second))
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("fleet poll blocked behind mutation PATCH")
	}
	if secondPolls.Load() < 2 {
		t.Fatalf("second miner polls = %d, want continued fleet polling", secondPolls.Load())
	}
	close(releasePatch)
}

func TestStartupGateSuppressesExplorationWhileSafetyPollingContinues(t *testing.T) {
	now := time.Now().Round(time.Second)
	info := matchingMiningInfo()
	info.IsUsingFallbackStratum = 1
	states := newMemoryOptimizerStore()
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return info, nil },
		asicSettings: gammaASIC(),
	}
	settings := mutationSettings(true)
	minerController := testController(devices, states, nil)
	minerController.settings = lib.SettingsFile{Defaults: settings}
	miner := discovered(info, "192.0.2.10")
	minerController.mutations = newMutationCoordinator(
		devices,
		states,
		minerController.settings,
		[]lib.DiscoveredMiner{miner},
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		nil,
		nil,
		minerController.resetRuntime,
	)
	for poll := range 10 {
		minerController.pollMiners(
			context.Background(),
			[]lib.DiscoveredMiner{miner},
			now.Add(time.Duration(poll)*10*time.Second),
		)
	}
	state := states.getState(info.MacAddr)
	if state.PendingKind != "" || minerController.mutations.GateOpen() {
		t.Fatalf("startup-gated optimizer state = %+v", state)
	}
	minerController.runtimeMu.Lock()
	sampleCount := 0
	if runtime := minerController.runtimes[info.MacAddr]; runtime != nil {
		sampleCount = len(runtime.samples)
	}
	minerController.runtimeMu.Unlock()
	if sampleCount != 0 {
		t.Fatalf("startup gate collected %d optimization samples", sampleCount)
	}
}

func TestSamePollSafetyArbitrationPreventsNormalCandidateCreation(t *testing.T) {
	now := time.Now().Round(time.Second)
	settings := mutationSettings(false)
	firstInfo := healthyInfo()
	firstInfo.MacAddr = "00:00:00:00:00:01"
	firstInfo.Hostname = "miner-one"
	firstInfo.Frequency = 490
	firstInfo.CoreVoltage = 1100
	firstInfo.Temp = settings.TempCutoff
	secondInfo := healthyInfo()
	secondInfo.MacAddr = "00:00:00:00:00:02"
	secondInfo.Hostname = "miner-two"
	firstState := optimizerState(now.Add(-time.Hour))
	firstState.MacAddr = firstInfo.MacAddr
	firstState.Hostname = firstInfo.Hostname
	firstState.IP = "192.0.2.10"
	firstState.SetCurrentPoint(operatingPointFromInfo(firstInfo))
	secondState := optimizerState(now.Add(-time.Hour))
	secondState.MacAddr = secondInfo.MacAddr
	secondState.Hostname = secondInfo.Hostname
	secondState.IP = "192.0.2.11"
	secondState.RampUntil = time.Time{}
	states := newMemoryOptimizerStore()
	states.putState(firstState)
	states.putState(secondState)
	devices := &fakeDeviceAPI{
		getInfo: func(_ context.Context, ip string) (lib.Info, error) {
			if ip == firstState.IP {
				return firstInfo, nil
			}
			return secondInfo, nil
		},
		asicSettings: gammaASIC(),
	}
	minerController := testController(devices, states, nil)
	minerController.settings = lib.SettingsFile{Defaults: settings}
	miners := []lib.DiscoveredMiner{
		discovered(firstInfo, firstState.IP),
		discovered(secondInfo, secondState.IP),
	}
	minerController.mutations = newMutationCoordinator(
		devices,
		states,
		minerController.settings,
		miners,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		nil,
		nil,
		minerController.resetRuntime,
	)
	minerController.mutations.gateOpen = true
	sample := telemetrySample{
		hashRate:     secondInfo.HashRate,
		expectedHash: secondInfo.ExpectedHashRate,
		temp:         secondInfo.Temp,
		vrTemp:       secondInfo.VRTemp,
		power:        secondInfo.Power,
	}
	minerController.runtimes[secondState.MacAddr] = &minerRuntime{
		samples: make(
			[]telemetrySample,
			targetSampleCount(settings)-1,
		),
	}
	for index := range minerController.runtimes[secondState.MacAddr].samples {
		minerController.runtimes[secondState.MacAddr].samples[index] = sample
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	minerController.pollMiners(ctx, miners, now)

	gotFirst := states.getState(firstState.MacAddr)
	gotSecond := states.getState(secondState.MacAddr)
	if gotFirst.Phase != lib.PhaseOverheat ||
		gotFirst.PendingKind != lib.MutationSafetyRollback {
		t.Fatalf("same-poll safety state = %+v", gotFirst)
	}
	if gotSecond.PendingKind != "" {
		t.Fatalf("normal candidate was queued behind safety work: %+v", gotSecond)
	}
	minerController.runtimeMu.Lock()
	sampleCount := len(minerController.runtimes[secondState.MacAddr].samples)
	minerController.runtimeMu.Unlock()
	if sampleCount != targetSampleCount(settings)-1 {
		t.Fatalf("normal optimizer consumed a sample behind safety work: %d", sampleCount)
	}
}

func TestDurableIntentSurvivesFailureAfterVerifiedDeviceRestart(t *testing.T) {
	now := time.Now()
	info := healthyInfo()
	target := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}
	state := optimizerState(now)
	state.SetPendingMutation(lib.MutationOperatingPoint, target, now)
	state.Phase = lib.PhaseUndervolt
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return info, nil },
		asicSettings: gammaASIC(),
	}
	post := info
	post.CoreVoltage = target.CoreVoltage
	post.UpTimeSeconds = 1
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(false),
		info,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			states.mu.Lock()
			states.saveErr = errors.New("synthetic durable completion failure")
			states.mu.Unlock()
			return discovered(post, "192.0.2.10"), nil
		},
		nil,
		nil,
	)
	coordinator.now = func() time.Time { return now }
	observations := func() map[string]*minerObservation {
		return map[string]*minerObservation{
			state.MacAddr: observation(
				info,
				states.getState(state.MacAddr),
				mutationSettings(false),
			),
		}
	}
	if _, err := coordinator.Advance(context.Background(), observations(), now); err != nil {
		t.Fatalf("start mutation: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var completionError error
	for completionError == nil && time.Now().Before(deadline) {
		_, completionError = coordinator.Advance(context.Background(), observations(), now)
		time.Sleep(time.Millisecond)
	}
	if completionError == nil ||
		!strings.Contains(completionError.Error(), "durable completion failure") {
		t.Fatalf("completion error = %v", completionError)
	}
	if got := states.getState(state.MacAddr); got.PendingKind == "" {
		t.Fatalf("post-restart persistence failure cleared intent: %+v", got)
	}
}

func TestMiningCompletionFailureDoesNotRetryInSameLaunch(t *testing.T) {
	now := time.Now()
	pre := matchingMiningInfo()
	pre.StratumUser = "drifted-worker"
	post := matchingMiningInfo()
	post.UpTimeSeconds = 1
	state := optimizerState(now)
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return pre, nil },
		asicSettings: gammaASIC(),
	}
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(true),
		pre,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			states.mu.Lock()
			states.saveErr = errors.New("synthetic mining completion failure")
			states.mu.Unlock()
			return discovered(post, state.IP), nil
		},
		fixedMiningPasswords("synthetic-password"),
		nil,
	)
	coordinator.now = func() time.Time { return now }
	observations := func() map[string]*minerObservation {
		return map[string]*minerObservation{
			state.MacAddr: observation(
				pre,
				states.getState(state.MacAddr),
				mutationSettings(true),
			),
		}
	}
	if _, err := coordinator.Advance(context.Background(), observations(), now); err != nil {
		t.Fatalf("start mining mutation: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var completionError error
	for completionError == nil && time.Now().Before(deadline) {
		_, completionError = coordinator.Advance(context.Background(), observations(), now)
		time.Sleep(time.Millisecond)
	}
	if completionError == nil ||
		!strings.Contains(completionError.Error(), "mining completion failure") {
		t.Fatalf("completion error = %v", completionError)
	}
	states.mu.Lock()
	states.saveErr = nil
	states.mu.Unlock()
	for range 3 {
		if _, err := coordinator.Advance(context.Background(), observations(), now); err != nil {
			t.Fatalf("advance blocked mining mutation: %v", err)
		}
	}
	coordinator.mu.Lock()
	blocked := coordinator.startupBlocked
	coordinator.mu.Unlock()
	if blocked != pre.Hostname ||
		!states.getState(state.MacAddr).MiningPending ||
		len(devices.miningRequests()) != 1 {
		t.Fatal("mining completion failure retried or abandoned durable intent")
	}
}

func TestRebootProofRequiresUptimeDiscontinuity(t *testing.T) {
	if !proveNewBoot(120, 2, 3*time.Second) {
		t.Fatal("clear uptime discontinuity was not accepted")
	}
	if proveNewBoot(120, 123, 3*time.Second) {
		t.Fatal("continuous uptime was accepted as a reboot")
	}
	if proveNewBoot(2, 1, time.Second) {
		t.Fatal("ambiguous low uptime was accepted as a reboot")
	}
}

func TestDurableCompletionPrecedesTelemetryReset(t *testing.T) {
	now := time.Now()
	info := healthyInfo()
	target := lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060}
	state := optimizerState(now)
	state.SetPendingMutation(lib.MutationOperatingPoint, target, now)
	states := newMemoryOptimizerStore()
	states.putState(state)
	resetSawCompletedState := false
	coordinator := newMutationCoordinator(
		&fakeDeviceAPI{},
		states,
		lib.SettingsFile{Defaults: mutationSettings(false)},
		[]lib.DiscoveredMiner{discovered(info, state.IP)},
		nil,
		nil,
		nil,
		nil,
		func(macAddr string) {
			got := states.getState(macAddr)
			resetSawCompletedState = got.PendingKind == "" &&
				got.CurrentPoint() == target &&
				!got.RampUntil.IsZero()
		},
	)
	post := info
	post.CoreVoltage = target.CoreVoltage
	post.UpTimeSeconds = 1
	coordinator.now = func() time.Time { return now }
	attemptID := verifiedMutationAttempt(
		t,
		states,
		state,
		lib.MutationOperatingPoint,
		target,
		now,
	)
	if err := coordinator.completeMutationLocked(mutationResult{
		attemptID: attemptID,
		macAddr:   state.MacAddr,
		hostname:  state.Hostname,
		kind:      lib.MutationOperatingPoint,
		point:     target,
		miner:     discovered(post, "192.0.2.88"),
	}); err != nil {
		t.Fatalf("complete mutation: %v", err)
	}
	if !resetSawCompletedState {
		t.Fatal("telemetry reset ran before durable completion")
	}
}
