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
	getenv func(string) (string, bool),
	logger *log.Logger,
) *mutationCoordinator {
	coordinator := newMutationCoordinator(
		devices,
		states,
		lib.SettingsFile{Defaults: settings},
		[]lib.DiscoveredMiner{discovered(info, "192.0.2.10")},
		reapply,
		discover,
		getenv,
		logger,
		func(string) {},
	)
	coordinator.rediscoveryDelay = 0
	return coordinator
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
		if err := coordinator.Advance(
			context.Background(),
			observations(),
			now,
		); err != nil {
			t.Fatalf("advance mutation: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
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
	devices := &fakeDeviceAPI{
		getInfo: func(context.Context, string) (lib.Info, error) {
			return info, nil
		},
		asicSettings: gammaASIC(),
		patchHook: func(kind lib.MutationKind, point lib.OperatingPoint) {
			got := states.getState(state.MacAddr)
			persistedBeforePatch = kind == lib.MutationOperatingPoint &&
				got.PendingKind == kind &&
				got.PendingPoint() == point
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
		got.CurrentPoint() != target ||
		got.IP != "192.0.2.44" ||
		!got.RampUntil.After(now) {
		t.Fatalf("completed state = %+v, persisted before patch = %v", got, persistedBeforePatch)
	}
	if len(devices.operatingRequests()) != 1 ||
		len(devices.restartRequests()) != 1 {
		t.Fatalf(
			"device requests = %+v, restarts = %+v",
			devices.operatingRequests(),
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
	if err := coordinator.Advance(context.Background(), observations(), now); err != nil {
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
	if err := coordinator.Advance(
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
		miningPending bool
	}{
		{name: "offline safety", phase: lib.PhaseCooldown},
		{name: "offline normal", phase: lib.PhaseUndervolt},
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
					lib.MutationOperatingPoint,
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
			if err := coordinator.Advance(
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
	if err := coordinator.Advance(context.Background(), observations(), now); err != nil {
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

func TestMatchingMiningConfigurationDoesNotResolveSecretsOrMutate(t *testing.T) {
	now := time.Now()
	info := matchingMiningInfo()
	state := optimizerState(now)
	states := newMemoryOptimizerStore()
	states.putState(state)
	devices := &fakeDeviceAPI{
		getInfo:      func(context.Context, string) (lib.Info, error) { return info, nil },
		asicSettings: gammaASIC(),
	}
	var environmentReads atomic.Int32
	coordinator := testMutationCoordinator(
		devices,
		states,
		mutationSettings(true),
		info,
		nil,
		func(context.Context, string) (lib.DiscoveredMiner, error) {
			return lib.DiscoveredMiner{}, errMinerNotFound
		},
		func(string) (string, bool) {
			environmentReads.Add(1)
			return "must-not-be-read", true
		},
		nil,
	)
	coordinator.now = func() time.Time { return now }
	observations := map[string]*minerObservation{
		state.MacAddr: observation(info, state, mutationSettings(true)),
	}

	if err := coordinator.Advance(context.Background(), observations, now); err != nil {
		t.Fatalf("first startup health poll: %v", err)
	}
	if coordinator.GateOpen() {
		t.Fatal("startup gate opened after only one health poll")
	}
	if err := coordinator.Advance(context.Background(), observations, now); err != nil {
		t.Fatalf("second startup health poll: %v", err)
	}
	if err := coordinator.Advance(context.Background(), observations, now); err != nil {
		t.Fatalf("open startup gate: %v", err)
	}
	if !coordinator.GateOpen() ||
		environmentReads.Load() != 0 ||
		len(devices.miningRequests()) != 0 ||
		len(devices.restartRequests()) != 0 {
		t.Fatalf(
			"gate/env/mining/restarts = %v/%d/%d/%d",
			coordinator.GateOpen(),
			environmentReads.Load(),
			len(devices.miningRequests()),
			len(devices.restartRequests()),
		)
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
		func(string) (string, bool) { return "synthetic-password", true },
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
		func(string) (string, bool) { return "synthetic-password", true },
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
		if err := coordinator.Advance(context.Background(), observations(), now); err != nil {
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
	if err := coordinator.Advance(
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
		func(name string) (string, bool) {
			return "synthetic-" + name, true
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
	if err := coordinator.Advance(context.Background(), postObservations, healthyAt); err != nil {
		t.Fatalf("first primary health poll: %v", err)
	}
	if coordinator.GateOpen() {
		t.Fatal("gate opened after one positive primary-health poll")
	}
	if err := coordinator.Advance(context.Background(), postObservations, healthyAt); err != nil {
		t.Fatalf("second primary health poll: %v", err)
	}
	if err := coordinator.Advance(context.Background(), postObservations, healthyAt); err != nil {
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
	if err := coordinator.Advance(context.Background(), observations, now); err != nil {
		t.Fatalf("start health deadline: %v", err)
	}
	if err := coordinator.Advance(
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
		if err := coordinator.Advance(
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
		func(string) (string, bool) { return secret, true },
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
		if err := coordinator.Advance(context.Background(), observations(), now); err != nil {
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
		func(string) (string, bool) { return secret, true },
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

	if err := minerController.handleOverheat(
		context.Background(),
		&state,
		info,
		gammaASIC(),
		optimizerSettings(),
		now,
	); err != nil {
		t.Fatalf("handle overheat: %v", err)
	}
	got := states.getState(state.MacAddr)
	if got.PendingKind != lib.MutationOverheatRecovery || !got.MiningPending {
		t.Fatalf("preempted durable state = %+v", got)
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
		func(string) (string, bool) { return "synthetic-password", true },
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
	if err := coordinator.Advance(context.Background(), observations(), now); err != nil {
		t.Fatalf("start mining mutation: %v", err)
	}
	select {
	case <-preflightStarted:
	case <-time.After(time.Second):
		t.Fatal("mining preflight did not start")
	}
	safetyState := states.getState(state.MacAddr)
	safetyState.SetPendingMutation(
		lib.MutationOperatingPoint,
		lib.OperatingPoint{Frequency: 400, CoreVoltage: 1060},
		now,
	)
	safetyState.Phase = lib.PhaseCooldown
	if err := states.SaveMiner(&safetyState); err != nil {
		t.Fatalf("record safety intent: %v", err)
	}
	if err := coordinator.Advance(context.Background(), observations(), now); err != nil {
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
		got.PendingKind != lib.MutationOperatingPoint {
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
	if err := coordinator.Advance(context.Background(), observations(), now); err != nil {
		t.Fatalf("start mutation: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var completionError error
	for completionError == nil && time.Now().Before(deadline) {
		completionError = coordinator.Advance(context.Background(), observations(), now)
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
		func(string) (string, bool) { return "synthetic-password", true },
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
	if err := coordinator.Advance(context.Background(), observations(), now); err != nil {
		t.Fatalf("start mining mutation: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var completionError error
	for completionError == nil && time.Now().Before(deadline) {
		completionError = coordinator.Advance(context.Background(), observations(), now)
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
		if err := coordinator.Advance(context.Background(), observations(), now); err != nil {
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
	if err := coordinator.completeMutationLocked(mutationResult{
		macAddr:  state.MacAddr,
		hostname: state.Hostname,
		kind:     lib.MutationOperatingPoint,
		point:    target,
		miner:    discovered(post, "192.0.2.88"),
	}); err != nil {
		t.Fatalf("complete mutation: %v", err)
	}
	if !resetSawCompletedState {
		t.Fatal("telemetry reset ran before durable completion")
	}
}
