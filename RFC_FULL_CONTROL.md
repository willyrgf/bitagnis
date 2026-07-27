# RFC: Full Bitaxe Mining Configuration Control

- Status: Proposed
- Date: 2026-07-27
- Scope: Bitagnis and AxeOS-managed Bitaxe devices

## Summary

Bitagnis should manage Bitaxe mining configuration in addition to optimizing
temperature, frequency, and core voltage.

The first supported configuration model will match the two currently deployed
devices: one primary Stratum pool and one fallback Stratum pool. Bitagnis will
read the live non-secret configuration, compare it with a desired configuration
from `settings.yaml`, apply changes through AxeOS, restart one miner at a time,
verify recovery, and then resume thermal optimization.

No additional hardware or service is required. The implementation can use the
existing AxeOS HTTP API, discovery loop, hostname overrides, and SQLite state
store.

## Context

The devices observed while preparing this RFC were:

| Hostname | ASIC | Board | AxeOS |
| --- | --- | --- | --- |
| `mineira` | BM1370 | 601 | v2.8.1 |
| `mineiro` | BM1370 | 601 | v2.8.1 |

Both advertise primary and fallback Stratum configuration through
`GET /api/system/info`. Passwords are write-only and are not returned.

AxeOS v2.8.1 accepts the following mining fields through
`PATCH /api/system`:

- `stratumURL`
- `stratumPort`
- `stratumUser`
- `stratumPassword`
- `fallbackStratumURL`
- `fallbackStratumPort`
- `fallbackStratumUser`
- `fallbackStratumPassword`

The contract is documented in the
[AxeOS v2.8.1 OpenAPI schema][axeos-v281-openapi].

## Critical prerequisite: settings require a restart

On AxeOS v2.8.1, `PATCH /api/system` persists configuration in NVS but does not
restart the device or reload the running mining process. The AxeOS web
interface separately asks the user to restart after saving.

This applies to pool configuration and to the frequency/core-voltage settings
that Bitagnis already writes. The current
[`BitaxeClient.SetOperatingPoint`](lib/bitaxe.go) method stops after the PATCH.
Therefore, Bitagnis can currently observe a persisted value as though it were
active before the ASIC has actually loaded it.

Before mining configuration is added, Bitagnis MUST introduce a shared device
mutation operation:

```text
validate desired change
    -> PATCH /api/system
    -> POST /api/system/restart
    -> wait for the device to leave and return
    -> verify boot and configuration
    -> reset the optimizer telemetry window
    -> resume optimization after ramp-up
```

The v2.8.1 behavior is visible in the
[firmware PATCH and restart handlers][axeos-v281-handler].

## Goals

1. Manage primary and fallback mining pool settings for the existing AxeOS
   v2.8.1 Bitaxes.
2. Provide read-only status and plan commands before any mutation.
3. Apply configuration to miners one at a time.
4. Verify both configuration readback and mining recovery.
5. Keep passwords out of YAML, logs, terminal output, and SQLite.
6. Prevent mining configuration changes from racing thermal-control changes.
7. Preserve validated operating-point history across configuration restarts.
8. Detect manual configuration drift and apply an explicit policy.
9. Create a version-aware foundation for newer AxeOS mining capabilities.

## Non-goals

The initial implementation will not:

- choose a pool based on profitability or market data;
- manage Wi-Fi credentials, firmware, displays, or unrelated AxeOS settings;
- require a web dashboard or a new daemon;
- store recoverable Stratum passwords in SQLite;
- depend on accepted shares appearing within a short fixed period;
- configure newer multi-pool, Stratum V2, or TLS fields on v2.8.1;
- silently apply options that the detected firmware cannot verify.

## Terminology

- **Desired configuration**: mining settings loaded from `settings.yaml` and
  referenced environment variables.
- **Observed configuration**: non-secret settings returned by AxeOS.
- **Managed configuration**: settings that Bitagnis is authorized to reconcile.
- **Drift**: a difference between desired and observed non-secret settings, or a
  changed desired secret revision.
- **Device mutation**: any AxeOS setting change that may require a restart,
  including an operating-point or mining-configuration change.
- **Healthy recovery**: the device has rebooted, reports the desired settings,
  and resumes plausible hashing without a safety fault.

## Proposed configuration

Mining configuration will be nested under the existing defaults and hostname
overrides:

```yaml
defaults:
  recoveryTemp: 61
  targetTemp: 65
  tempLimit: 66
  tempCutoff: 70
  maxPower: 24
  vrTempHigh: 97
  maxErrorPercentage: 5
  metricsInterval: 10
  rampUpSeconds: 60
  evaluationWindowMinutes: 5
  overheatCooldownMinutes: 120

  mining:
    mode: observe
    restartTimeoutSeconds: 180
    healthyHashTimeoutSeconds: 300
    primary:
      url: pool.example.net
      port: 3333
      user: worker-name
      passwordEnv: BITAGNIS_PRIMARY_STRATUM_PASSWORD
    fallback:
      url: fallback.example.net
      port: 3333
      user: worker-name
      passwordEnv: BITAGNIS_FALLBACK_STRATUM_PASSWORD

overrides:
  mineira:
    mining:
      primary:
        user: worker-mineira
      fallback:
        user: worker-mineira

  mineiro:
    mining:
      primary:
        user: worker-mineiro
      fallback:
        user: worker-mineiro
```

### Management modes

- `disabled`: do not read, compare, or mutate mining configuration.
- `observe`: report status and drift; mutate only through an explicit apply
  command. This SHOULD be the default.
- `managed`: reconcile drift automatically when a miner is discovered and safe
  to restart.

An omitted `passwordEnv` means "preserve the existing device password." Bitagnis
cannot verify that password and MUST report the field as unmanaged. Supplying
`passwordEnv` makes the desired password managed, although operational recovery
rather than readback is the only available verification.

Literal passwords in YAML MUST be rejected. Environment variable names may be
stored, but their resolved values MUST NOT be persisted or logged.

Hostname overrides will merge individual nested fields using the same
omitted-versus-explicit semantics as existing settings overrides. Unknown YAML
keys will continue to fail startup.

### Validation

Bitagnis MUST validate the complete effective configuration before contacting any
device:

- pool URL/host is non-empty and within the firmware's length limit;
- ports are in the range 1 through 65535;
- primary URL, port, and user are complete;
- fallback URL, port, and user are either complete or explicitly disabled;
- referenced environment variables exist when their values are required;
- v2.8.1 pool hosts are normalized to the bare-host form expected by its UI;
- values unsupported by the detected firmware produce an error rather than
  being silently ignored;
- restart and health timeouts are positive and bounded.

## Device API changes

[`lib/bitaxe.go`](lib/bitaxe.go) should add separate public models for observed
and desired state:

```go
type PoolConfig struct {
    URL  string
    Port uint16
    User string
}

type MiningConfig struct {
    Primary  PoolConfig
    Fallback *PoolConfig
}
```

Passwords should be carried only in a short-lived apply request. They should not
be fields on long-lived observed models or printable types.

The client interface should gain operations equivalent to:

```go
GetMiningConfig(context.Context, string) (MiningConfig, error)
SetMiningConfig(context.Context, MiningConfigPatch, string) error
Restart(context.Context, string) error
```

The lower-level PATCH implementation should accept typed payloads without
forcing mining fields into the existing operating-point structure.

`GET /api/system/info` should also capture:

- firmware version;
- ASIC and board model;
- primary and fallback pool fields;
- fallback-in-use status;
- any available pool connection status;
- uptime and telemetry needed for restart verification.

## Mutation coordinator

Bitagnis MUST serialize mutations per miner. A mining update, optimizer trial,
safety rollback, and overheat recovery may not issue overlapping PATCH or
restart requests.

Safety remains the highest priority:

1. Emergency and overheat recovery.
2. Safety rollback.
3. Confirmation of an already-started mutation.
4. Explicit mining configuration apply.
5. Automatic managed-configuration reconciliation.
6. Optimizer exploration.

An emergency may interrupt a pending mining rollout. A normal mining update
MUST wait until the miner is not overheated and does not have an unconfirmed
operating-point request.

The coordinator should own the complete PATCH/restart/verify lifecycle instead
of exposing restart behavior separately to optimizer policy code.

## Reconciliation flow

For each target miner:

1. Discover the device and identify it by MAC address.
2. Read system information and determine firmware capabilities.
3. Resolve and validate the effective desired configuration.
4. Compare observed non-secret fields with desired fields.
5. Compare the desired secret revision with the last successfully applied
   revision, without storing secret values.
6. Produce a redacted plan.
7. Wait for a safe mutation window.
8. Persist an `APPLYING` state before the PATCH.
9. PATCH only the managed fields. Password fields are included only when their
   configured revision needs to be applied.
10. Persist a `RESTARTING` state and call `POST /api/system/restart`.
11. Poll with bounded exponential backoff until the device returns.
12. Confirm that uptime reset and the MAC address still identifies the target.
13. Verify all readable mining settings.
14. Clear pre-restart telemetry samples and begin a new ramp-up period.
15. Wait for plausible positive hashing and the absence of thermal, power,
    voltage-regulator, or firmware faults.
16. Mark the desired revision `IN_SYNC`.
17. Proceed to the next device only after the current device succeeds.

A short period without an accepted share is not a failure. Solo pools and
high-difficulty pools may legitimately take much longer than the health
timeout to return an accepted share. Hashing, pool status when available, and
fault telemetry are the primary short-term health signals.

## Optimizer coordination

A configuration restart resets uptime, hash telemetry, and share counters. It
must not contaminate an evaluation window.

After any confirmed restart, Bitagnis MUST:

- discard in-memory telemetry samples for that MAC address;
- clear pending observation counters;
- set `RampUntil` using the effective host ramp-up setting;
- return the optimizer to a baseline/hold transition appropriate to its prior
  state;
- preserve validated operating-point records;
- preserve the selected safe or best operating point when it is still the live
  configuration;
- avoid interpreting reset share counters as wraparound or negative deltas.

Bitagnis SHOULD track the last observed uptime or an equivalent boot generation
so that restarts initiated outside Bitagnis also reset the telemetry window.

## Durable state

Mining reconciliation state belongs in a separate SQLite table rather than in
the optimizer phase:

```text
mining_config_state
  mac_addr                 primary key
  desired_revision
  last_applied_revision
  phase
  changed_at
  restart_requested_at
  last_verified_at
  retry_after
  last_error
```

Suggested phases:

- `UNKNOWN`
- `IN_SYNC`
- `DRIFT`
- `APPLYING`
- `RESTARTING`
- `VERIFYING`
- `FAILED`

The desired revision should be derived from the canonical effective
configuration. Secret material MUST NOT be stored directly. A plain,
unsalted hash of a password MUST NOT be stored because weak passwords could be
tested offline. A process-local comparison or keyed digest using a separately
managed key is acceptable.

If Bitagnis stops during a mutation, the next process must resume reconciliation
from persisted state and live device evidence. It must not blindly send a
second restart.

## CLI

Existing optimizer invocation remains compatible:

```sh
bitagnis
bitagnis mineira mineiro
```

Mining configuration adds explicit subcommands:

```sh
bitagnis mining status
bitagnis mining status mineira
bitagnis mining plan mineira
bitagnis mining apply mineira
bitagnis mining apply mineira mineiro
bitagnis mining apply --all
```

Behavior:

- `status` is read-only and redacts users as appropriate.
- `plan` is read-only and shows which non-secret fields would change, whether a
  write-only secret revision would be applied, and whether a restart is needed.
- `apply` performs the planned rolling mutation.
- Applying to every discovered miner requires the explicit `--all` flag.
- A failed miner stops the remaining rollout by default.
- A future `--continue-on-error` option may relax that behavior.

The daemon's terminal table should add a compact mining state such as
`POOL:OK`, `POOL:DRIFT`, `POOL:RESTART`, or `POOL:FAILED`. It must not print
passwords or complete sensitive usernames.

## Failure handling and rollback

Failures are divided into four classes:

1. **Validation failure**: make no API calls.
2. **PATCH failure**: do not restart; record the error and retry only with
   bounded backoff or a new explicit apply.
3. **Restart/reappearance failure**: stop the fleet rollout and preserve
   recovery instructions.
4. **Post-restart health failure**: attempt rollback only when Bitagnis has a
   complete, previously managed recovery configuration whose secrets are still
   resolvable.

Because AxeOS does not return passwords, Bitagnis cannot safely snapshot an
arbitrary manually configured pool and later restore its password. The first
managed apply therefore has no automatic secret rollback unless the operator
provides an explicit recovery profile.

If rollback is unavailable, Bitagnis MUST:

- stop applying configuration to other miners;
- continue safety monitoring if the device remains reachable;
- show the failed hostname and redacted intended endpoints;
- state that manual AxeOS recovery may be required;
- avoid repeated restart loops.

Automatic retries must use a durable `retry_after` backoff. Transient read
errors or a single mismatching poll must never trigger a restart.

## Security

AxeOS configuration calls can redirect hash power and carry pool credentials.
They must be treated as privileged operations.

- Keep Bitagnis and Bitaxe management endpoints on a trusted LAN.
- Do not expose the AxeOS HTTP API through an unauthenticated public proxy.
- Resolve passwords from environment variables or a future secret provider.
- Never include secrets in logs, errors, test fixtures, SQLite, plan output, or
  panic dumps.
- Redact pool users by default because they may contain a Bitcoin address or
  account identifier.
- Set restrictive permissions on any local file containing user identifiers.
- Reject redirects to unexpected hosts when calling device APIs.
- Continue using bounded response sizes and HTTP timeouts.
- Record who/what initiated a mutation (`explicit`, `managed`, `safety`, or
  `optimizer`) without recording secret payloads.

## Firmware compatibility

### Required first target

AxeOS v2.8.1 on BM1370 board 601 is the required compatibility target. Its
primary/fallback flat fields are the normative initial implementation.

### Version-aware extensions

Newer AxeOS releases advertise additional fields such as Stratum protocol,
TLS, suggested difficulty, richer connection status, pause/resume, and a
multi-pool schema. The current official release is
[v2.14.2][axeos-v2142-release], and its
[OpenAPI schema][axeos-current-openapi] documents the expanded model.

Bitagnis must not infer support solely from a version string. It should combine:

- parsed firmware version;
- fields observed from `GET /api/system/info`;
- known capability rules;
- integration tests against the target firmware.

Unknown firmware should fall back to read-only reporting unless the requested
payload is proven compatible. A future adapter interface may support legacy
flat pools and newer pool models independently.

Firmware upgrades are operationally separate from mining configuration. The
recommended rollout is to add version-aware Bitagnis support, upgrade one miner,
validate it, and only then upgrade the rest.

## Observability

Each mutation should emit structured, redacted events:

- desired drift detected;
- apply started;
- PATCH acknowledged;
- restart requested;
- device offline;
- device rediscovered;
- readback verified;
- mining health recovered;
- rollout completed or stopped;
- rollback started/completed/failed.

Useful durations include PATCH latency, restart downtime, time to positive hash
rate, and total reconciliation time. Repeated identical poll failures should be
rate-limited in logs.

## Testing

### Unit tests

- YAML defaults and nested hostname override merging.
- Unknown-field rejection.
- URL, port, fallback completeness, environment, and timeout validation.
- Exact legacy v2.8.1 JSON payloads.
- Password omission versus intentional secret update.
- Secret redaction in plans, errors, logs, and formatted structs.
- Capability selection by firmware and observed schema.
- Drift detection and canonical configuration revision.
- Uptime reset and reboot detection.

### Controller tests

- No-op when configuration is already synchronized.
- Plan never mutates.
- PATCH occurs before restart.
- Restart occurs exactly once after a successful PATCH.
- PATCH failure never triggers restart.
- Device disappearance and reappearance are tolerated.
- Wrong MAC after restart is rejected.
- Readback mismatch fails verification.
- Hash recovery succeeds without requiring an accepted share.
- Optimizer samples reset after restart.
- Safety rollback preempts mining reconciliation.
- Mining apply waits for an existing optimizer mutation.
- A failed first miner prevents mutation of the second miner.
- Process restart resumes an interrupted reconciliation safely.

### Integration test

Use one Bitaxe as a canary:

1. Record its current non-secret mining and operating-point settings.
2. Run `mining plan` and confirm zero writes.
3. Apply a test worker configuration.
4. Confirm one restart and the same MAC address after rediscovery.
5. Confirm exact readable pool settings.
6. Confirm positive sustained hash rate and safe telemetry.
7. Confirm the optimizer begins a fresh ramp/evaluation window.
8. Restore the production profile through the same managed path.

No fleet-wide automatic management should be enabled until this canary test
passes.

## Rollout plan

### Phase 0: correct existing actuator semantics

- Add restart support to `BitaxeClient`.
- Add the per-miner mutation coordinator.
- Route operating-point PATCHes through PATCH/restart/verify.
- Detect external reboots and reset telemetry windows.

### Phase 1: read-only mining control

- Extend `Info` with firmware and pool fields.
- Add configuration models and validation.
- Implement `mining status` and `mining plan`.
- Add redaction tests.

### Phase 2: explicit legacy apply

- Implement v2.8.1 mining PATCH payloads.
- Add durable reconciliation state.
- Implement rolling restart and verification.
- Enable `mining apply` for named miners.

### Phase 3: managed reconciliation

- Enable `mode: managed`.
- Add drift detection, backoff, recovery behavior, and fleet stopping rules.
- Surface mining state in normal output.

### Phase 4: newer firmware capabilities

- Add a capability adapter for supported newer AxeOS releases.
- Add pause/resume where available.
- Add protocol, TLS, and extended-pool settings only after device-level tests.

## Acceptance criteria

The RFC is implemented when:

1. `bitagnis mining plan` performs no writes and shows a fully redacted,
   deterministic plan.
2. Bitagnis can apply primary and fallback pool settings to either current
   v2.8.1 Bitaxe.
3. Applying to both miners restarts and verifies them sequentially.
4. A failure on one miner stops the default rollout before changing the next.
5. No password appears in YAML, logs, output, SQLite, errors, or tests.
6. The optimizer cannot mutate a miner concurrently with mining
   reconciliation.
7. Every restart creates a fresh telemetry ramp/evaluation window.
8. A no-op configuration causes no PATCH and no restart.
9. Manual drift is visible and follows the configured management mode.
10. Existing `bitagnis [hostnames...]` behavior remains compatible.

## Alternatives considered

### Write pool configuration during every metrics poll

Rejected. Mining configuration is low-frequency desired state. Repeated writes
increase flash wear, create restart risk, and couple configuration availability
to the thermal loop.

### Put passwords directly in `settings.yaml`

Rejected. The file is local and ignored by Git, but plaintext secrets would
still leak through backups, diagnostics, or accidental copies.

### Restart every miner concurrently

Rejected. A rolling restart preserves fleet capacity and prevents one bad
configuration from taking every miner offline.

### Treat PATCH success as completion

Rejected. AxeOS v2.8.1 persists settings without loading them into the running
miner, and its password fields cannot be verified by readback.

### Require an accepted share during health verification

Rejected. Share timing depends on pool difficulty and is unsuitable as a short
fixed-time readiness check.

## Follow-up work

- Decide whether an explicit recovery profile is required before enabling
  automatic managed mode.
- Define and test the exact capability matrix for supported post-v2.8.1 AxeOS
  releases.
- Consider a pluggable secret provider after environment-variable support is
  proven.

[axeos-v281-openapi]: https://github.com/bitaxeorg/ESP-Miner/blob/v2.8.1/main/http_server/openapi.yaml#L302-L449
[axeos-v281-handler]: https://github.com/bitaxeorg/ESP-Miner/blob/v2.8.1/main/http_server/http_server.c#L388-L530
[axeos-v2142-release]: https://github.com/bitaxeorg/ESP-Miner/releases/tag/v2.14.2
[axeos-current-openapi]: https://github.com/bitaxeorg/ESP-Miner/blob/v2.14.2/main/http_server/openapi.yaml
