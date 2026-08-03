# Bitagnis

Bitagnis finds the highest sustained Bitaxe hash rate that remains inside
configured ASIC-temperature, voltage-regulator-temperature, and board-power
limits. It treats ASIC frequency and core voltage as one operating point;
neither setting is tuned independently.

## Optimizer

Bitagnis reads the frequency and voltage options advertised by AxeOS and only
sends complete `(frequency, coreVoltage)` pairs from that grid.

For a new miner, Bitagnis starts from the settings currently running in AxeOS. It
waits through the configured ramp period and then measures a full evaluation
window before changing anything. For each window it calculates:

- median actual and expected hash rate;
- actual/expected hash attainment;
- mean and p95 ASIC temperature, plus p95 VR temperature and power; and
- optional AxeOS ASIC error percentage and pool-share deltas.

Actual sustained hash rate is the optimization objective. AxeOS expected hash
and attainment are displayed and stored as diagnostics, but never veto a
faster, healthy operating point.

The search follows the thermal frontier:

1. Find the lowest voltage at the baseline frequency that preserves hash.
2. Start every higher frequency at the lowest voltage advertised by AxeOS.
3. Sweep voltage upward only while actual hash or ASIC errors materially improve.
4. Keep the healthy operating point with the highest sustained actual hash.

This low-to-high sweep prevents a hot high-voltage trial from hiding a faster,
cooler point at the same frequency. One bad window at an established point is
never enough to change voltage.

## Safety

Safety is checked on every metrics poll, including while a trial is ramping.
The default policy:

- explores while p95 ASIC temperature is below the 65°C target, power is at
  most 22 W, and VR temperature is below 90% of its limit;
- accepts a faster point but stops exploring between 65°C and the 66°C limit;
- rolls back above 66°C, at 24 W, or at 97°C VR temperature;
- treats 70°C as a host emergency and immediately contains a still-running
  miner at the exact minimum advertised pair; and
- treats AxeOS `overheat_mode` as a firmware emergency that must cool before
  the flag is cleared with that same minimum pair.

Normal rollback selects the validated point with the highest actual hash,
complete ASIC/VR/power headroom evidence, and no frequency or voltage increase
relative to the failed live pair. If no such record exists, it uses the exact
minimum advertised pair. A rollback can run while the failed point remains
above an ordinary hard limit; ordinary point and mining changes cannot.

AxeOS v2.8.1 trips strictly above 75°C ASIC temperature or 105°C VR
temperature and stores the unadvertised emergency state `50 MHz / 1000 mV`.
Bitagnis never adopts or evaluates that firmware state. Firmware recovery
requires positive, finite ASIC temperature, VR temperature, and power; every
recovery boundary; no power fault; and supported device identity. Repeated
emergency episodes extend cooldown up to 24 hours.

`OVERHEAT` is the one durable emergency episode and fleet safety block. A
typed `safety_rollback` or `overheat_recovery` intent is the only authority for
the corresponding PATCH. If the exact minimum is already active and remains
unsafe without a firmware flag, Bitagnis holds the emergency without replaying
PATCH or restart. Once a host-contained miner at the minimum reports complete
recovery telemetry, it enters cooldown without another hardware request.

## Hardware mutations and startup

Every operating-point, rollback, overheat-recovery, and enabled mining-setting
change uses one restart-verified lifecycle:

```text
validate -> persist intent and attempt -> recheck identity and safety
         -> record PATCH milestone -> PATCH -> record restart milestone
         -> restart -> rediscover the same MAC -> prove a new boot -> verify
         -> atomically complete state and attempt -> two healthy mining polls
```

AxeOS v2.8.1 writes frequency and voltage separately to NVS, and its running
power task may observe and apply those writes before restart. Bitagnis
therefore completes every identity, telemetry, grid, durable-authority, and
rollback-evidence check before PATCH. PATCH success or pre-restart configured
readback never proves active configuration. Completion still requires restart,
same-MAC rediscovery, a proven new boot, and exact configured NVS pair readback;
AxeOS exposes no measured active-frequency field.

Fleet polling begins immediately in safety-only mode. Emergency recovery and
hard rollback outrank normal work and may run concurrently for different
miners. Any selected miner with an emergency episode or typed safety intent,
including an offline miner or a mutation-free emergency hold, suppresses new
normal fleet work without stopping polling. Enabled mining settings reconcile
one miner at a time in MAC order, and optimization opens only after every
selected miner has two consecutive safe, positive-hash startup polls. A mining
failure blocks the next normal miner while safety polling continues.

## State

Optimizer state and evaluated operating points are stored in `optimizer.db`.
The database is exclusively owned by one Bitagnis process. A second process
using the same path fails at startup.

The current schema is version 5, with typed pending mutations, finite frontier
state, hourly accounting, and one durable
`mutation_attempts` row per controller-owned hardware attempt. It has no legacy
`overheat_pending` field, migration, or compatibility reader. Schema versions 3
and 4,
an old partial database, or an unknown application object is rejected without
modification; move it aside or remove it to create the current baseline.
Evaluated history, cooldown, pending mutation ages, emergency episode ages,
mutation attempts, and the bounded 384-hour hourly accounting history persist
across ordinary restarts after that baseline is created.

An optimized operator retune atomically preserves the prior selected point's
complete frequency/voltage pair, conservative median hash, and settled
timestamp in the pass-reference snapshot. Initial and manual passes may have
no arm snapshot. Report mode consumes this exact snapshot for historical
AB/BA control evidence when the second retune starts exactly at the prior arm
end; a reset inside an arm or after an inter-arm gap leaves that historical
boundary unavailable. It never infers a missing boundary from current state or
the new pass's point rows.

Mutation history records the mutation kind, finite reason, complete source and
target pair, intent/start time, configured-readback and restart milestones,
reboot proof, first-positive observation, durable completion, two-poll
healthy-mining resumption, and a deterministic failure stage. An interrupted
process leaves the same durable attempt unfinished for reconciliation; it is
never silently marked failed or replayed as a second hardware authority. This
supports long-term restart counts and restart-to-healthy-mining duration by
miner and mutation kind. It does not record external/manual reboots, raw
telemetry, free-form errors, Stratum settings, or credentials.

For a credential-free restart summary:

```sql
SELECT
  kind,
  SUM(restart_requested_at != 0) AS restart_requests,
  SUM(mining_resumed_at != 0) AS healthy_resumptions,
  ROUND(AVG(CASE
    WHEN mining_resumed_at != 0
    THEN (mining_resumed_at - restart_requested_at) / 1000000000.0
  END), 1) AS average_restart_to_healthy_seconds
FROM mutation_attempts
GROUP BY kind
ORDER BY kind;
```

Before replacing an older baseline, stop the old controller, prove it is no
longer issuing requests, record every selected miner's exact MAC, live complete
pair, uptime, and safe telemetry, and resolve every old pending obligation.
Archive the old database as owner-only diagnostic state and start the new
baseline only after the live fleet state is understood.

If AxeOS settings are changed manually while Bitagnis is running, two consecutive
polls must confirm the new pair. Bitagnis then adopts it as a fresh baseline.
Off-grid manual settings can be monitored, but Bitagnis will not emit them as
automated requests.

Normal optimization is a finite pass. Each advertised complete pair is consumed
at most once per pass, terminal point outcomes are not reopened by elapsed time
or cooler telemetry, and a settled `HOLD` performs no normal operating-point
mutation. After an environmental or hardware change, explicitly qualify one
named miner for a new pass:

```sh
./bitagnis --retune bitaxe-example
```

`--retune` never resets safety state or issues hardware writes by itself. It is
accepted only after the named miner has two consecutive safe startup polls in a
settled, unblocked `HOLD`; it rejects `all`, mining reapply, pending work, and
active or unsettled safety episodes. A settled safety-derived `HOLD` may qualify;
the accepted pass clears its safety reason and starts without an arm snapshot.

## Configuration

Copy `settings.example.yaml` to `settings.yaml`:

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
    enabled: false
    primary:
      host: pool.example.net
      port: 3333
      user: worker-name
      passwordEnv: BITAGNIS_PRIMARY_STRATUM_PASSWORD
    fallback:
      host: fallback.example.net
      port: 3333
      user: worker-name
      passwordEnv: BITAGNIS_FALLBACK_STRATUM_PASSWORD

overrides:
  bitaxe-example:
    targetTemp: 64
    mining:
      enabled: false
      primary:
        user: worker-bitaxe-example
      fallback:
        user: worker-bitaxe-example
```

Every value except the global metrics interval may be overridden by hostname.
Unknown keys are rejected during startup. Mining may be enabled in the defaults
for every selected miner and disabled or customized by hostname overrides. When
enabled, both pools must be complete, hosts must be bare DNS names or IPv4
addresses, and `passwordEnv` names portable entries in the local `.env` file.
Literal password keys are rejected. Resolved passwords are never printed or
persisted by Bitagnis. Enabling mining in the defaults authorizes reconciliation
for every selected miner; it does not waive the new-deployment canary procedure
below.

In clean durable state, matching readable pool settings cause no PATCH, restart,
or `.env` lookup. A pending mining obligation still resumes after a crash.
AxeOS does not return passwords, so explicitly reapply a password-only change
for named enabled miners. Put the named entries in the ignored `.env` file:

```dotenv
BITAGNIS_PRIMARY_STRATUM_PASSWORD='synthetic-example'
BITAGNIS_FALLBACK_STRATUM_PASSWORD='synthetic-example'
```

Then run:

```sh
./bitagnis --reapply-mining bitaxe-example
```

The supported `.env` syntax is blank lines, full-line `#` comments, and
`NAME=VALUE` entries. Values may be unquoted or wrapped in single or double
quotes. Bitagnis reads both passwords from one file snapshot only when a mining
PATCH is required; it does not fall back to process environment variables.

Keep `.env` owner-readable only and provision it without placing real values in
shell history. It is ignored by Git. Never place real values in YAML, tests,
examples, or diagnostics.

## Output

The terminal table reports requested/actual core voltage, optimizer state,
window progress, ASIC and VR temperature, actual/expected hash rate, and power.
Per-miner hash rates are aggregated below the table.

Optimizer states are:

- `BASELINE`: validating a live or manually adopted point;
- `UNDERVOLT`: testing a lower voltage at the same frequency;
- `FREQ_TEST`: testing the next frequency;
- `VOLT_TEST`: testing whether one higher voltage improves a new frequency;
- `HOLD`: running the best currently safe point;
- `COOLDOWN`: monitoring without upward exploration; and
- `OVERHEAT`: containing or waiting for safe recovery.

Durable ordinary work is shown as `PENDING`, typed hard-limit work as
`ROLLBACK`, host containment as `OVERHEAT / contain <pair>`, firmware cooling
as `OVERHEAT / wait cool`, and a minimum-pair hold as
`OVERHEAT / min active / wait cool`. Target and episode ages come from durable
timestamps. These labels report obligations, not an unproven in-process PATCH
stage. The hash column remains live AxeOS actual/expected telemetry, so values
above 100% expected are possible. Only median actual hash from a completed
evaluation window is optimized; pending work never evaluates a trial point.

## Running

Run with Nix:

```sh
nix run .#bitagnis
```

Run every discovered Bitaxe:

```sh
go run .
```

Limit optimization to selected hostnames:

```sh
go run . bitaxe-01 bitaxe-02
```

Every explicitly named hostname must resolve to exactly one MAC. Unknown flags
fail before discovery.

Build and run:

```sh
go build
./bitagnis
```

Read-only long-term economics reports use retained hourly aggregates and
credential-free mutation history. A one-arm report compares a treatment miner
with a settled control over exactly 168 UTC hours:

```sh
./bitagnis --report one-arm treatment-host control-host 2026-08-01T00:00:00Z
```

An AB/BA report evaluates two non-overlapping 168-hour arms with roles reversed:

```sh
./bitagnis --report ab-ba miner-a miner-b 2026-08-01T00:00:00Z 2026-08-08T00:00:00Z
```

Reports normalize actual hash over the full wall duration, count unknown time
as zero work, conservatively classify partial boundary hours as unknown, and
separate normal and safety restart exposure. An arm is coverage-valid when both
miners have at least 95% coverage; its nonnegative uplift is reported even when
operational acceptance gates fail. Acceptance additionally requires an observed
normal-restart baseline with at least a 90% reduction, convergence by 48 hours,
normal restart exposure no greater than 1% of arm time, at least 95%
post-settlement selected-point coverage, an audited first-24-hour frontier with
no duplicate target entry or time-created eligibility, and a settled unchanged
control. The 2% uplift is shown as a practical target. Report mode performs no
discovery, PATCH, restart, mining reconciliation, or mutation.

Bitagnis cannot substitute for adequate cooling or a correctly sized power
supply. AxeOS thermal protection remains the final safety layer.

AxeOS v2.8.1 uses plain HTTP, cannot positively read back pool passwords, may
log a string value when an NVS write fails, and does not prove individual NVS
writes succeeded. `tempCutoff` may not exceed 75°C and `vrTempHigh` may not
exceed 105°C for this supported firmware and board profile.

## Safety-write canary

Device-level verification requires separate explicit authorization for one
named canary, its recorded MAC, live complete pair, uptime, safe telemetry, and
an owner-approved recovery plan. Never heat a miner deliberately or raise a
safety threshold. A lowered host cutoff may exercise containment while the
device remains inside its normal approved envelope.

Require exactly the typed minimum-pair PATCH, one restart, same-MAC return,
proven uptime discontinuity, exact configured pair readback, durable intent
clear only after proof, retained `OVERHEAT` until recovery telemetry is safe,
no same-pair restart at the minimum, and no change to a second miner. Canary
authorization never permits a fleet rollout.

## Mining-write canary

Mining writes on a new deployment remain canary-first because host-side code
cannot remove those firmware risks. Before mutation, the owner must explicitly
authorize one named miner, accept or resolve the risks above, record its exact
MAC, uptime behavior, readable pool fields, operating point, and safe
pre-change telemetry, and keep an owner-only recovery snapshot.

First confirm that matching readable settings cause no PATCH or restart. Then
exercise one named `--reapply-mining` and require one complete PATCH, restart,
same-MAC rediscovery, uptime discontinuity, exact readable-field and
operating-point verification, a fresh ramp, and two safe positive-hash primary
polls. A fallback failover requires separate authorization and must be restored
through the same durable path. Do not enable another miner until the canary
passes, and never treat canary authorization as permission for a fleet rollout.
