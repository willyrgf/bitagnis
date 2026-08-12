# Bitagnis

Bitagnis finds the highest sustained Bitaxe hash rate that remains inside
configured ASIC-temperature, voltage-regulator-temperature, and board-power
limits. It treats ASIC frequency and core voltage as one operating point;
neither setting is tuned independently.

## Optimizer

Bitagnis reads the frequency and voltage options advertised by AxeOS and only
sends complete `(frequency, coreVoltage)` pairs from that grid.

For a new miner, Bitagnis starts from the settings currently running in AxeOS. It
waits through the configured ramp period and then requires two admitted
evaluation windows before changing anything. For each window it calculates:

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

Evidence collection is durable and state-linear, not wall-clock. Ramp
completion is a count of consecutive settled samples at the current point, not
a timer, so a slow or lossy poll cycle degrades by taking longer instead of by
failing. A window closes once it has enough samples or its span backstop is
reached, and is admitted only if it kept enough samples and no gap inside it
was too large; a poll that cannot be read (bad identity, bad ASIC grid, or
incomplete telemetry) is a non-event for this machinery and neither advances
nor discards progress. Progress toward the current evaluation, including a
window already closed and stored, survives a process restart. An evaluation
that keeps failing to produce an admissible window six times is marked
starved, not silently absorbed. Starved and measured-rejected points both
enter `MONITOR`, which continues collecting two-window assessments until
conditions justify a fresh pass. No timer or operator action is required.

## Safety

Safety is checked on every metrics poll, including while a trial is ramping.
The default policy:

- explores while p95 ASIC temperature is below the 65°C target, power is at
  most 22 W, and VR temperature is below 90% of its limit;
- accepts a faster point but stops exploring between 65°C and the 66°C limit;
- rolls back above 66°C, at 24 W, or at 97°C VR temperature;
- treats 70°C as a host emergency and immediately contains a still-running
  miner at the exact minimum advertised pair; and
- treats AxeOS `overheat_mode` as a firmware-owned emergency: Bitagnis waits
  for AxeOS to cool, restart mining at its paired 100 MHz / 100 mV reduction,
  and clear the flag before reconciling the reduced complete pair.

Normal rollback selects the closest validated point with complete
ASIC/VR/power headroom evidence and no frequency or voltage increase relative
to the failed live pair. It prefers a point that lowers both components; it
never guesses an unvalidated adjacent pair. If no such record exists, it uses
the exact minimum advertised pair. A rollback can run while the failed point
remains above an ordinary hard limit; ordinary point and mining changes cannot.

AxeOS v2.8.1 trips strictly above 75°C ASIC temperature or 105°C VR
temperature and stores the unadvertised emergency state `50 MHz / 1000 mV`.
Bitagnis never adopts or evaluates that firmware state. Firmware recovery
requires positive, finite ASIC temperature, VR temperature, and power; every
recovery boundary; no power fault; and supported device identity.
On 600-series boards AxeOS powers the ASIC down during this episode, so ASIC
temperature is unavailable while firmware performs its own VR-temperature and
minimum-cooling-cycle loop. Bitagnis treats that readback as a non-event: it
does not PATCH, restart, clear a typed safety intent, or downgrade a previously
verified firmware-overheat cause. Once the flag clears, two stable safe reads
adopt an already-advertised reduced pair without another hardware request and
enter `COOLDOWN`. If AxeOS reduced to an off-grid pair, Bitagnis waits for
recovery telemetry, chooses the closest advertised complete pair no greater in
either component (falling back to the exact minimum only when none exists),
and applies it through one restart-verified `firmware_recovery`. The recovered
point then passes the normal dwell and safety-validation window before a fresh
baseline resumes upward exploration.

**`COOLDOWN` exits on a durable count of consecutive healthy polls, not a
timer.** Its previous exit — a wall-clock timer that grew with repeated
emergency count, capped at 24 hours — raced accumulated evidence rather than
authorizing a transition and has been removed along with the clock and
`overheatCooldownMinutes` setting it depended on. Every poll while a miner is
in `COOLDOWN` is checked against the same `safeToRecover` predicate firmware
recovery uses; a satisfying poll advances a durable counter, and any
non-satisfying poll resets it to zero. The dwell length — `ceil(60s /
metricsInterval)` consecutive satisfying polls — is derived from AxeOS's own
autonomous overheat recovery (`power_management_task.c`'s six consecutive
5-second checks at or below its 45°C safe threshold, 30s), doubled for
Bitagnis's coarser and lossier poll cadence. Reaching the threshold clears the
durable `SafetyReason` in the same transition and opens a `safety_validation`
evidence epoch; a single
required window's worth of admitted evidence then either closes validation and
atomically opens a fresh two-window baseline at the recovered current point,
or, if that window's quality is unhealthy, rejects the epoch and leaves the
miner in `COOLDOWN` to accumulate the healthy-poll count again. Successful
recovery resumes the same finite optimization pass: evaluated point history is
preserved, an interrupted unobservable candidate remains consumed, and the
frontier continues with the next unseen eligible pair. Safety recovery is not
an absorbing state and does not require an operator retune.
A completed safety mutation and healthy-mining resumption never open this
epoch: the cooldown recovery predicate is its sole owner. Every store
transition and database reopen verifies that an open `safety_validation` epoch
is paired with `COOLDOWN`, an empty `SafetyReason`, a zero recovery counter,
and no unfinished safety resumption.
A process restart or a lost poll tick costs at most the healthy-poll count
in progress, never previously-earned recovery evidence.

`EMERGENCY` is the one durable emergency episode and fleet safety block. A
typed `safety_rollback` or `firmware_recovery` intent is the only authority for
the corresponding PATCH. If the exact minimum is already active and remains
unsafe without a firmware flag, Bitagnis holds the emergency without replaying
PATCH or restart. Once a host-contained miner at the minimum reports complete
recovery telemetry, it enters cooldown without another hardware request. A
safe verification-unknown episode likewise adopts two stable observations of
an advertised live pair and rebaselines; it never manufactures minimum-pair
containment from missing data. A neutral or incomplete poll cannot replace an
unchanged pending safety authority; only newly validated unsafe evidence can
supersede it.

## Hardware mutations and startup

Every operating-point, rollback, firmware-recovery, and enabled mining-setting
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

Temporary disappearance, incomplete telemetry, a delayed/unsupported grid,
wrong identity, or a safe pair mismatch never closes the durable attempt. The
worker deadline bounds only one reconciliation worker; a later pass resumes at
the recorded milestone without replaying PATCH or restart. Only proven unsafe
telemetry may supersede immediately. Two consecutive complete safe post-boot
reads exactly 100 MHz / 100 mV below the requested pair are classified as the
specific AxeOS autonomous reduction described above; other stable safe
mismatches use the manual-adoption path.

Fleet polling begins immediately in safety-only mode. Emergency recovery and
hard rollback outrank normal work and may run concurrently for different
miners. Any selected miner with an emergency episode or typed safety intent,
including an offline miner or a mutation-free emergency hold, suppresses new
normal fleet work without stopping polling. Enabled mining settings reconcile
one miner at a time in MAC order, and optimization opens only after every
selected miner has two consecutive safe, positive-hash startup polls. A mining
failure blocks the next normal miner while safety polling continues.
The closed write gate does not erase healthy evidence on other miners: a
baseline may close and store its first of two windows, then pauses before the
second window could admit a frontier mutation. Trials pause immediately
because their first window can authorize an early return mutation.

## State

Optimizer state and evaluated operating points are stored in `optimizer.db`.
The database is exclusively owned by one Bitagnis process. A second process
using the same path fails at startup.

The current schema is version 11, with typed pending mutations, finite frontier
state, continuous-monitor references, hourly accounting, a durable
evidence-epoch ledger, and one durable
`mutation_attempts` row per controller-owned hardware attempt. It has no legacy
`overheat_pending` field, migration, or compatibility reader. Schema version 10
and earlier, an old partial database, or an unknown application object is
rejected without modification; move it aside or remove it to create the
current baseline. Earlier schemas allowed optimizer conclusions to become
absorbing and do not contain the durable evidence needed to reconstruct a
continuous monitor reference, so they are rejected whole rather than
reinterpreted or repaired. Evaluated
history, pending mutation ages, emergency episode ages,
mutation attempts, evidence epochs, and the bounded 384-hour hourly accounting
history persist across ordinary restarts after that baseline is created.

`evidence_epochs` is the durable ledger for evaluation progress, shaped after
`mutation_attempts`: one open epoch per miner, monotone settled-sample and
window counters, and a terminal outcome (`validated`, `rejected`, `starved`,
or `contradicted`). Each open purpose has one durable state shape: baseline,
trial, and monitor epochs require their matching phase and two windows; safety
validation requires `COOLDOWN` and one window. A selected monitor points to a
closed monitor epoch and a separately stored conservative combination of both
windows; the ledger retains its first admitted window as evidence. These
relationships are checked before every `Apply` commit and on reopen. The ledger
replaces the volatile in-memory progress and wall-clock
evidence deadline schema version 6 used, which discarded everything it had
learned on every process restart or mutation gate and could not distinguish
"never measured" from "measured many times and lost it." A process restart
now costs at most one partial evaluation window, not the whole evaluation.
`optimizer_miners` additionally carries `unreadable_poll_count` (a durable
count of consecutive polls the optimizer could not read at all — bad grid, bad
identity, or incomplete telemetry — that escalates to a safety-unknown episode
after twelve consecutive misses, and never on its own suppresses instantaneous
safety assessment) and `recovery_healthy_count` (the `COOLDOWN` recovery
predicate's durable dwell counter described above; nonzero only in `COOLDOWN`
or `EMERGENCY`, reset to zero by any non-satisfying poll and on the poll that
reaches the threshold).

Hourly wall-clock coverage is stored as integer nanoseconds, so merged bucket bounds and
accounting-cursor coverage are exact. Hash-work totals remain floating-point measurements.

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
polls must confirm the new pair. Bitagnis then adopts it into a fresh monitor
assessment.
Off-grid manual settings can be monitored, but Bitagnis will not emit them as
automated requests. Before treating a differing live pair as a manual change,
Bitagnis checks whether it is actually the verified target of one of its own
failed or superseded mutation attempts; if so, the eventual adoption — still
gated on the same two consecutive polls — is logged as a reconciliation of its
own ledger rather than as an external retune. This covers a non-safety
mutation failure after a verified PATCH (for example a post-restart health
check that never saw a positive hash); it does not yet cover a safety-recovery
attempt reaching the same shape, since automated safety recovery does not
currently clear the durable safety reason that a reconciliation would need to
see cleared — a known, separate gap.

`MONITOR` is never terminal. `selected` means the current point is the latest
safe frontier choice; `manual`, `rejected`, `starved`, and `off_grid` record why
that point entered monitoring. Every reason retains an open two-window monitor
epoch. Selected monitoring compares each completed assessment with its durable
reference and starts a new environmental pass after a persistent material hash,
quality, or thermal-headroom change. Rejected and starved monitoring starts a
new pass after conditions recover. Off-grid points are observed but never used
as automated targets; if that exact canonical pair later appears on the
supported advertised grid, healthy monitoring may begin a same-point pass.

Each optimization pass is finite: an advertised complete pair is consumed at
most once in that pass. Continuous monitoring may start a fresh environmental
pass when the measured environment or performance materially changes; it does
not issue hardware work merely because time elapsed. A safety episode pauses a
pass but does not finish it: successful
recovery rebaselines the safe current point and continues past every already
consumed pair. Selected monitoring begins only after the safe frontier has
no unseen admissible candidate (or no exploration headroom remains) and the
selected highest sustained-hash point has passed final validation. After an
environmental or hardware change, explicitly qualify one named miner for a new
pass:

```sh
./bitagnis --retune bitaxe-example
```

`--retune` never resets safety state or issues hardware writes by itself. It is
accepted only after the named miner has two consecutive safe startup polls in a
safe selected, manual, rejected, or starved monitor state; it rejects `all`,
mining reapply, pending work, off-grid points, and active safety episodes.

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

- `BASELINE`: validating the current pass point, including after safety recovery;
- `UNDERVOLT`: testing a lower voltage at the same frequency;
- `FREQ_TEST`: testing the next frequency;
- `VOLT_TEST`: testing whether one higher voltage improves a new frequency;
- `MONITOR`: continuously collecting wider two-window evidence at the current
  selected, manual, rejected, starved, or off-grid point;
- `COOLDOWN`: monitoring without upward exploration until the consecutive-safe
  dwell and safety-validation window complete, then resuming `BASELINE`; and
- `EMERGENCY`: the durable fleet safety block; its typed cause determines
  whether AxeOS is cooling, host containment is required, or verification is
  waiting for readable evidence.

Durable ordinary work is shown as `PENDING`, typed hard-limit work as
`BACKOFF`, host or firmware-trip containment as `CONTAIN`, AxeOS-owned cooling
as `AXEOS`, unreadable verification as `VERIFY`, and post-backoff observation
as `RECOVERY`. During `MONITOR`, the window column shows the reason plus live
window progress, such as `selected 1/2 12/30`. It shows `minimum`, `normalize`,
`firmware cool`, or `verify` for the matching safety obligation. Target and episode ages come from durable
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
clear only after proof, retained `EMERGENCY` until recovery telemetry is safe,
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
