# RFC: Long-Term Optimization Churn and Restart Cost

Status: Implemented — canary validation required

Date: 2026-08-01

## Summary

Bitagnis' durable mutation-attempt history proves that the restart-verified hardware mutation
lifecycle and safety rollback behavior are working correctly. It also exposes a separate long-term
optimization problem: the controller does not converge to a stable operating point and hold it for
meaningful periods. Instead, it repeatedly revisits a small set of known points, restarts after most
evaluation windows, and frequently requires an immediate safety rollback.

The analyzed schema-version-3 database covers two miners for approximately 48.4 wall-clock hours,
or 96.6 combined miner-hours. During that interval the controller recorded 661 mutation attempts
and issued 660 restart requests:

| Mutation kind | Attempts | Restart requests | Healthy resumptions | Failed or superseded |
|---|---:|---:|---:|---:|
| Ordinary operating point | 501 | 500 | 445 | 56 |
| Safety rollback | 160 | 160 | 160 | 0 |
| Total | 661 | 660 | 605 | 56 |

This is one fleet restart approximately every 4.4 minutes for more than two days. The median gap
between restarts was 400 seconds for one miner and 420 seconds for the other. Those intervals closely
match one restart, the configured 60-second ramp, and one five-minute evaluation window.

The measured lifecycle history attributes 6.67 miner-hours, or 6.9% of the observed miner-time, to
the interval between a restart request and the second consecutive healthy positive-hash poll. The
second poll is deliberately conservative and occurs after mining has already resumed. Reboot proof
and first-positive-poll timing place plausible actual restart-related mining loss around 4% to 6%
of potential output, before accounting for time spent hashing at poor trial points.

Safety is not the problem. All 160 safety rollbacks strictly de-escalated the complete operating
point, completed the PATCH/restart/reboot-proof lifecycle, and returned to confirmed healthy mining.
The problem is that normal exploration repeatedly enters conditions that require those protections,
and then returns to similar candidates soon afterward.

This RFC records the problem and evidence and proposes a replacement exploration policy based on a
finite frontier pass, isolated trials, terminal point outcomes, and a durable settled state.

## Scope

This RFC covers:

- controller-owned operating-point and safety-recovery mutation attempts;
- restart frequency and restart-to-healthy-mining duration;
- repeated point and transition selection;
- interaction between normal exploration and safety rollback;
- persisted evidence quality and its limitations; and
- requirements and acceptance criteria for the proposed optimization policy.

This RFC does not weaken, delay, rate-limit, or otherwise change safety behavior. Emergency and
hard-limit enforcement must remain immediate regardless of any future normal-optimization budget.

## Dataset

The source is `high_temp_optimizer.db`, inspected read-only on 2026-07-31. The database remained
unchanged during analysis.

| Property | Value |
|---|---:|
| SQLite integrity | `ok` |
| Schema version | 3 |
| Miners | 2 |
| First mutation attempt | 2026-07-29 09:44:48 BRT |
| Last recorded activity | 2026-07-31 10:06:47 BRT |
| Wall-clock span | 48.37 hours |
| Combined miner span | 96.59 miner-hours |
| Mutation attempts | 661 |
| Durable operating-point rows | 40 |
| Open mutation-attempt rows | 0 |

Miner identifiers are anonymized as `miner-01` and `miner-02`. No Stratum configuration,
credentials, IP history, free-form error text, or raw telemetry was read or reproduced.

The active configuration during the run used:

- a ten-second metrics interval;
- a 60-second ramp;
- a five-minute evaluation window;
- a 66°C ordinary thermal rollback limit;
- a 70°C host emergency cutoff;
- a 24 W board-power limit; and
- a 97°C VR-temperature limit.

## What the Mutation History Proves

The schema-version-3 lifecycle records are complete enough to distinguish these milestones:

```text
durable attempt start
    -> PATCH request authorized
    -> restart request authorized
    -> new boot proven
    -> durable mutation completion
    -> two consecutive healthy positive-hash polls
```

For failed attempts they also identify the deterministic stage at which the lifecycle stopped.
Every attempt in the dataset reached either healthy mining or a recorded failure stage. There are no
unclassified or currently open rows.

This is a material improvement over operating-point summaries alone. The earlier schema could show
the latest result for a point but could not answer how many times the point had been entered, how
many restarts occurred, whether a restart was verified, or when mining became healthy again.

## Safety Behavior Is Correct

The history contains 160 `safety_rollback` attempts. All 160 have:

- a PATCH milestone;
- a restart milestone;
- proven reboot;
- durable completion;
- confirmed healthy mining;
- no failure stage; and
- a target that does not increase either frequency or core voltage and is not the failed source
  point.

There are zero non-de-escalating safety targets and zero failed safety attempts.

The latest operating-point records contain thermal failures but no power or VR-temperature failure
status. Their maximum recorded VR temperature and power remain well below the configured hard
limits. This makes ASIC temperature the strong likely cause of the safety rollbacks, although the
mutation-attempt row does not persist the triggering safety dimension and therefore cannot prove
that cause for each individual event.

No `overheat_recovery` attempt appears in the dataset. The run therefore exercises ordinary
hard-limit rollback extensively but does not provide long-term evidence for firmware-overheat flag
recovery.

## Normal Mutation Outcomes

Ordinary operating-point work produced 501 attempts. One failed during preflight and issued no
hardware request. The remaining 500 issued PATCH and restart requests.

| Outcome | Count | Meaning |
|---|---:|---|
| Healthy mining confirmed | 445 | Mutation completed and reached two healthy positive-hash polls |
| `canceled` | 31 | PATCH and restart were requested, but safety superseded the durable intent before completion |
| `mining_superseded` | 24 | Mutation completed, but safety replaced it before two healthy polls |
| `preflight` | 1 | Validation failed before PATCH or restart |

Every one of the 31 canceled attempts was followed by a safety rollback, on average 12.3 seconds
after cancellation. Every one of the 24 mining-superseded attempts was followed immediately by a
safety rollback. These are not missing-history artifacts: they are complete evidence that 55 normal
attempts consumed a restart and were unsafe quickly enough to require another restart before stable
mining was established.

An additional 103 safety rollbacks followed an ordinary attempt that had reached healthy mining.
Of all safety rollbacks, 151 occurred within six minutes of the preceding ordinary attempt's start.
The controller is therefore not merely reacting to slow ambient drift after long stable holds. Most
safety work is directly associated with recently selected trial points.

## Restart Frequency

Restart volume is high for each miner independently:

| Miner | Attempts | Restart requests | Safety restarts | Median restart gap | Approximate restart rate |
|---|---:|---:|---:|---:|---:|
| `miner-01` | 372 | 372 | 68 | 400 seconds | one every 7.8 minutes |
| `miner-02` | 289 | 288 | 92 | 420 seconds | one every 10.1 minutes |
| Fleet | 661 | 660 | 160 | — | one every 4.4 minutes |

Across the 658 measurable same-miner restart gaps:

- 339, or 51.5%, were between 360 and 480 seconds;
- 90, or 13.7%, were under 60 seconds;
- the active-hour average was 13.2 fleet restarts; and
- the busiest hours contained 18 restarts.

The 360-to-480-second concentration is significant because the configured lifecycle naturally
produces approximately this cadence:

```text
restart and verification: about 30 seconds
configured ramp:          60 seconds
evaluation window:       300 seconds
decision and next start:  one polling interval
```

The data therefore shows that a common steady-state behavior is one hardware restart per completed
evaluation window.

## Repeated Search Rather Than Convergence

The controller repeatedly selected the same small set of targets:

| Miner | Ordinary attempts | Distinct ordinary targets | Attempts per target |
|---|---:|---:|---:|
| `miner-01` | 304 | 27 | 11.3 |
| `miner-02` | 197 | 11 | 17.9 |

Frequently repeated transitions include:

- `miner-02`: `400/1060 -> 400/1100` 38 times;
- `miner-02`: `400/1000 -> 400/1060` 29 times;
- `miner-01`: `550/1100 -> 550/1150` 22 times;
- `miner-01`: `525/1100 -> 525/1150` 20 times; and
- `miner-02`: `490/1000 -> 490/1060` 19 times.

Safety rollbacks also repeat. For example, `miner-02` rolled back from `400/1100` to `400/1000`
15 times.

At the end of the 48-hour observation both miners were still in active exploration:

| Miner | Current point | Persisted best | Phase |
|---|---:|---:|---|
| `miner-01` | `490/1200` | `490/1150` | `VOLT_TEST` |
| `miner-02` | `490/1000` | `400/1000` | `FREQ_TEST` |

Neither miner had a pending durable mutation, but neither had converged to a long-lived `HOLD`
state. The controller remained in the same exploration/restart cycle after two days.

## Current Point Evidence

The `operating_points` table stores one current summary per complete pair. Re-evaluating a pair
replaces its earlier summary, so these counts describe the latest status of unique points rather
than the number of evaluation windows:

| Latest status | Unique points | Aggregate actual/expected hash |
|---|---:|---:|
| `validated` | 9 | 87.1% |
| `no_gain` | 18 | 69.4% |
| `unstable` | 4 | 81.4% |
| `thermal` | 9 | 94.0% |

The latest persisted best points are currently validated:

- `miner-01`: `490/1150`, 994.4 GH/s, latest p95 ASIC temperature 58.8°C;
- `miner-02`: `400/1000`, 476.5 GH/s, latest p95 ASIC temperature 51.8°C.

This confirms that the controller has found safe points. The problem is not inability to find a
usable point. It is failure to stop repeatedly leaving those points for candidates already explored
many times.

The no-gain points currently deliver only 69.4% of their aggregate expected hash. Expected hash is
not the optimization objective, and some earlier results have been overwritten, so this is not an
exact loss calculation. It nevertheless shows that restart downtime is not the only cost: the
controller also spends evaluation windows on materially weak candidates.

## Restart Lifecycle Timing

Successful attempts have consistent timing:

| Kind | Metric | Median | p95 | Maximum |
|---|---|---:|---:|---:|
| Ordinary | Restart request to reboot proof | 22.1 s | 22.2 s | 56.3 s |
| Ordinary | Restart request to second healthy poll | 39.9 s | 39.9 s | 79.9 s |
| Safety rollback | Restart request to reboot proof | 22.1 s | 27.1 s | 85.6 s |
| Safety rollback | Restart request to second healthy poll | 39.9 s | 49.9 s | 129.9 s |

Only 17 successful attempts took more than 50 seconds to reach the second healthy poll. Eight took
more than 60 seconds, and two safety attempts took more than 90 seconds. The dominant issue is not
an abnormally slow reboot tail; it is the number of otherwise normal 30-to-40-second restart cycles.

## Mining-Time Impact

The database directly measures the interval from restart request to the second consecutive safe
positive-hash poll. Summed across successful attempts:

| Miner | Observed miner-hours | Restart to second healthy poll | Share of observed time |
|---|---:|---:|---:|
| `miner-01` | 48.22 h | 3.97 h | 8.23% |
| `miner-02` | 48.37 h | 2.70 h | 5.58% |
| Combined | 96.59 h | 6.67 h | 6.90% |

This 6.90% is a conservative availability exposure, not an exact zero-hash percentage. The second
healthy poll occurs approximately ten seconds after the first positive-hash poll, and hashing can
resume before the first observation.

For successful attempts:

- restart request to reboot proof totals 3.73 miner-hours;
- restart request to the inferred first positive poll totals 4.99 miner-hours; and
- failed or superseded restarted attempts add another 0.50 miner-hour without confirmed healthy
  mining in their own lifecycle.

Reboot proof does not prove that hashing was zero until that instant, while the first positive poll
proves that hashing had resumed by that instant. These observations support a practical estimate of
roughly 4% to 6% lost potential mining output from restart interruption alone. Weighting miner-time
by the final persisted best hash rates produces a similar estimate: approximately 4.1% to 5.5%,
with failed or superseded attempts contributing up to another 0.4% of uncertainty.

This estimate intentionally excludes:

- underperformance while evaluating a no-gain or unstable point;
- share variance;
- any external or manual restart;
- pool or network downtime unrelated to the controller; and
- the configured ramp after mining is already positive.

The total optimization cost is therefore higher than the restart-only estimate.

## Mechanisms That Can Sustain the Cycle

The current implementation contains several behaviors that are individually understandable but can
combine into an indefinite loop:

1. A rejected or unsafe point receives a two-hour retry time rather than a permanent or
   session-scoped exclusion.
2. A 48-hour process gives the same point many opportunities to become eligible again.
3. Point history is canonical per frequency/voltage pair; a new evaluation replaces the previous
   status and retry time for that pair.
4. After ordinary safety rollback, the controller can return to baseline and resume exploration once
   the immediate ramp/window conditions allow it.
5. The search retains no durable concept of a completed frontier, settled operating point, minimum
   dwell period, restart budget, or evidence that environmental conditions changed enough to justify
   retrying a thermal region.
6. Safety checks correctly run during ramp and evaluation, so an aggressive candidate can generate a
   second restart before the original attempt reaches two healthy polls.

This RFC does not claim that any one mechanism is the sole root cause. The observed repeated
transitions are the system-level result of their interaction.

## Why Safety Must Remain Separate

The proposed response must not reduce the number of restarts by suppressing safety actions. The data
shows that the protections are detecting real hard-limit conditions and returning miners to safer
points reliably.

The two restart categories have different meanings:

- a normal restart spends mining availability to seek higher sustained hash rate;
- a safety restart spends mining availability to contain a point that has already failed a hard
  constraint.

Only normal exploration is eligible for convergence, pacing, retry, or budget changes. Safety
rollback, host containment, and firmware-overheat recovery must remain immediate and exempt from any
normal restart limit.

## Problem Statement

Bitagnis lacks a long-term convergence contract for normal operating-point optimization.

The current controller can discover and persist safe high-performing points, but it does not have a
durable stopping condition that prevents repeated exploration of previously rejected regions. Over a
long-lived process, point retry eligibility reopens the frontier and produces a sustained cycle of:

```text
safe point
    -> restart into candidate
    -> ramp
    -> one evaluation window or immediate hard-limit observation
    -> rollback or next candidate restart
    -> retry similar region later
```

In the analyzed run this cycle caused 660 restarts in 48.4 hours, at least 160 hard-limit rollbacks,
and an estimated 4% to 6% restart-related loss before candidate underperformance is considered. This
is not an acceptable steady state for an optimizer whose objective is sustained actual hash rate.

## Solution Requirements

The proposed solution must demonstrate all of the following without choosing weaker safety
thresholds:

- Safety rollback and emergency recovery retain their existing priority and complete lifecycle.
- Every automated target remains a complete AxeOS-advertised frequency/voltage pair.
- The controller reaches a measurable settled state after bounded normal exploration.
- Known thermal, unstable, and no-gain regions are not retried indefinitely merely because wall time
  elapsed.
- Any retry has explicit new evidence or a deliberate operator/policy trigger.
- Normal restart frequency can be bounded and reported independently from safety restart frequency.
- Material performance-winner admission uses sustained actual hash and requires the candidate to
  repay its own entry/ramp cost over the declared horizon; final selection uses safe sustained actual hash, while rejected
  trials, returns, and final-placement cost are measured in the multi-day treatment result rather
  than falsely claimed as a pass-level optimum. Peak or expected hash alone never wins.
- Crash recovery retains one durable mutation authority and does not abandon pending safety work.
- Evaluated history remains available across ordinary restarts.
- No raw telemetry or credentials are added to durable state.
- Tests cover convergence, process reopen, repeated candidate eligibility, safety supersession, and
  the boundary between normal restart pacing and immediate safety action.

The proposed policy must also state how success will be evaluated over a multi-day run. A short unit
test or one completed sweep is insufficient evidence for a long-term convergence policy.

## Suggested Evaluation Metrics

These metrics describe the problem without prescribing implementation:

- normal restart requests per miner-hour;
- safety restarts per miner-hour;
- median and p95 restart-to-first-positive-hash duration;
- fraction of miner-time attributed to restart recovery;
- distinct targets versus repeated attempts per target;
- time continuously held at the selected best safe point;
- net actual hash produced during exploration versus a stable-point counterfactual;
- number of normal attempts superseded by safety before healthy mining;
- number of rejected points retried without changed environmental evidence; and
- controller state after 24, 48, and 168 hours.

The schema-v3 mutation history already supports most restart metrics. Exact exploration opportunity
cost may require an additional bounded evaluated-window history or another summary that does not
overwrite every prior result, but that is a separate design decision.

## Non-Goals

- Weakening ASIC, VR-temperature, power, host-cutoff, or firmware-overheat protections.
- Delaying a safety rollback to satisfy a restart budget.
- Treating expected hash as the optimization objective.
- Persisting raw polling history.
- Persisting Stratum credentials or sensitive mining identifiers.
- Automatically retrying rejected regions solely because time elapsed or current device temperature
  is lower.
- Adding multiple optimizer modes, compatibility paths, or feature flags around the selected policy.

## Evidence Uncertainties

### Exact zero-hash time

Choice or assumption: restart-related loss is reported as a practical range derived from reboot
proof, the ten-second polling interval, and the first/second positive-hash observations.

Why uncertain: AxeOS does not emit a continuous host-visible mining-active timestamp, and the
database intentionally does not persist raw polling history.

Consequence if wrong: actual restart-only loss may be somewhat below or above the 4% to 6% estimate,
although the directly measured 6.67 miner-hours to second healthy confirmation remains correct.

Resolution: validate one authorized canary with externally timestamped hash availability or persist
a bounded first-positive lifecycle timestamp with proven semantics.

### Safety trigger attribution

Choice or assumption: ASIC temperature caused most or all safety rollbacks.

Why uncertain: mutation history stores the durable kind but not the triggering thermal, power, or VR
dimension. The inference comes from latest point statuses and the large observed power/VR headroom.

Consequence if wrong: the proposed policy could address thermal retry while leaving another repeated
hard-limit cause unresolved.

Resolution: add a finite validated safety-reason enum to lifecycle summaries, without storing
raw telemetry or free-form errors.

### Trial-point opportunity cost

Choice or assumption: restart interruption is only part of the loss and weak candidate windows add a
material cost.

Why uncertain: `operating_points` retains only the latest evaluation per pair, so earlier window
hash summaries are overwritten.

Consequence if wrong: the total benefit of reducing exploration churn could be overestimated or
underestimated.

Resolution: compare a future multi-day run against a stable-point control using aggregate actual
hash and bounded, credential-free evaluation summaries.

### Environmental change

Choice or assumption: most repeated retries were not justified by a meaningful change in ambient
temperature, cooling, supply, or device condition.

Why uncertain: environmental conditions are not persisted, and no operator intervention log is
present.

Consequence if wrong: an overly permanent rejection policy could miss legitimate new headroom after
conditions improve.

Resolution: because no independent environmental signal exists, the selected policy requires an
explicit named `--retune`; live temperature alone never creates a new pass.

### External reboots

Choice or assumption: controller-owned attempts dominate restart-related loss in this run.

Why uncertain: schema v3 records controller-owned mutations, not manual power cycles or unrelated
device resets.

Consequence if wrong: total reboot loss is higher than reported, but controller churn is not lower.

Resolution: correlate uptime discontinuities with mutation history in a future observation run if
total device-reboot accounting is required.

## Firmware Ground Truth

This design was checked against the exact supported firmware tree, AxeOS tag `v2.8.1`, commit
`faae189017d3d18f11c14140887edc779826dcd3`, rather than inferred only from the HTTP API.

The firmware establishes the following constraints:

- `PATCH /api/system` writes `coreVoltage` and `frequency` as two independent NVS operations, in
  that order. The NVS setter returns no error to the handler, and the handler returns success after
  attempting both writes. HTTP success therefore does not prove that either or both writes stuck.
- Recovery adds a third independent write: mere presence of the `overheat_mode` JSON key makes the
  handler attempt `overheat_mode = 0`, regardless of the supplied value. Bitagnis must send a typed
  value of zero, but success proves none of the three writes; configured and post-boot verification
  must independently require the exact pair and `overheat_mode == 0`.
- The power-management task applies voltage first and frequency second, then delays 1,800 ms after
  completing its loop. It does not run at a fixed 1,800 ms period. BM1370 frequency changes ramp in
  6.25 MHz steps with a 100 ms delay per step, so a 400-to-625 MHz transition alone adds about
  3.6 seconds before the final delay. A complete JSON request is not an atomic hardware operation,
  and an intermediate or partially written pair can become active before restart.
- `GET /api/system/info` reports configured `coreVoltage` and `frequency` from NVS. It reports an
  actual measured core voltage but has no measured active-frequency field. Configured readback alone
  therefore cannot prove the running pair. Its temperature, VR-temperature, and power fields are
  cached values last updated by the power task, not fresh sensor reads performed by the GET.
- `POST /api/system/restart` sends its response, waits one second, and calls `esp_restart()`. A lost
  response is not proof that restart did not occur.
- `uptimeSeconds` is derived from a boot-local ESP timer. A discontinuity against a durable
  pre-restart observation is valid reboot evidence.
- The full firmware trip predicate is `(VR > 105°C OR ASIC > 75°C) AND
  (frequency_value > 50 MHz OR input_voltage > 1000 mV)`. When true, the code attempts to force the
  fan to 100%, disable VCORE, store `50 MHz / 1000 mV`, store manual fan mode at 100%, set the
  overheat flag, and call `exit(EXIT_FAILURE)`. The comparisons are strictly greater-than. Return
  values from the fan/VCORE/emergency NVS effects are not all checked, the NVS setter is void and
  log-only, and this tree contains no explicit `nvs_commit`; source alone therefore does not prove
  that every attempted effect persists. Because a new pair is read and applied later in the loop,
  its next thermal check can occur only after frequency ramp work, the 1.8-second delay, device I/O,
  and scheduler delay. The emergency pair is firmware state, not an advertised normal point, and
  must never enter optimizer history.
- The overheat flag is reported and displayed but is not a boot interlock: boot initializes VCORE
  from NVS even when the flag is set. When the attempted fan settings persist, current recovery
  clears the flag and restores the advertised minimum but deliberately leaves manual 100% fan mode;
  restoring auto-fan would be a separate safety design.
- The supported BM1370 grid is exactly six frequencies
  `{400, 490, 525, 550, 600, 625}` MHz by six voltages
  `{1000, 1060, 1100, 1150, 1200, 1250}` mV, with defaults `525 MHz / 1150 mV`. Board `601` selects
  this Gamma/BM1370 configuration.

These facts settle the recovery and pass-bound decisions below. In particular, no normal mutation
may be blindly retried after `patch_requested`, and no pre-restart configured readback may be treated
as active-pair proof.

## Proposed Solution

### Decision

Replace the two-hour retry loop with one finite thermal-frontier pass per miner. Every complete pair
can be admitted at most once in a pass, and row presence—not elapsed time—consumes the pair. Every
completed or aborted outcome is terminal for that pass. When no unseen eligible point remains, the
miner enters durable `HOLD` and normal operating-point mutations stop.

Elapsed time, process restart, cooldown expiry, and lower live temperature never reopen a pair.
Only the explicit named `--retune` operator action starts another pass. There is no periodic retune,
environment epoch, replenishing budget, legacy retry reader, or compatibility mode.

The pass retains the existing low-to-high frontier ordering:

1. Validate the current advertised complete pair as the incumbent.
2. Try the next lower voltage at the incumbent frequency while it preserves actual hash and improves
   headroom.
3. Try each higher frequency first at its minimum advertised voltage.
4. Try the next voltage at that frequency only when the preceding voltage response is useful.
5. Stop at the first safety frontier or exhausted response and settle at the best feasible evaluated
   point.

Expected hash, attainment, ASIC error percentage, and share deltas remain diagnostics or quality
constraints. Expected hash never selects the winner and never vetoes a faster safe point.

Pass bootstrap is terminal and deterministic. New-miner creation and accepted retune atomically
insert the current live advertised pair as `entered`, with `entered_at = pass_started_at` and no
entry attempt. The pair must pass two consecutive complete baseline windows, each independently
safe and quality-healthy, before their conservative combined record becomes the first incumbent.
Failure of either completed window marks it `unstable`; failure to obtain both complete windows by
the durable evidence deadline marks it `unobservable`. Either outcome enters non-settled blocked
`HOLD`. An off-grid current pair enters the same blocked hold without a frontier row. Without a
validated incumbent the controller does not improvise an exploration fallback; hard safety still
uses the canonical advertised minimum when no validated rollback record qualifies.

### Supported Grid Contract

Automated tuning remains limited to firmware version `v2.8.1`, ASIC `BM1370`, board `601`, and pairs
advertised by the device. In addition, the normalized advertised frequency and voltage arrays and
their defaults must exactly match the firmware values above before any automated mutation is
authorized. These API values limit the supported write surface but cannot authenticate the firmware
commit; out-of-band provenance for commit `faae189017d3d18f11c14140887edc779826dcd3` is an operator
precondition for every miner authorized for mutation, with the named canary qualified first. A
normal-grid mismatch blocks normal optimization. Safety containment may still use `400 MHz / 1000 mV` only when
the same MAC still reports the supported version/model/board and currently advertises both canonical
minimum values. If even that cannot be established, enter mutation-free `OVERHEAT`, retain the fleet
block, and do not claim controller containment.

The grid is checked on startup, before every normal mutation, and after rediscovery. It freezes the
normal finite-pass bound at 36 pairs but does not prove firmware authenticity. Support for another
normal firmware grid is a deliberate new design, not a dynamically enlarged pass.

### Durable Phase Contract

Keep the existing `BASELINE`, `UNDERVOLT`, `FREQ_TEST`, `VOLT_TEST`, `HOLD`, `COOLDOWN`, and
`OVERHEAT` phases. Collapsing the three trial phases would require another durable trial-purpose
field or derived branching at every recovery site; retaining them gives each decision one canonical
representation with less cutover risk.

Their new exact meanings are:

- `BASELINE`: an active pass is collecting a fresh established window or applying the final best
  point.
- `UNDERVOLT`, `FREQ_TEST`, or `VOLT_TEST`: one isolated trial is entering, evaluating, or returning
  to its reserved incumbent. The phase states why the candidate exists.
- `HOLD`: normal optimization is terminal. It may be ramping or validating after manual adoption,
  but it never selects a candidate and never changes `PhaseStartedAt` merely because another window
  elapsed.
- `COOLDOWN`: safety recovery owns the miner, including its post-cooldown ramp and safe validation.
  Successful validation transitions directly to `HOLD`, never to `BASELINE`.
- `OVERHEAT`: the existing durable emergency episode and fleet safety block.

`HOLD` alone does not prove convergence. The derived `verifiedSettled` predicate requires
`phase == HOLD`, `hold_reason` of `optimized` or `safety`, nonzero `settled_at`, elapsed ramp, no
pending operating-point or mining obligation, no unfinished attempt, live pair equal to durable
current, and a complete safe latest sample. `optimized` additionally requires the current supported
pair to be the final fixed-band selection with a validated row. `safety` instead means the current
supported rollback/recovery point passed a fresh safe window; it may intentionally differ from the
historical maximum-hash `BestPoint` because a candidate-caused safety event ends the pass. Manual and
blocked holds are displayed separately and never count as optimizer settlement.

Firmware emergency evidence is classified before generic off-grid or manual-point handling. A
nonzero firmware flag or any configured frequency of 50 MHz—including `50/1000` and a partial
`50/<old voltage>` write—enters `OVERHEAT`, is never adopted, and is never persisted as current
frontier evidence. A lone 1000 mV value is not a sentinel because it is advertised normally. Once a
known trip has entered durable `OVERHEAT`, a flagless partial emergency readback does not clear the
episode; only the full typed recovery lifecycle can do so.

Manual complete-pair adoption still requires two consecutive observations. It is disabled in
`OVERHEAT`, `COOLDOWN`, a safety-derived `HOLD`, or while any safety reason, authority, or unfinished
safety attempt exists; emergency classification always wins. An eligible adoption resets samples
and starts a ramp plus one-window evidence deadline in `HOLD`; it does not start or reset a pass or
clear a safety episode. An off-grid manual point can be observed
and held but can never be an automated candidate, best point, fallback, rollback target, or
`operating_points` row. Post-adoption hold validation is safety observation and reporting only; it
does not overwrite a terminal frontier row or update the pass best.

### One Isolated Trial at a Time

A trial is admitted only after a complete safe established window. One SQLite transaction must:

- insert, never replace, an `entered` record for the candidate;
- store the exact advertised incumbent in the existing fallback fields;
- set the appropriate existing trial phase; and
- persist one typed pending `operating_point` intent for the complete candidate pair.

The `entered` row controls eligibility and is not hardware authority. Only the typed pending intent
authorizes the mutation coordinator. If the insert conflicts, the candidate is already consumed and
no request is created.

The state shapes are deterministic:

```text
entry pending:   trial phase, current == fallback, pending == candidate
candidate live:  trial phase, current != fallback, pending empty
return pending:  trial phase, current != fallback, pending == fallback
return complete: BASELINE, current == former fallback, pending/fallback empty
promotion:       BASELINE, current == candidate, pending/fallback empty
```

A losing or unstable completed first window is finalized and returned to the reserved incumbent
through the normal complete mutation lifecycle. Failure to obtain the required complete evidence by
the deadline finalizes it `unobservable` and performs the same return. A provisional candidate remains
active without another restart and must pass a second consecutive complete window. Both windows
must independently pass completeness, safety, quality, and the applicable performance or undervolt
predicate. Their one persisted summary is exactly:

```text
median_hash    = min(W1.median_hash, W2.median_hash)
expected_hash  = max(W1.expected_hash, W2.expected_hash)
attainment     = median_hash / expected_hash, or 0 when expected_hash == 0
mean_temp      = max(W1.mean_temp, W2.mean_temp)
p95_temp       = max(W1.p95_temp, W2.p95_temp)
p95_vr_temp    = max(W1.p95_vr_temp, W2.p95_vr_temp)
p95_power      = max(W1.p95_power, W2.p95_power)
error_percent  = nil only when both are nil; otherwise max of available values
share deltas   = checked sums that fit both uint64 and SQLite INTEGER
measured_at    = second-window completion time
```

An overflow or invalid aggregate makes the point `unobservable`; it is never wrapped or saturated.
A process restart discards the in-memory first window and requires two new complete windows; it
never turns one window into two durable confirmations.

A complete window is the configured count of consecutive successful scheduled observations for the
same point and state. Add the scheduled timestamp to each in-memory sample. A failed poll,
incomplete telemetry, point/state change, nonpositive time delta, or gap larger than one configured
metrics interval resets the partial window and any provisional first window. The first poll after a
process reopen starts new evidence. Safe telemetry collection may continue while the fleet-wide
normal-mutation gate blocks a decision; the gate delays state transition, not sampling.

Every evidence-collecting state persists one absolute `evidence_deadline_at`; process restart and
configuration reload never extend it. A state requiring two consecutive windows sets it to
`ramp_until + 4 * evaluationWindow`, and a state requiring one sets it to
`ramp_until + 2 * evaluationWindow`. Safety cooldown validation uses
`max(ramp_until, cooldown_until) + 2 * evaluationWindow`. The deadline is checked before accepting a
late sample. Expiry has these exact outcomes: baseline becomes `unobservable` and blocked `HOLD`;
candidate becomes `unobservable` and queues its reserved return; fresh-incumbent or final-placement
validation retains existing point evidence and enters blocked `HOLD`; manual validation becomes
manual non-settled `HOLD`; and safety validation remains visibly blocked in `COOLDOWN`. A pending
hardware mutation has no evidence deadline. Thus an incomplete observation resets evidence without
instantly rejecting a point, while intermittent telemetry cannot keep a pass active forever.

There is no candidate-to-candidate mutation. A rejected voltage response is first returned to the
incumbent; its terminal record may then justify the next voltage after one fresh incumbent window.
Promotion also resets samples and requires a fresh established window before another admission.
That fresh window gates current safety and exploration headroom; hash comparison uses the fixed
maximum-rate reference persisted on the candidate, so no ephemeral age-matched summary is needed
after a crash.
The next higher voltage at one frequency is useful exactly when it improves median actual hash by at
least 2% over the immediately lower-voltage record or reduces ASIC error percentage by at least one
percentage point when both error values are present. Otherwise that voltage sweep ends. An
`unobservable` trial returns and ends upward exploration. Any candidate safety failure terminates
the whole pass immediately, regardless of voltage or frequency.

The minimum-voltage row at a newly tested frequency is the one seed exception because it has no
lower-voltage response. If its completed result is `no_gain` or `unstable`, all safety/headroom
fields are complete, and the incumbent admission headroom predicate still passes, admit exactly the
next advertised voltage once. This permits an under-volted new frequency to demonstrate a response.
After that seed, every further voltage requires the adjacent-response rule above. A minimum row that
is `unobservable`, safety-failed, or lacks headroom never seeds another voltage.

A hard limit, firmware trip, firmware flag, or host cutoff supersedes the ordinary return
atomically, clears the trial fallback, and enters the existing typed safety path. A candidate-caused
safety action terminates the pass. After cooldown or overheat recovery, a fresh safe window leads to
`HOLD`; upward work does not resume without `--retune`.

At exhaustion, select the best point using the comparator below. If it is not active, apply it once
through the normal lifecycle, ramp, and collect one safe established window. Only then report
settled `HOLD`.

### Behavior When Temperature Rises Quickly

Bitagnis still evaluates instantaneous safety before optimization on every metrics poll, including
ramp, either confirmation window, return, `HOLD`, and mutation reconciliation. With current defaults
the exact host behavior is below. Here “instantaneous” means the latest complete firmware sample in
the GET response; the firmware does not synchronously sample sensors for the request.

- p95 ASIC temperature below 65°C may provide exploration headroom;
- p95 temperature at or above 65°C stops new upward exploration;
- instantaneous temperature above 66°C or window p95 above 66°C requests an immediate typed safety
  rollback;
- instantaneous temperature at or above 70°C requests exact-minimum host containment;
- power at or above 24 W and VR temperature at or above 97°C independently request rollback; and
- any firmware flag or telemetry above the known 75°C/105°C firmware trip boundary enters the
  existing overheat recovery or emergency hold before normal work.

An incomplete, non-finite, nonpositive safety sample or reported power fault is never interpreted as
cool headroom. It resets partial window evidence, blocks normal decisions for that poll, and cannot
complete configured verification, reboot proof, recovery, or settlement.

The ten-second host poll cannot guarantee that it observes 66°C or 70°C before a very fast transient
crosses 75°C. The firmware provides an independent local trip path, but not a 1.8-second worst-case
response guarantee: after applying a large frequency transition, the next check can be roughly
5.4 seconds later even before I/O and scheduling delay. When it does observe the trip, it attempts to
disable VCORE, force the fan, write the non-normal emergency pair and flag, and exit. On
rediscovery Bitagnis recognizes any successfully persisted flag or sentinel and waits for safe
recovery telemetry before applying and verifying the exact advertised minimum while clearing the
flag.

Do not add a rate-of-rise controller in the first implementation. A derivative inferred from
ten-second network samples is too noisy to replace threshold safety, and the firmware owns the local
trip. Candidate admission remains conservative and threshold based. A canary must measure real
thermal slew and the firmware loop's worst observed latency before fleet rollout.

Unsafe telemetry at the exact advertised minimum with no firmware flag remains a mutation-free
durable emergency hold. Neither the pass nor mutation reconciliation may replay PATCH/restart for
that condition.

### Behavior When Temperature Is Lower Than Expected

When auto-fan is enabled, AxeOS runs a fan PID whose default target is 60°C and refreshes that target
from NVS once per variable-duration power-task loop. After a firmware trip, the attempted emergency
settings select manual 100% fan and current recovery does not restore auto-fan. A cool observation
can therefore reflect either fan mode, low active load, ambient temperature, or a different
operating point; it is not independent evidence that a previously hot pair became safe.

During an active pass, one complete incumbent window may admit an unseen candidate only when all of
the following hold with current defaults:

```text
p95 ASIC temperature < 65°C
p95 board power       <= 22 W      (24 W limit minus 2 W headroom)
p95 VR temperature    <= 87.3°C    (90% of the 97°C limit)
median actual hash    > 0
ASIC error percentage <= 5%, when available
```

AxeOS exposes no ambient measurement and this design adds no expected-temperature model; the
controller responds only to complete observed windows. A known `no_gain`, `unstable`, `unobservable`,
`thermal`, `power`, or `vr_hot` pair remains consumed
even if the active point later runs cooler. In `HOLD`, cool telemetry produces no normal mutation.
After a real cooling, airflow, supply, or hardware change, the operator must explicitly request one
new finite pass. This is intentionally conservative because neither firmware nor Bitagnis exposes an
independent ambient or cooling-change signal.

### Balancing Actual Hash Rate and Temperature

The optimizer uses constrained lexicographic selection, not a weighted temperature/hash score.
Unsafe temperature can never be purchased with more hash.

1. A point must be an exact supported advertised pair with complete identity and safety telemetry.
2. Every instantaneous and window safety constraint must pass.
3. Mining quality must pass: positive actual hash and, when present, ASIC error percentage no higher
   than the configured limit.
4. `M` is the maximum conservative actual hash among the current pass's `validated` rows.
   `BestHashRate` always stores `M`; `BestPoint` stores a row whose rate is exactly `M`, using the
   headroom order below only for an exact maximum-rate tie. It never ratchets to a slightly slower
   point.
5. Trial admission freezes `R = M` in the candidate row. Every performance-candidate window must
   independently produce at least `1.02 * R`. Every same-frequency undervolt window must
   independently produce more than `0.98 * R` and beat the reference row by headroom. Other
   higher-frequency points inside the 2% band are `no_gain`, not cooler substitutes.
6. At final settlement, recompute `M` across all feasible validated rows once. The equivalence set is
   exactly `(0.98 * M, M]`. Choose within that fixed set by lower p95 ASIC temperature, lower p95
   power, lower p95 VR temperature, lower voltage, then lower frequency. Use strict comparison at
   each tie-break and retain the current selection only when every value is equal.

The boundaries are deliberate: `C >= 1.02 * R` is a material performance gain,
`C <= 0.98 * R` is materially worse, and only `0.98 * R < C < 1.02 * R` is
measurement-equivalent. Using the fixed maximum anchor prevents sequential undervolts such as
`1000 -> 981 -> 962` GH/s from each passing pairwise tolerance while losing more than 2% overall.

A performance candidate must also pass a conservative candidate-entry horizon margin. All units are
seconds and hash-per-second:

```text
C = min(C1, C2)
R = frozen maximum-hash reference at trial admission
H = (168 * time.Hour).Seconds()
D = (entry.mining_resumed_at - entry.patch_requested_at).Seconds()
    + configured_ramp_seconds

entry_margin = (C - R) * H - C * D
winner iff C >= 1.02 * R, 0 <= D < H, and entry_margin > 0
```

Starting `D` at PATCH authorization covers the firmware interval in which a partial pair can already
be applied; adding the full ramp conservatively assigns no useful candidate work before its first
window. The candidate's durable `entry_attempt_id` identifies the attempt directly. The second
healthy-poll timestamp deliberately overstates unavailable time.

This margin answers whether that candidate can repay its own entry and ramp over one week. With the
current 2% rule and lifecycle deadlines it is normally redundant, but it prevents a future larger
grid or deadline from silently making the same threshold uneconomic. It is not presented as total
pass profit. Rejected trials, returns, and final placement are measured once in the hourly treatment
work against the frozen baseline counterfactual; restart intervals classify that work and are not
subtracted again.

The margin applies only to a material performance winner. A same-frequency undervolt inside the
fixed 2% band is a measurement-equivalent headroom substitution; when its conservative hash is below
`R`, the RFC does not claim that it repays work in hash units. Its total effect remains visible in the
control-uplift acceptance result.

Temperature therefore has three roles only: hard feasibility, exploration headroom, and a tie-break
inside the measurement-equivalent band. The controller never tries to heat the ASIC up to 65°C.

### Exact Schema-Version-6 Contract

Schema version 6 replaces versions 3, 4, and 5. Opening any other nonzero version fails with an explicit
incompatible-schema error; there is no migration, dual reader, or silent reinterpretation.

`optimizer_miners` keeps the current canonical fields and adds:

- `pass_started_at INTEGER NOT NULL`;
- `pass_trigger TEXT NOT NULL`, exactly `initial` or `operator`;
- `pass_reference_hash REAL NOT NULL`, zero for an initial or non-validated-current pass until its
  two-window baseline validates; an optimized retune freezes the pre-reset selected row's positive
  conservative median in the reset transaction. Once positive it is frozen for the pass;
- `pass_reference_frequency INTEGER NOT NULL`, zero when no arm snapshot exists and otherwise the
  exact pre-reset selected frequency;
- `pass_reference_core_voltage INTEGER NOT NULL`, zero when no arm snapshot exists and otherwise the
  exact pre-reset selected core voltage; and
- `pass_reference_settled_at INTEGER NOT NULL`, zero when no arm snapshot exists and otherwise the
  durable settlement timestamp for that complete pre-reset boundary. The three point/snapshot fields
  are either all zero or a canonical, non-sentinel pair with a positive hash and a timestamp no later
  than the new `pass_started_at`;
- `safety_reason TEXT NOT NULL`, empty outside `COOLDOWN`, `OVERHEAT`, and a safety-derived `HOLD`,
  and otherwise one of the finite safety reasons below even while no mutation is actuatable;
- `hold_reason TEXT NOT NULL`, empty outside `HOLD` and exactly `optimized`, `safety`, `manual`, or
  `blocked` inside it;
- `settled_at INTEGER NOT NULL`, nonzero only after a complete safe post-ramp hold-validation window
  for `optimized`, `safety`, or `manual`;
- `evidence_deadline_at INTEGER NOT NULL`, using the existing UTC Unix-nanosecond timestamp encoding,
  zero outside active window collection and otherwise the absolute, nonextendable deadline defined
  above; and
- `accounted_through_at INTEGER NOT NULL`, UTC Unix nanoseconds using the store's existing timestamp
  encoding and initialized to miner creation time.

The current trial phases and fallback fields are the only durable trial-leg representation. Do not
add a second trial object, generation table, or serialized telemetry window.

`operating_points` removes `retry_after` and adds `entered_at INTEGER NOT NULL`,
`entry_attempt_id INTEGER NOT NULL`, and `reference_hash REAL NOT NULL`. Its exact status enum
becomes:

```text
entered, validated, no_gain, unstable, unobservable, thermal, power, vr_hot
```

For `entered`, `entered_at` is nonzero, `measured_at` is zero, and measurement fields are zero or
NULL. The baseline row has `entry_attempt_id = 0` and `reference_hash = 0`. A candidate initially has
`entry_attempt_id = 0` and `reference_hash = BestHashRate`; creating its entry attempt atomically
binds the generated positive attempt ID to that still-`entered` row. Returns and final placement do
not change that binding. Every terminal candidate status requires its positive entry attempt ID,
nonzero `entered_at`, and `measured_at >= entered_at`. Row presence consumes the pair. Only
`validated` rows with complete current headroom evidence may authorize a normal rollback; `entered`
and all rejected statuses never do.

`mutation_attempts` keeps its existing lifecycle fields and adds:

- `reason TEXT NOT NULL`, using the finite values below;
- `configured_verified_at INTEGER NOT NULL`;
- `configured_verified_uptime_seconds INTEGER NOT NULL`, `-1` until verified; and
- `first_positive_at INTEGER NOT NULL`.

`configured_verified_at` means that a post-PATCH GET returned the exact configured target and the
associated uptime was persisted. It does not mean the pair was active. `reboot_verified_at` remains
the first point at which a new boot plus exact post-boot configured readback proves that the new boot
reports the configured NVS pair. AxeOS has no physical active-frequency readback, so this is not a
direct measurement of the ASIC clock; subsequent safe positive-hash windows are the operational
evidence.
`first_positive_at` is reporting evidence only; `mining_resumed_at` remains the second consecutive
safe positive-hash poll and the control gate.

The failure-stage enum becomes exactly `preflight`, `configured_verification`,
`reboot_verification`, `mining_resume`, and `safety_superseded`. Delete `interrupted` because process
shutdown leaves an unfinished attempt to resume. Delete `completion` because a failed host
transaction leaves the same reboot-verified attempt open. Transport errors themselves are evidence
to reconcile, not terminal stages.

The reason enum is empty for ordinary and mining mutations and otherwise exactly:

```text
asic_limit, host_cutoff, firmware_overheat, firmware_trip,
power_limit, vr_limit, mutation_uncertain
```

`safety_reason` is the canonical episode cause. Its escalation order is
`mutation_uncertain < asic_limit/power_limit/vr_limit < host_cutoff < firmware_trip <
firmware_overheat`; causes at the same tier retain the first observation. A flag or 50 MHz sentinel
sets `firmware_overheat`; a strictly-above firmware-trip observation persists `firmware_trip` even
when no mutation is currently actuatable. `safety_reason` is required for `safety_rollback` and
`overheat_recovery`; each safety attempt copies its current value into immutable `reason`. Normal and
mining attempts require an empty reason. Clearing the typed intent never clears the episode reason;
successful safety-derived hold validation preserves it as the reason for that hold. New-miner
bootstrap initializes it empty; accepted retune clears it only after the no-episode
qualification above; manual adoption is eligible only when it is already empty.

Add and exact-schema-validate these partial unique indexes, including their predicates:

```sql
CREATE UNIQUE INDEX mutation_attempts_one_unfinished
ON mutation_attempts(mac_addr)
WHERE failed_at = 0 AND mining_resumed_at = 0;

CREATE UNIQUE INDEX operating_points_one_entry_attempt
ON operating_points(entry_attempt_id)
WHERE entry_attempt_id > 0;
```

`StartMutationAttempt` rejects an existing unfinished row; it never marks it interrupted or
supersedes it as a side effect. For a candidate entry it also validates
`entered_at == pending_since == intent_created_at`, MAC, kind, and exact target, then inserts the
attempt and binds its ID to the point row in the same transaction. Startup loads that same unfinished
row and resumes or atomically supersedes it.

The store exposes these complete transaction boundaries under the existing serialized connection:

- bootstrap/retune: reset pass state and insert the current supported pair as the baseline
  `entered` row, with the two-window evidence deadline based on the new ramp;
- trial admission: insert the candidate row and save phase, fallback, frozen reference, and pending
  intent;
- point decision: finalize the exact row and save promotion or ordinary return;
- mutation supersession: close the exact attempt, finalize any candidate evidence, and save the
  replacing typed safety or blocked state;
- mutation completion: load attempt and miner inside the transaction, derive the one valid
  post-state with zero evidence deadline while healthy mining remains unconfirmed, and save both
  idempotently;
- healthy resumption: persist `mining_resumed_at`, set the phase-appropriate ramp and exact one- or
  two-window evidence deadline, and save the miner state atomically; and
- retune: revalidate and reset the one explicitly named miner atomically.

Mutation completion does not trust a caller-supplied stale miner copy. If `completed_at` is already
set, it verifies that stored miner state has the derived completed shape and returns success. A lost
successful COMMIT can therefore be retried without hardware or a false failure row.

Do not preserve independent `SavePoint` then `SaveMiner` call sequences at these boundaries. A crash
must see either side of each complete state transition, never half of it.

For reporting, add `optimizer_hourly` with this exact application-owned shape:

```sql
CREATE TABLE optimizer_hourly (
    mac_addr TEXT NOT NULL,
    hour_started_at INTEGER NOT NULL,
    observed_duration_nanos INTEGER NOT NULL,
    unknown_gap_duration_nanos INTEGER NOT NULL,
    actual_hash_seconds REAL NOT NULL,
    trial_actual_hash_seconds REAL NOT NULL,
    incumbent_counterfactual_hash_seconds REAL NOT NULL,
    settled_duration_nanos INTEGER NOT NULL,
    trial_duration_nanos INTEGER NOT NULL,
    PRIMARY KEY (mac_addr, hour_started_at)
);
```

`hour_started_at` must be a positive UTC Unix-second value aligned to an hour. Duration columns are
nonnegative integer nanoseconds and hash-work columns are finite, nonnegative REAL values. This
table has no optimizer-authority fields. Retain 384 UTC hours so two 168-hour crossover arms plus
transition fit.
`optimizer_miners.accounted_through_at` is advanced atomically with all affected hourly fragments;
it prevents additive replay after an ambiguous commit or reopen. These fields are credential-free
reporting state and never authorize a candidate or rollback; raw telemetry remains in memory.
Only the hourly compare-and-set transaction may update this cursor. General miner-state saves ignore
the caller's cursor copy and can neither overwrite nor regress it.

Exact schema validation includes tables, columns, primary keys, both partial indexes and predicates,
and the expected application indexes; missing, altered, or unexpected application objects are
rejected.
The reopen validator also checks the cross-table phase/entry/return shapes, attempt-to-intent and
directional entry-attempt links, one unfinished row per MAC, configured-verification sentinels,
safety reasons, completed-but-unresumed state, the zero/nonzero evidence-deadline shape, and
exclusion of every
firmware emergency sentinel from frontier evidence. Promotion, return completion, final placement,
manual adoption, and safety recovery set the deadline in the same transaction that enters window
collection. Settlement, blocked expiry, pending hardware, and accepted retune clear or replace it in
that same transition; no general miner save may recompute or extend it.

Every positive current `operating_points.entry_attempt_id` must identify exactly one
`operating_point` attempt with the same ID, MAC, target, and
`intent_created_at == entered_at`. A historical attempt need not retain a point row after retune.
`entry_attempt_id == 0` is allowed only for the baseline or for an `entered` candidate whose attempt
has not yet been atomically bound; every terminal candidate requires a positive ID. The partial
unique index prevents one entry attempt from authorizing multiple rows.

### Exact Mutation Reconciliation

Every mutation kind uses one durable attempt and the ordered milestones below. A process shutdown
leaves the row unfinished; startup loads it before creating any work.

```text
started
    -> patch_requested
    -> configured_verified
    -> restart_requested
    -> reboot_verified
    -> completed
    -> first_positive
    -> mining_resumed
```

`configured_verified_uptime_seconds == -1` exactly when `configured_verified_at == 0`; otherwise it
is nonnegative. Repeating a milestone is idempotent only when the existing timestamp/value agrees.
`first_positive_at` is the immutable earliest safe positive observation after completion. An
unhealthy poll resets the in-memory consecutive count but not that timestamp. Reopen also resets the
count and requires two new consecutive polls. The three-minute deadline is checked before accepting
a late poll, and persisted ordering requires
`completed_at <= first_positive_at <= mining_resumed_at` when all are nonzero. Zero means no such
observation: positive `first_positive_at` requires positive `completed_at`, positive
`mining_resumed_at` requires positive `first_positive_at`, and `first_positive_at` may remain positive
while `mining_resumed_at == 0`.

The verification predicates are exact:

- `normal-safe` means same MAC, supported safety identity, complete finite positive ASIC/VR/power
  telemetry, no power fault, no firmware flag or 50 MHz sentinel, ASIC temperature at or below
  `tempLimit`, power strictly below `maxPower`, and VR temperature strictly below `vrTempHigh`;
- `recovery-thermal-safe` independently means the common identity and complete finite telemetry, no
  power fault, ASIC temperature at or below `recoveryTemp`, power at or below `maxPower - 2 W`, and
  VR temperature at or below `0.9 * vrTempHigh`. It deliberately permits the pre-existing flag or
  50 MHz sentinel during overheat-recovery preflight; and
- `safety-continuable` always requires the common identity, complete telemetry, no power fault, no
  flag/sentinel, and no strictly-above firmware-trip observation. For `mutation_uncertain` it also
  requires `normal-safe`; for `firmware_trip` it requires `recovery-thermal-safe`. For `asic_limit`,
  `power_limit`, `vr_limit`, or `host_cutoff`, residual ordinary-limit telemetry is allowed because
  the previously reboot-verified source can remain active until restart. This remains true when an
  unresolved containment PATCH makes NVS report minimum while durable current is still a higher
  source, so configured verification may be recorded and the same attempt may issue its one restart.
  That restart completes the already-issued PATCH lifecycle; it is not a new same-pair attempt or a
  replay. Unsafe telemetry when durable current is already canonical minimum and no post-PATCH
  operation is unresolved is mutation-free `OVERHEAT`. An escalation that changes kind or target is
  arbitrated before the lifecycle continues.

Configured verification is then kind-specific:

- `operating_point`: exact complete configured target plus `normal-safe`;
- `safety_rollback`: exact complete configured target plus `safety-continuable`; configured target
  equality never proves that target active;
- `overheat_recovery`: exact canonical advertised minimum, `overheat_mode == 0`, no 50 MHz
  sentinel, and `recovery-thermal-safe`; and
- `mining_configuration`: unchanged complete operating point, exact readable non-secret pool
  host/port/user fields, and `normal-safe`. Passwords are write-only and are never read back or
  persisted.

The configured milestone never proves active frequency. Reboot verification additionally requires a
new boot and the same kind-specific predicate; it proves only that the new boot reports the configured
NVS pair, not a measured ASIC clock. In the same controller process, reboot proof uses monotonic
elapsed time and the existing tolerance. After reopen, wall-clock elapsed is never used; proof
requires device uptime to regress by more than Bitagnis' existing conservative five-second
reboot-proof tolerance from the
persisted configured-verification uptime. Ambiguous low uptimes do not pass.

#### Normal operating-point stages

The existing phase/fallback shape identifies the purpose without another type:

```text
entry:           trial phase, current == fallback, pending != fallback
reserved return: trial phase, current != fallback, pending == fallback
final placement: BASELINE, fallback empty, pending target set
```

| Durable stage | Exact behavior |
|---|---|
| No `patch_requested` | Read-only preflight may repeat in the same attempt. Entry has an absolute two-minute deadline from `started_at`; expiry atomically closes `preflight`, marks the candidate `unobservable`, clears intent/fallback, and restores `BASELINE` at the unchanged incumbent. Return or final placement never clears its obligation on transient unavailability; it remains visibly blocked in the same attempt. A safe external pair change remains observational until the normal second consecutive confirmation. Before confirmation the same attempt stays unfinished and no request is made. On confirmation, entry atomically closes `preflight`, marks its candidate `unobservable`, clears intent/fallback, adopts the external point, and enters manual `HOLD`; return/final placement performs the same transition while retaining its already-terminal point evidence. Unsafe evidence goes through safety arbitration. Persistence failure leaves the attempt open. |
| `patch_requested`, no `configured_verified` | Never re-PATCH the normal target. On the first complete same-MAC read, exact target plus `normal-safe` persists configured verification immediately. A mixed, old, off-grid, flagged, or unsafe read arbitrates safety immediately; it is not allowed to “settle.” Only absence of a readable same-MAC response waits until the absolute `patch_requested_at + 2 minutes` deadline. Every such post-PATCH arbitration atomically closes `safety_superseded`, preserves no normal hardware authority, enters the strongest durable safety state (using `mutation_uncertain` when no stronger cause exists), and authorizes exact-minimum containment only if same-MAC supported safety identity and canonical minimum can first be revalidated; otherwise it retains a mutation-free fleet block. |
| `configured_verified`, no `restart_requested` | Revalidate same MAC, exact target, supported safety identity, and `normal-safe`. On success persist `restart_requested`, then POST exactly once. Any readable wrong, off-grid, flagged, or unsafe result atomically closes `safety_superseded` and performs the same strongest-cause or `mutation_uncertain` arbitration as the preceding row. Only unavailability waits until `configured_verified_at + 2 minutes`; at expiry perform that same arbitration and do not restart blindly. |
| `restart_requested`, no `reboot_verified` | Wait even when POST returned an error, because firmware may already be restarting. A proven new boot with exact `normal-safe` readback persists reboot verification. Wrong, flagged, or unsafe readback arbitrates safety immediately. Only absence or ambiguous reboot proof waits until `restart_requested_at + 2 minutes`; at expiry atomically close `safety_superseded` and perform the same durable `mutation_uncertain` exact-minimum-or-mutation-free arbitration. Never issue a second normal PATCH or restart. |
| `reboot_verified`, no `completed_at` | Retry only the store-derived idempotent completion transaction. Optional network reads are observational. Never mark a completion persistence error terminal and never touch hardware again for this attempt. |
| `completed_at`, no `mining_resumed_at` | Require two consecutive safe positive polls by `completed_at + 3 minutes`. Candidate failure atomically closes `mining_resume`, marks it `unstable`, and queues its one reserved ordinary return. Return or final-placement failure closes `mining_resume` and enters `HOLD/blocked` with `settled_at = 0`; exact safe boot proof means another hardware write is not justified. |
| `mining_resumed_at` | Close the lifecycle and begin the phase-appropriate ramp/window. |

Safety arbitration always preserves the strongest observed cause:

- firmware flag or 50 MHz sentinel: `OVERHEAT` plus typed `overheat_recovery` only after recovery
  telemetry is safe;
- telemetry strictly above a known 75°C ASIC or 105°C VR firmware-trip boundary: mutation-free
  `OVERHEAT`, because firmware may already be executing a partial emergency sequence;
- telemetry at the host cutoff but not above a known firmware-trip boundary: `OVERHEAT` and
  exact-minimum containment when its supported advertised minimum can be validated and the reported
  configured pair is not already that minimum;
- ordinary ASIC, power, or VR limit: `COOLDOWN` plus the existing strictly de-escalating validated
  rollback, or canonical advertised minimum when no record qualifies; if the reported configured
  pair is already that minimum, use mutation-free `OVERHEAT` instead;
- no stronger cause after an ambiguous normal PATCH/restart: `mutation_uncertain` exact-minimum
  safety containment; and
- unsupported identity or an unadvertised canonical minimum: mutation-free `OVERHEAT` with no
  actuatable pending intent until same-MAC supported safety identity and minimum are revalidated.

No new safety attempt may be created for unsafe telemetry when GET already reports configured
canonical minimum with no flag. This is mutation-free `OVERHEAT`, irrespective of durable current,
and it does not claim the minimum physically active. An attempt that already issued PATCH may finish
its configured verification and issue that lifecycle's first, recorded restart while durable current
still names the hotter source; after the milestone, it only observes that restart. It never
authorizes a repeat request or a new unsafe same-pair attempt.

Durable `safety_reason = firmware_overheat` survives later loss of the flag and sentinel. Once
telemetry becomes `recovery-thermal-safe`, that reason always selects the complete
`overheat_recovery` lifecycle—even if current readback is flagless and already reports configured
minimum—because only its PATCH/flag-clear/reboot/readback/health proof may clear the episode. A
flagless `firmware_trip` episode likewise persists while hot and creates no hardware authority. Once
recovery-thermal-safe, it selects exact-minimum `safety_rollback(reason=firmware_trip)` when durable
current is non-minimum, including the safe same-configured-pair lifecycle when NVS already reports
minimum. A durable current already at minimum enters `COOLDOWN` without a write. Unsafe telemetry at
configured minimum remains mutation-free `OVERHEAT` when no request is already in flight; an issued
restart is only observed. Configured pair equality alone never proves active containment and never
clears either episode.

The supersession transaction closes the exact normal attempt as `safety_superseded`, finalizes the
candidate with the strongest observed status or `unobservable`, clears the ordinary fallback, and
persists only the selected typed safety authority. It never authorizes an old `entered` row.

If unresolved normal post-PATCH ambiguity reports the exact canonical minimum, the same MAC and
supported safety identity/minimum are valid, there is no flag/sentinel, and the latest complete
sample is safe, the coordinator must create exactly one separately recorded
`safety_rollback(reason=mutation_uncertain)` lifecycle. Its preflight explicitly permits the same
configured pair, PATCHes the complete minimum once, verifies configured minimum again, records and
issues one restart, and proves boot/readback/health. Unsafe telemetry at configured minimum remains
subject to the strongest safety cause, forbids creation of a new same-pair attempt, and remains
mutation-free `OVERHEAT` unless an already-requested restart is merely being observed. Safe telemetry
remains mandatory for the narrow `mutation_uncertain` same-pair exception.

#### Safety and overheat-recovery stages

Safety arbitration is immediate; a recovery PATCH may still wait for safe telemetry. Safety work is
never abandoned in favor of normal work and is exempt from normal pass limits.

| Durable stage | Exact behavior |
|---|---|
| No `patch_requested` | Retry read-only identity, canonical-minimum, and kind-specific preflight in the same attempt only after a typed intent is actuatable. `overheat_recovery` and flagless `firmware_trip` containment wait for `recovery-thermal-safe`; `mutation_uncertain` waits for `normal-safe`; ordinary rollback/host containment require `safety-continuable`. Unsafe telemetry with reported configured canonical minimum always closes the nonissued attempt at `preflight`, clears actuatable authority, and remains mutation-free `OVERHEAT`, regardless of durable current. Unsupported identity/minimum retains a mutation-free fleet block and no containment claim. |
| `patch_requested`, no `configured_verified` | PATCH is never repeated in this attempt. Exact target plus the kind-specific predicate persists configured verification even when an ordinary rollback's source-limit telemetry remains. Configured minimum with durable current still above minimum is an in-flight readback, not the no-replay exception. A same-obligation wrong/mixed pair closes `configured_verification` and retains the episode reason and typed obligation for a new recorded attempt; for `overheat_recovery`, an uncleared flag is this same-obligation flag-clear failure. A new flag during any other kind, a firmware trip, or another stronger cause closes `safety_superseded`, atomically persists the stronger reason, and replaces or clears authority according to the arbitration above. Only unavailability waits to `patch_requested_at + 2 minutes`; expiry closes `configured_verification`, retains the reason and fleet block, and does not claim containment. |
| `configured_verified`, no `restart_requested` | Perform one final same-MAC kind-specific check. If exact and predicate-valid, persist the restart milestone and issue the attempt's one restart. Configured canonical minimum plus residual ordinary-limit telemetry is allowed only while durable current still names the higher source; this completes the in-flight lifecycle and never permits a second restart. If durable current is already canonical minimum, unsafe telemetry closes `safety_superseded`, clears actuatable authority, and remains mutation-free `OVERHEAT`. A same-obligation mismatch—including a reappeared flag during `overheat_recovery`—closes `configured_verification`; another stronger cause closes `safety_superseded` and atomically replaces or clears authority. Unavailability waits no longer than two minutes from configured verification, then closes `configured_verification`, retains the reason and fleet block, and does not infer or issue a restart. |
| `restart_requested`, no `reboot_verified` | Never repeat PATCH/restart in this attempt. A proven new boot with exact target and the same kind-specific predicate persists reboot verification; residual ordinary-limit telemetry is allowed while durable current still identifies the higher source and is re-arbitrated immediately after completion. The durable-current-minimum unsafe exception closes `safety_superseded`, clears actuatable authority, and remains mutation-free `OVERHEAT`; configured readback alone does not. A post-boot wrong pair, or an uncleared flag for `overheat_recovery`, closes `reboot_verification` and retains the typed obligation. Another stronger cause closes `safety_superseded` and atomically replaces or clears authority. Absence or ambiguous proof waits to `restart_requested_at + 2 minutes`, then closes `reboot_verification`, retains the reason and fleet block, and does not claim containment. |
| `reboot_verified`, no `completed_at` | Retry only idempotent host completion. |
| `completed_at`, no `mining_resumed_at` | Require two new safe positive polls. On deadline, close `mining_resume`, clear the completed pending authority, and remain visibly blocked in `COOLDOWN` with no automatic hardware replay. Safe zero hash is not an overheat episode. |

An overheat episode is not cleared by target-pair equality alone. Exact minimum, cleared flag when
applicable, proven reboot, and the applicable safe completion predicate move canonical phase from
`OVERHEAT` to `COOLDOWN`; the unfinished attempt and fleet gate remain blocking. Recovery is not
reported healthy or closed until two new safe positive polls set `mining_resumed_at`. Health-deadline
failure remains visibly blocked in `COOLDOWN`. The controller tolerates disappearance, continued
execution, or reboot after firmware `exit(EXIT_FAILURE)` because the source does not establish which
runtime outcome occurs.

#### Mining-configuration stages

Mining mutation uses the same one-attempt ordering and kind-specific readable verification above.
Before PATCH, transient password-source or device errors leave the same obligation visibly blocked;
no secret enters SQLite or logs. After PATCH, a response error is reconciled by readable fields. The
first complete same-MAC readable mismatch closes `configured_verification` immediately; only
unavailability waits until `patch_requested_at + 2 minutes`. Missing boot proof at
`restart_requested_at + 2 minutes` closes `reboot_verification`; failure of two safe primary
positive-hash polls by the health deadline closes `mining_resume`. In each terminal case
`MiningPending` remains true, startup is blocked, and no new mining attempt is created automatically
in the same launch. A later attempt requires a new explicit named `--reapply-mining` authorization.
On reopen, `MiningPending` with no unfinished row and a latest failed mining attempt remains the same
blocked state; only a matching explicit authorization in the current launch may create its next
attempt.

Emergency safety may atomically close an unfinished mining attempt as `safety_superseded` while
preserving `MiningPending`; safety completes first and mining remains operator-visible. Process
shutdown never closes a mining attempt. Password readback is impossible, so healthy primary mining
after proven reboot is the final operational evidence, not proof of the stored secret bytes.

### Explicit Retune Contract

The operator command is:

```sh
./bitagnis --retune bitaxe-example
```

`--retune` requires exactly one explicit hostname, rejects `all`, and is mutually exclusive with
`--reapply-mining`. The single-miner contract avoids a partially accepted fleet reset and matches the
one-canary rollout rule. It starts the normal long-running controller and holds one in-memory retune
request; it is never persisted as a recurring obligation.

Qualification ends at the earlier of the named miner's second consecutive startup-health poll or
three minutes after its first successful discovery. Safety, overheat recovery, an already-durable
hardware attempt, and startup mining reconciliation retain priority during that interval and may
legitimately issue their own requests; `--retune` never suppresses them. The reset is accepted only
when all of these are true in the same qualifying observation:

- exact named discovery maps the hostname to one MAC;
- phase is `HOLD`;
- `hold_reason` is not `blocked` and `settled_at` is nonzero;
- no pending operating-point or mining obligation exists;
- no unfinished mutation attempt exists;
- cooldown has expired and there is no overheat episode or firmware flag;
- live pair equals durable current pair and is on the exact supported grid; and
- identity and all instantaneous safety telemetry are complete and safe.

If any condition fails or the deadline expires, log one credential-free refusal, clear the in-memory
retune request, change no pass state, and continue the controller's safety and monitoring duties. It
is never applied hours later. “No hardware effect” applies to the retune reset itself, not to higher
priority startup safety or mining reconciliation that may already have been required.

Under coordinator serialization, the accepted transaction rechecks every condition and the unchanged
current pair; deletes that miner's `operating_points` rows; clears best, fallback, pending,
observation, bad-window, hold, settlement, safety-reason, and evidence-deadline fields;
preserves current point,
MAC/hostname/IP, overheat count, expired cooldown history, hourly accounting cursor, all mutation
attempts, and mining state; sets `pass_started_at` and `phase_started_at` to now;
sets `pass_trigger = operator`, `phase = BASELINE`, and
`ramp_until = now + configured ramp` and
`evidence_deadline_at = ramp_until + 4 * evaluationWindow`; and inserts the current supported pair
as the new baseline `entered` row. Before deleting the old rows it copies the current selected row's
positive conservative median into `pass_reference_hash` only when `hold_reason = optimized` and the
row is the verified final selection; safety/manual retunes set zero for the new baseline to
establish. It then resets in-memory samples. The reset issues no PATCH or restart.

New miners use the same state with `pass_trigger = initial`. Prior point summaries are deliberately
deleted on retune rather than retained behind a generation key. Mutation history and bounded hourly
aggregates are the audit record; old point rows must never remain available as current rollback or
candidate authority.

### Structural Restart and Time Bound

For `P` complete pairs, let `N <= P - 1` be candidate admissions. A fault-free pass has at most:

```text
N candidate-entry normal restarts
  + N reserved-return normal restarts, with no final move
  or at most (N - 1) returns + 1 final move
  = at most 2N <= 2P - 2
```

The current advertised baseline consumes one pair without an entry restart. The exact supported grid
therefore has a tight structural bound of 70 normal restarts per initial or operator-triggered pass.
Safety arbitration is immediate and its eventual restarts are excluded. Row insertion before intent
creation and the one-unfinished-attempt index make the bound survive process crashes.

No additional trial cap is added. Counting every bounded successful stage conservatively, one
maximally expensive candidate uses at most two minutes preflight, two minutes configured-readback
reconciliation, two minutes final pre-restart revalidation, two minutes reboot proof, three minutes
health confirmation, one minute ramp, two five-minute candidate windows, the same eleven-minute
mutation allowance for return, one minute return ramp, and one five-minute incumbent window. The
durable evidence slack raises those two candidate windows to a worst-case 20 minutes and the
incumbent window to 10 minutes, for 54 minutes per candidate. At most 35 candidates, a worst-case
21-minute bootstrap, and an optional 22-minute final move therefore fit inside approximately
32 hours 13 minutes for one uncontended miner when every temporarily unavailable stage recovers by
its stated deadline. Frontier pruning and normally immediate HTTP calls should make the realized
cost much lower.

Network, device, or persistence faults can extend wall time, but cannot create more normal trials.
During such a fault the state is visibly reconciling or blocked, never falsely settled. After
verified `HOLD`, the normal operating-point restart rate is exactly zero until `--retune`. The
70-restart bound is per miner; fleet-wide normal-mutation serialization can extend wall time, so the
48-hour acceptance gate is measured on one authorized treatment canary, not claimed as a fleet
makespan bound.

### Multi-Day Measurement

`mutation_attempts` remains the canonical restart source. Hourly buckets are UTC half-open intervals
`[hour, hour + 1h)`, and an interval crossing an hour is split. A new miner initializes
`accounted_through_at` to its creation time. The first poll after launch or reopen establishes an
in-memory sample and credits the positive interval since the durable cursor as unknown. If the
cursor precedes the 384-hour retention horizon, the transaction materializes only the retained
portion as unknown and advances across the older, deliberately expired portion without creating
rows. Thereafter:

- two consecutive successful scheduled polls with `0 < delta <= metricsInterval` credit
  `current.HashRate * delta` to actual work;
- a successful poll without a compatible immediately preceding successful poll credits the positive
  cursor-to-poll interval as unknown, then becomes the new predecessor;
- a failed poll or invalid/negative hash credits the positive interval as unknown and clears the
  predecessor;
- a successful point/state discontinuity or larger positive gap credits the whole interval as
  unknown, then makes the current poll the new predecessor;
- all bucket fragments and `accounted_through_at` advance in one transaction, including unknown
  intervals; and
- buckets whose end is no later than `now - 384 hours` are deleted only after that transaction
  succeeds.

When wall time is not strictly later than `accounted_through_at`, persist no fragment, never move
the cursor backward, and reset the in-memory predecessor. The first later observation credits the
positive interval from the unchanged cursor as unknown. This is the complete clock-rollback rule;
negative or duplicate time is never represented as seconds.

An hourly transaction uses the loaded cursor as its compare-and-set precondition. On a definite or
ambiguous store error, discard the in-memory predecessor and reload the cursor before accounting
again; a committed cursor suppresses replay, while an unchanged cursor makes the elapsed interval
unknown rather than double-counting actual work.

`trial_duration_nanos` means candidate-live time: a trial phase with `current != fallback`. Entry preflight
while the incumbent remains active is not trial time. `trial_actual_hash_seconds` and
`incumbent_counterfactual_hash_seconds` cover those same observed seconds, using the candidate row's
frozen `reference_hash`. `settled_duration_nanos` requires `verifiedSettled`; manual or blocked `HOLD` never
counts. Restart intervals classify availability exposure through mutation history and are not
subtracted again from actual work.

Every bucket validates exact, nonnegative duration values and finite, nonnegative hash-work values,
with the cross-field bounds below:

```text
observed_duration_nanos + unknown_gap_duration_nanos <= 3,600,000,000,000
settled_duration_nanos <= observed_duration_nanos
trial_duration_nanos <= observed_duration_nanos
trial_actual_hash_seconds <= actual_hash_seconds
```

Report starts are arbitrary UTC timestamps because an accepted retune records the actual
`pass_started_at`. When a report boundary falls inside an hourly bucket, the overlapping boundary
portion is conservatively unknown: the evaluator never invents sub-hour state history from an
aggregate row. Report mode opens the schema-v6 database read-only and rejects missing or
incompatible durable evidence.

The first/second positive timestamps bound restart loss, while hourly actual work is the treatment
source. At each arm boundary, both miners must be `verifiedSettled` with `hold_reason = optimized`;
freeze each miner's positive
conservative `median_hash` from its currently selected validated row as that miner's
`pre_arm_settled_hash_rate`. For each exact 168-hour arm, normalize over the full wall duration, not
only observed seconds:

The credential-free report input carries the complete boundary point and its durable settlement
timestamp alongside each frozen rate. A boundary is valid only when the point is an exact canonical
frequency/voltage pair, the hash rate is finite and positive, and settlement is a nonzero UTC time
no later than the arm start. A missing or invalid tuple is an explicit invalid-evidence result, not
a request to consult current state.

```text
coverage(m, arm) = observed_seconds(m, arm) / 168h_seconds

normalized_work(m, arm) = actual_hash_seconds(m, arm) /
                          (pre_arm_settled_hash_rate(m) * 168h_seconds)

arm_uplift = normalized_work(treatment, arm) -
             normalized_work(contemporaneous_control, arm)

crossover_uplift = (arm_uplift_AB + arm_uplift_BA) / 2
```

Unknown time contributes zero to this explicitly labeled lower bound and coverage is reported
separately. Treatment and control use the same wall interval; this prevents missing polls or restart
gaps from disappearing through observed-time normalization. An arm is coverage-valid only when both
miners' coverage is at least 95%; the evaluator reports nonnegative uplift separately from operational
acceptance. A one-arm canary is accepted on work only when the coverage-valid uplift is nonnegative
and every convergence, restart, frontier, settlement, baseline-evidence, and control-stability gate
passes. A complete AB/BA crossover requires the same acceptance for both non-overlapping arms and
nonnegative crossover uplift. The predeclared practical target is the corresponding accepted uplift
at or above `0.02`.

Evaluate an authorized treatment miner against an already-settled control under unchanged firmware,
pool configuration, and safety settings. With two comparable miners, use an AB/BA crossover to
reduce device and environment bias: freeze both pre-arm rates, accept `--retune` for miner A to start
the AB arm while B remains in `HOLD`, then after 168 hours and renewed boundary qualification accept
`--retune` for B to start the BA arm while A remains in `HOLD`. Each arm starts at the accepted retune
transaction's persisted `pass_started_at`; the treatment denominator is its atomically frozen
`pass_reference_hash`, and the evaluator records the control's unchanged selected-row median at that
same timestamp. The arm ends exactly 168 hours later and uses that same contemporaneous wall interval
for its control. Any control point change invalidates the arm. Schema v5 preserves the prior selected
point, rate, and settlement timestamp in one atomic pass-reference snapshot, so a control retuned at
the exact arm boundary remains auditable without using its new pass rows; the report reader consumes
that snapshot rather than inferring the boundary from the new pass. An inter-arm gap or a later retune
that overwrites the snapshot discards the historical boundary evidence.
The 24- and 48-hour checks are relative to this arm start.
Success requires:

- at 24 hours, zero duplicate `entered` targets and zero time-created eligibility;
- at 48 hours, `verifiedSettled` `HOLD`; unresolved reconciliation is a separately reported
  safety-correct but unsuccessful convergence outcome;
- at 168 hours, at least a 90% reduction from the observed normal-restart baseline, normal
  restart-to-healthy exposure no more than 1% of miner-time, and at least 95% of post-settlement time
  at the selected point;
- the exact valid-arm lower-bound uplift rule above; the 2% value remains a practical target, not an
  alternative safety or convergence condition; and
- unchanged safety thresholds, arbitration priority, exact-pair validation, and recovery outcomes.

## Material Uncertainties

### Physical thermal slew under load

Choice or assumption: the first implementation keeps the ten-second host polling interval and uses
the firmware's variable-latency local power task as an independent thermal backstop, without adding
a host rate-of-rise predictor.

Why uncertain: source establishes the code path but its loop is not fixed-period, and source cannot
establish worst-case physical heating rate, device-I/O time, scheduler delay under load, sensor lag,
or cooling variation on the actual miners.

Consequence if wrong: a rapid rise may reach the 75°C firmware trip before Bitagnis observes its
66°C rollback or 70°C cutoff. The firmware attempts to disable VCORE, but the source does not prove
that effect completes; firmware-overheat events could also be more frequent than expected.

Resolution: fake-clock tests must cover skipped host thresholds and exact firmware-boundary
telemetry. Before fleet rollout, an explicitly authorized named canary must record the pre-change
state and recovery plan and measure worst observed rise and recovery without changing any threshold.

### Firmware emergency persistence and termination

Choice or assumption: controller recovery treats every firmware emergency side effect and the
subsequent task/process outcome as uncertain until the same MAC is rediscovered and read back.

Why uncertain: the emergency path does not check every fan, VCORE, flag, or NVS result; the NVS
setter is void and log-only; this source tree has no explicit `nvs_commit`; and the runtime meaning
of `exit(EXIT_FAILURE)` is not established by source inspection alone.

Consequence if wrong: the miner may disappear, reboot, continue with only part of the emergency
state, or return with the flag or configured pair missing. Assuming any one outcome could either
replay a dangerous write or falsely claim containment.

Resolution: keep the RFC's readback-driven `OVERHEAT` state machine for implementation. On the named
canary, induce only an approved safe test condition, record serial/device observations through the
trip and reboot boundary, and verify fan, VCORE, flag, configured pair, uptime, and post-recovery
mining before any fleet rollout.

### Five-minute hash measurement confidence

Choice or assumption: two consecutive complete five-minute candidate windows, their lower median,
and a 2% material band are sufficient to distinguish a useful point.

Why uncertain: actual pool-facing hash measurements retain share variance and can be affected by
short network or workload disturbances.

Consequence if wrong: one finite pass may terminally reject a genuinely better pair or select a
point whose apparent gain does not persist. It will still converge and preserve safety.

Resolution: replay captured credential-free summaries in deterministic tests, then compare the
chosen point with the 168-hour settled-control result. Changing the band or window count after that
evidence is a new explicit policy revision, not a time-based retry.

No architecture, ownership, recovery-action, retune-contract, pass-cap, or schema-boundary
uncertainty remains for implementation. The schema-v6 snapshot is the canonical durable source for a
historical control boundary, and the report reader consumes it without inferring an unavailable
boundary from current state.

## Complete Cutover

The implementation must replace the current design completely:

- delete `blockedPointRetry`, `RetryAfter`, `candidateDue`, and every elapsed-time eligibility path;
- retain the three current trial-purpose phases but replace candidate chaining with isolated
  admission, reserved return, and two-window promotion;
- make `HOLD` terminal and make safety cooldown recovery lead to `HOLD`;
- add the exact schema-v6 fields, enums, unique indexes, and atomic store operations above;
- add configured post-PATCH readback and exhaustive same-attempt reconciliation;
- stop `StartMutationAttempt` from auto-closing older work and stop completion failures from being
  marked terminal;
- implement the named clean-state `--retune` contract;
- reject schema v3 and noncanonical firmware grids without migration or fallback;
- delete superseded retry tests and fixtures; and
- update README, settings/operator examples, terminal state descriptions, and this RFC in the same
  current-design cutover.

Required tests cover both sides of every ASIC, VR-temperature, and power threshold during baseline,
ramp, both candidate windows, return, post-PATCH readback, post-restart verification, cooldown, and
`HOLD`. They also cover all mutation-stage rows above; PATCH success with either NVS field unchanged;
lost restart response followed by a proven boot; wrong and mixed configured pairs; persistence
failure after reboot proof; completion reopen; at-most-once admission; exact 36-pair/70-restart
simulation; seven days in `HOLD`; retune preservation/refusal; manual adoption; exact-min ambiguity;
and safety supersession. Safety cases explicitly include pair writes succeeding while flag clear
fails and vice versa; flagless `50/<old voltage>`; durable `firmware_overheat` after flag/sentinel
loss; configured minimum plus unsafe telemetry at initial preflight or with durable current already
minimum (no PATCH/restart); configured minimum plus residual source heat after this attempt's PATCH
while durable current is hotter (exactly one first restart); a new flag/sentinel or strictly-above
firmware trip before restart (no restart and correct escalation); the same residual observation after
`restart_requested` (observe only, no replay); one safe same-pair
`mutation_uncertain` lifecycle per attempt; equality and just-above observations at 75°C ASIC and
105°C VR; and repeated-overheat cooldown extension capped at 24 hours. Frontier tests include every
evidence-deadline expiry, process reopen without deadline extension, the first voltage seed and all
later adjacent-response gates, fixed-anchor selection, hourly cursor idempotence/rollback, and exact
single-arm/AB-BA uplift calculations. All device interactions use fakes; no automated test contacts
a miner.

## Logical Implementation Sequence

1. `replace timed retries with finite frontier settlement`: complete the inseparable schema,
   persistence, optimizer, mutation reconciliation, CLI, hourly accounting population, tests,
   README, and RFC cutover; delete the old retry path and reject schema v3.
2. `persist crossover boundary evidence in schema v6`: add the complete pass-reference snapshot,
   exact schema rejection, reset capture, and reopen validation.
3. `report long-term optimizer economics`: add terminal rendering and multi-day queries over the
   already-populated schema-v6 aggregates, consume historical boundary snapshots, and add their
   query, formatting, and historical-AB/BA regression tests.

## Conclusion

The long-term run validates the new durable mutation history and the safety mutation lifecycle. It
also establishes that the current normal optimizer is not converging: it performs a restart after
most evaluation windows, revisits the same targets many times, and repeatedly enters thermal regions
that require another restart.

The proposed finite frontier pass gives normal optimization a durable stopping condition without
weakening safety. It explores each complete pair at most once, compares isolated candidates against
a fresh incumbent, requires a winning candidate to repay its entry/ramp cost, measures total pass
cost against a settled control, and then spends steady-state time hashing at the selected proven safe
point. Temperature below target permits only unseen exploration, temperature
between target and the hard limit stops upward work, and rapid unsafe heating still invokes the
existing immediate rollback or emergency lifecycle.
