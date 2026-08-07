# RFC: State-Linear Optimizer Progress

Status: Proposed

Date: 2026-08-07

## Summary

Bitagnis stores optimizer deadlines durably and optimizer progress volatilely. `evidence_deadline_at`
is a column; the evidence it measures lives in an in-memory `minerRuntime` that 23 call sites erase
and that every process restart discards. The controller therefore runs a monotone clock against a
counter that resets to zero on any perturbation. That race is not close, and it is not tunable.

The schema-version-6 database `bug_optimizer3.db` covers two miners for 5.90 wall-clock hours. In
that interval the controller produced **one** mutation attempt, which failed; recorded **zero**
evaluated operating points; and left both miners in terminal states that no automatic transition can
leave:

| Miner | Phase | Since | Current point | Terminal cause |
|---|---|---|---|---|
| mineiro | `HOLD` / `blocked` | 14:09:52 | 400/1000 | baseline evidence deadline expired |
| mineira | `COOLDOWN` | 15:42:12 | 490/1060 (stale) | cooldown expiry unreachable |

Both miners are parked at or below the advertised minimum, which is the observed low hash rate and
the observed 50 °C. `mineira.cooldown_until` elapsed at 17:42:09 and the phase was still `COOLDOWN`
at the 19:42:52 accounting cursor, two hours later.

The same database contains the diagnosis. `optimizer_hourly` classifies 49.3% of all observed
wall-clock time as `unknown_gap_duration_nanos`, using the identical `gap <= MetricsTime` predicate
that governs sample admission. Roughly one poll cycle in four is never delivered, and the clean
interval rate is 0.673. Baseline evidence requires 60 consecutive clean intervals. The probability
is approximately 5 × 10⁻¹¹ per attempt, against a budget of roughly 95 polls.

This RFC does not propose widening the tolerance. Widening the tolerance raises the per-interval
probability and leaves the shape unchanged: a non-monotone progress counter racing a monotone
deadline still loses, and it loses invisibly, because nothing durable records how far it got.

This RFC proposes replacing wall-clock authority with durable monotone progress throughout the
optimizer, using the pattern the repository already proves works. `mutation_attempts` is a
milestone ledger; it is the sole reason the mineira failure is reconstructable to the millisecond six
hours after the fact. `optimizer_miners` is a set of clocks over volatile counters; it cannot
distinguish "never made progress" from "made progress forty times and lost it."

## Scope

This RFC covers:

- durable representation of evidence collection, ramp completion, and safety recovery;
- the conditions under which accumulated optimizer progress may be discarded;
- replacement of wall-clock deadlines with attempt budgets and confirmation counts;
- classification of unreadable polls as non-events rather than as safety evidence;
- exit predicates for `blocked` `HOLD` and `COOLDOWN`;
- the schema-version-7 contract and complete cutover scope; and
- the control-flow ordering defect that makes `COOLDOWN` unreachable to its own expiry.

This RFC does not weaken, delay, or rate-limit any safety control. Instantaneous safety assessment
remains on every metrics poll. Hard-limit rollback, host containment, firmware recovery, and
emergency hold remain immediate and remain independent of every counter introduced here.

This RFC does not change the poll transport, the metrics interval, or the AxeOS HTTP contract. The
poll loop's tick-drop behavior is the perturbation this design must tolerate, not the defect it
must fix. Making the poll loop more reliable is worthwhile and out of scope; a design that only
works when the poll loop is perfect is the defect.

## Dataset

The source is `bug_optimizer3.db`, schema version 6, inspected read-only on 2026-08-07. The database
remained unchanged during analysis. Coverage runs from the 13:48:52 UTC bootstrap to the 19:42:52 UTC
accounting cursor.

## What the Evidence Proves

### Progress is volatile and deadlines are durable

Every byte of evidence progress lives in one process-local struct:

```go
// optimizer.go:38-46
type minerRuntime struct {
	samples         []telemetrySample
	firstWindow     *windowSummary
	deferredWindows []windowSummary
	lastSampleAt    time.Time
	lastPoint       lib.OperatingPoint
	lastPhase       lib.OptimizerPhase
	accounting      *accountingSample
}
```

`optimizer.go` calls `resetRuntime` at 23 sites. Each call destroys `samples`, `firstWindow`, and
`deferredWindows` together. `addSample` destroys the same three fields inline at two more sites when
consecutive scheduled times differ by more than `MetricsTime`. None of this is observable after the
fact, because none of it is written anywhere.

`evidence_deadline_at` is a durable `INTEGER NOT NULL` column that only advances toward expiry. The
ledger records the deadline and never records the progress. Both terminal rows in
`operating_points` show the consequence: status `unobservable`, every measurement column zero. That
row is produced identically by a miner that never returned a valid sample and by a miner that reached
59 of 60 samples eleven times.

### The budget and the work use different units

The budget is denominated in seconds: `RampUpTime + 4 × EvaluationWindowTime` = 1260 s
(`optimizer.go:807`). The work is denominated in consecutive admitted samples:
`2 × ceil(EvaluationWindowTime / MetricsTime)` = 60 (`optimizer.go:1704`, `optimizer.go:498-509`).

Those units convert only at 100% poll yield. Below that, the budget stops bounding the work and
starts betting on the network. There is no feedback path: a degraded poll yield does not extend the
deadline, does not surface a warning, and does not alter the terminal outcome recorded.

### The race is unwinnable, not merely tight

`accountingSamplesCompatible` (`optimizer.go:106-109`) applies the same `gap <= MetricsTime`
predicate as `addSample` and persists its verdict. Per-hour:

| Hour (UTC) | Observed s | Unknown gap s | Observed % |
|---|---:|---:|---:|
| 13:00 | 337.54 | 330.00 | 50.6 |
| 14:00 | 1832.18 | 1767.82 | 50.9 |
| 15:00 | 1760.00 | 1840.00 | 48.9 |
| 16:00 | 1827.53 | 1772.47 | 50.8 |
| 17:00 | 1790.03 | 1809.97 | 49.7 |
| 18:00 | 1872.38 | 1727.62 | 52.0 |
| 19:00 | 1349.99 | 1222.22 | 52.5 |
| Total | 10769.66 | 10470.10 | 50.7 |

Solving `10n + 20m = 21239.76` against 10769.66 s of clean coverage gives n ≈ 1077 clean intervals
and m ≈ 523 doubled intervals: 1600 delivered cycles against 2124 nominal ticks, a clean-interval
rate of 0.673 and a delivered-tick rate of 0.753.

Baseline evidence needs 60 consecutive clean intervals. `0.673⁶⁰ ≈ 5 × 10⁻¹¹`. The 1260 s budget
affords roughly 95 polls at the observed 13.3 s mean interval, so at most one full attempt and a
fraction of a second. Six hours produced zero windows, which is the expected outcome.

Raising the tolerance to 15 s might lift the clean rate to 0.95. `0.95⁶⁰ ≈ 0.046`. The system would
then fail 95% of the time for reasons still invisible in the database. The tolerance is not the
variable that matters.

### Absence of evidence is processed as evidence

The mineira rollback ledger is complete and unambiguous:

```
intent_created_at      15:41:52.316
started_at             15:41:52.447
patch_requested_at     15:41:52.505    PATCH to 400/1000 issued
configured_verified_at 15:41:52.565    device confirmed 400/1000
restart_requested_at   15:41:52.603    restart issued
reboot_verified_at     0               never reached
failed_at              15:42:09.700    safety_superseded, 17.097 s later
```

`restart_requested_at` was set and `reboot_verified_at` was zero. The ledger stated that a reboot was
in flight, and the mutation worker was correctly awaiting it (`mutation.go:1172`). Concurrently the
safety loop polled the booting device, received an ASIC payload that failed
`ValidateCanonicalASICGrid` (`lib/bitaxe.go:74-84`), and executed the demolition path at
`optimizer.go:172-232`: `ClearPendingMutation`, `SetFallbackPoint({})`, zero `SettledAt`, zero
`RampUntil`, zero `EvidenceDeadlineAt`, then `SupersedeMutation`.

A device that cannot be read is not a device in a known state. The controller treated an unreadable
poll as positive evidence of danger, discarded the pending authority, and left durable state
permanently desynchronized from hardware.

`resetRuntime` on `safetyUnavailable` (`optimizer.go:251-253`) is the same error in its purest form:
a poll that produced no information erases information already earned.

### Two controllers hold incompatible models of one device

The mutation coordinator is milestone-driven and reconciles against `mutation_attempts`. The
optimizer control loop is stopwatch-driven and reconciles against `optimizer_miners` timestamps.
They run against the same miner in the same poll cycle, and at 15:42:09 the stopwatch-driven one
overwrote the milestone-driven one mid-lifecycle. There is no shared notion of "how far has this
device progressed" that both consult.

### The state-linear machinery exists and is vestigial

```go
ConsecutiveBadWindows int   // lib/state.go:125
```

It is persisted (`state.go:3233`), read back (`state.go:3312`), schema-validated (`state.go:3399`),
range-validated (`state.go:4316`), and zeroed at two sites (`state.go:644`, `state.go:830`). It is
never incremented and no control decision reads it. The design began state-linear and drifted to
wall-clock authority, leaving the counter behind.

`ObservedCount` is the one surviving correct instance: a durable column, monotone, reset only by
contradiction — a *different* point observed — with the threshold `manualConfirmationPolls = 2`
(`optimizer.go:409-440`). It survives restarts, it is diagnosable, and it governs the least
consequential decision in the system. The pattern is already expressible in the existing schema.

### Clock count drives state-space complexity

`MinerState` has 35 fields, 9 of them `time.Time`. Five — `RampUntil`, `CooldownUntil`,
`EvidenceDeadlineAt`, `PendingSince`, `SettledAt` — can each independently veto a transition, and
their relative evaluation order is load-bearing. The mineira deadlock is exactly an ordering defect
between two of them: `optimizer.go:294` returns into `observeExternalPoint` before `optimizer.go:311`
can observe that `CooldownUntil` elapsed, and `observeExternalPoint` refuses to act because the phase
is `COOLDOWN` (`optimizer.go:405-407`). Neither condition can clear the other.

N independent clocks present N! evaluation orderings to get right. A monotone milestone chain
presents N states and N−1 transitions.

## Problem Statement

The optimizer's authority for state transitions is wall-clock time. Its record of accomplished work
is volatile memory. Consequently:

1. Progress toward every evaluation is lost on any perturbation and is never durable.
2. Terminal states are entered by clock expiry and carry no record of what expired or how close it
   came, so no exit predicate can be expressed.
3. "Could not measure" and "measured and rejected" collapse into the same `unobservable` status and
   the same `blocked` hold, which are the two outcomes that most require different handling.
4. A degraded environment produces terminal failure rather than slower progress.
5. Unreadable polls mutate state, so transport quality is indistinguishable from hardware condition.
6. Independent clocks interact by evaluation order, and at least one ordering is already deadlocked
   in production.

## Decision

Adopt one rule and derive the design from it.

> **Time may be an input to a predicate. Time must never be the authority for a transition.**

"Temperature has been at or below `recoveryTemp` on three consecutive polls" uses time and is
state-linear. "120 minutes have elapsed" is time as authority. The first degrades correctly under a
degraded poll yield by taking longer; the second degrades into a wrong transition.

Three corollaries govern the cutover:

- **Progress is durable and monotone.** Any quantity a transition depends on is a column, advances
  only forward, and is reset only by an observation that contradicts it — never by elapsed time,
  never by a failed read, never by process restart.
- **A poll that yields no information causes no transition.** It advances no counter that gates a
  decision and clears no counter that records progress. It may advance a diagnostic counter.
- **A budget is a count of failed attempts, not a duration.** Exhausting it produces a state with a
  named cause and a matching exit predicate, never an absorbing state.

## Durable Evidence Contract

### Evidence epochs

Introduce `evidence_epochs`, shaped after `mutation_attempts`: one open row per miner, monotone
columns, a terminal outcome. It replaces `evidence_deadline_at`, `ramp_until`, the in-memory
`firstWindow`, and the in-memory `deferredWindows`.

An epoch is opened by exactly the events that today set an evidence deadline: bootstrap, baseline
entry, trial entry, trial return, hold validation, safety validation after recovery, manual
adoption, and operator retune. It records the complete operating point under evaluation, the
purpose, and the number of windows required.

An epoch is closed by one of four outcomes: `validated` when the required windows closed and passed
the quality predicate; `rejected` when they closed and failed it; `starved` when the rejected-window
budget was exhausted; `contradicted` when the point, phase, or device identity changed underneath it.
`starved` and `rejected` are different outcomes and must never map to the same state.

### Ramp completion is a sample count

Delete `ramp_until`. Add `settled_sample_count`: consecutive samples observed at the epoch's exact
operating point since the epoch opened. The ramp is complete at
`rampSamples = ceil(RampUpTime / MetricsTime)`.

Under nominal polling this is exactly equivalent to the current timestamp. Under a degraded poll
yield it is strictly more correct: the current code declares a device settled after 60 s regardless
of whether it observed the device during those 60 s. The counter declares it settled only after
actually watching it settle. The operator contract keeps `rampUpSeconds`; `rampSamples` is derived,
so there is one canonical representation.

### Window closure is a predicate over accumulated samples

Replace exact-spacing admission with a validity predicate. A window closes when its span reaches
`EvaluationWindowTime`. At closure it is admitted if:

- `sample_count >= windowMinSamples`, where `windowMinSamples = ceil(0.8 × targetSampleCount)`; and
- `span <= windowMaxSpan`, where `windowMaxSpan = 2 × EvaluationWindowTime`; and
- no individual gap exceeds `windowMaxGap = 3 × MetricsTime`; and
- `validateWindowSummary` passes.

Otherwise it is rejected, `rejected_windows` increments, and the sample buffer clears. Sampling
jitter becomes a data-quality attribute of a window instead of a fatal event between two samples. A
single missed poll costs one sample. Under the measured 0.673 clean rate and 0.753 delivered-tick
rate, a 300 s span delivers ≈ 23 samples against a `windowMinSamples` of 24, so the window closes
after roughly one extension rather than never closing at all.

### The first closed window is durable

`combineWindowSummaries` pairs exactly two windows, and `windowCount` is 1 or 2 everywhere
(`mutation.go:546-560`). The epoch therefore needs one durable window slot, not a table and not a
serialized telemetry blob. On the first admitted window the epoch row stores its aggregate columns —
the same shape `operating_points` already stores — and sets `closed_windows = 1`. The second admitted
window combines against the stored aggregate inside the deciding transaction.

Volatility is now bounded to at most one partial window. A process restart, a `resetRuntime`, or a
mutation gate costs a maximum of `EvaluationWindowTime` of exposure instead of the entire evaluation.

This does not violate the schema-version-6 prohibition on a serialized telemetry window: no raw
samples are persisted, only the same closed aggregate the frontier already stores.

### The budget is a count

Delete `evidence_deadline_at`. An epoch ends as `starved` when
`rejected_windows >= maxRejectedWindows`, proposed as 6. A degraded environment makes an evaluation
take longer; it does not make it fail. It fails only after the environment has demonstrably failed to
produce admissible evidence six separate times, and the epoch row records exactly that.

### Contradiction, and only contradiction, resets progress

An epoch's `settled_sample_count`, `closed_windows`, and stored window are reset only by:

- an observed operating point differing from the epoch's point;
- a phase change;
- a device identity change; or
- a safety transition that takes ownership of the miner.

They are not reset by a failed HTTP read, an incomplete telemetry payload, a non-canonical ASIC
grid, an implausible sample, a mutation gate, or process restart. `resetRuntime` retains only its
sample-buffer clearing and loses its authority over durable progress.

## Recovery Predicates Replace Cooldown Timers

`safeToRecover` (`optimizer.go:1707`) already encodes the physical exit condition, including
`Temp <= RecoveryTemp` at the configured `recoveryTemp: 61`. It is currently subordinated to
`overheatCooldown = OverheatCooldownMins × OverheatCount`, so the clock overrides the thermometer.

Replace the gate:

- `COOLDOWN` exits when `safeToRecover` holds on `recoveryHealthyPolls` consecutive polls, tracked in
  a durable `recovery_healthy_count` that is reset by any non-satisfying poll and is not advanced by
  an unreadable poll.
- Retain a physical minimum dwell of `rampSamples` consecutive safe samples so the exit cannot fire
  on a single cool reading during a thermal transient.
- `OverheatCount` stops gating *when the controller may look* and starts gating *what it may try
  next*: after an overheat episode the next epoch is restricted to points at or below the last safe
  validated point, and the restriction relaxes by one grid step per validated epoch.

This preserves the escalation's purpose — repeated overheating must make the controller more
conservative — while removing an authority that currently holds a demonstrably cool miner idle for
two hours and then, because of the ordering defect, indefinitely.

## Unreadable Polls Are Non-Events

The `canonicalASICGrid` failure path at `optimizer.go:172-232` must not mutate state. A poll whose
identity, grid, or telemetry validation fails produces no transition, no supersession, and no
authority clearing. It increments a durable `unreadable_poll_count`.

Escalation is by count, not by elapsed time. Only `unreadablePollLimit` consecutive unreadable polls
escalate to a safety-unknown episode. The limit must exceed the longest expected legitimate
unreadable interval, which is a reboot; `defaultRebootDeadline` is 2 minutes, so
`unreadablePollLimit = ceil(defaultRebootDeadline / MetricsTime)` = 12 keeps the two bounds derived
from one number.

An unfinished mutation attempt with `restart_requested_at` set and `reboot_verified_at` zero
suppresses escalation entirely for the duration of that attempt. The ledger already states that the
device is expected to be unreadable; a second subsystem must not contradict it. This is the specific
change that would have prevented the mineira supersession.

## Terminal States Get Exit Predicates

`blocked` `HOLD` is currently absorbing: `optimizer.go:339-342` returns immediately and nothing
re-arms. Split it by cause, because the two causes need opposite handling.

- **Blocked by starvation** (epoch outcome `starved`): the controller could not measure. The miner is
  healthy and the environment failed. Exit automatically by opening a new epoch once
  `windowMinSamples` consecutive samples have been admitted at the current point, which is a direct
  observation that the environment recovered. No timer.
- **Blocked by rejection** (epoch outcome `rejected`): the controller measured, and the point failed
  the quality or headroom predicate. This is a real conclusion about hardware. It remains terminal
  until an operator retune, exactly as today.

`operating_points` gains the same split. `unobservable` currently means both "never measured" and
"measured and unusable," which is why neither miner's history distinguishes a broken network from a
bad operating point. Reserve `unobservable` for starvation and require every other rejected status to
carry closed-window evidence.

## Control-Flow Ordering Contract

The live-point reconciliation at `optimizer.go:294` must not precede the phase and recovery handling
at `optimizer.go:311-333`. Reconciliation is not a phase; it is an input to every phase.

Additionally, a live point that differs from durable current is not automatically an external manual
change. Before treating it as one, consult `mutation_attempts` for a failed or superseded attempt on
this miner whose target equals the live point and whose `configured_verified_at` is nonzero. That is
a *known* outcome of a controller-owned action, and it must be adopted as a reconciliation of the
controller's own ledger rather than refused as a foreign change. mineira's live 400/1000 is precisely
this case, and the current code cannot express it in any phase.

## What Time Remains Authoritative For

Enumerated exhaustively, so the boundary is testable:

- **Hourly accounting.** `optimizer_hourly` is a wall-clock time series by definition. It is
  reporting, not control, and it keeps its current semantics.
- **Mutation stage deadlines.** A device that never answers must not hold an attempt open forever.
  `defaultRebootDeadline` remains, and it remains the only place a duration terminates anything.
  Its expiry must produce a retryable failure, never a supersession of durable authority.
- **`PhaseStartedAt`, `PendingSince`, `SettledAt`, `opened_at`, `closed_at`.** Retained as
  observability. No control predicate may read them.

Every other duration in the optimizer becomes a derived count or is deleted.

## Exact Schema-Version-7 Contract

Schema version 7 replaces version 6. Opening any other nonzero version fails with an explicit
incompatible-schema error. There is no migration, dual reader, or silent reinterpretation, per the
replace-do-not-preserve rule. Existing databases are moved aside; the analysis value of
`bug_optimizer3.db` is preserved by keeping the file, not by reading it.

`optimizer_miners` removes:

- `ramp_until` — replaced by `evidence_epochs.settled_sample_count`;
- `evidence_deadline_at` — replaced by `evidence_epochs.rejected_windows` and the budget; and
- `consecutive_bad_windows` — dead since introduction.

`optimizer_miners` adds:

- `evidence_epoch_id INTEGER NOT NULL`, zero when no epoch is open, otherwise the open epoch's id;
- `recovery_healthy_count INTEGER NOT NULL`, zero outside `COOLDOWN` and `OVERHEAT`, never exceeding
  `recoveryHealthyPolls`; and
- `unreadable_poll_count INTEGER NOT NULL`, zero after any readable poll, never exceeding
  `unreadablePollLimit`.

`hold_reason` replaces `blocked` with exactly `starved` and `rejected`. The enum becomes
`optimized`, `safety`, `manual`, `starved`, `rejected`.

New table `evidence_epochs`:

```sql
CREATE TABLE evidence_epochs (
	id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	mac_addr TEXT NOT NULL,
	frequency INTEGER NOT NULL,
	core_voltage INTEGER NOT NULL,
	purpose TEXT NOT NULL,
	required_windows INTEGER NOT NULL,
	opened_at INTEGER NOT NULL,
	settled_sample_count INTEGER NOT NULL,
	closed_windows INTEGER NOT NULL,
	rejected_windows INTEGER NOT NULL,
	missed_polls INTEGER NOT NULL,
	window_sample_count INTEGER NOT NULL,
	window_span_nanos INTEGER NOT NULL,
	window_median_hash REAL NOT NULL,
	window_expected_hash REAL NOT NULL,
	window_attainment REAL NOT NULL,
	window_mean_temp REAL NOT NULL,
	window_p95_temp REAL NOT NULL,
	window_p95_vr_temp REAL NOT NULL,
	window_p95_power REAL NOT NULL,
	window_error_percent REAL,
	window_accepted_delta INTEGER NOT NULL,
	window_rejected_delta INTEGER NOT NULL,
	closed_at INTEGER NOT NULL,
	outcome TEXT NOT NULL
);

CREATE UNIQUE INDEX evidence_epochs_one_open
	ON evidence_epochs(mac_addr)
	WHERE closed_at = 0;

CREATE INDEX evidence_epochs_mac_opened
	ON evidence_epochs (mac_addr, opened_at);
```

`purpose` is exactly `baseline`, `trial`, `hold_validation`, or `safety_validation`.
`required_windows` is 1 or 2. `outcome` is empty while `closed_at = 0` and otherwise exactly
`validated`, `rejected`, `starved`, or `contradicted`.

The `window_*` columns are zero or NULL while `closed_windows = 0` and hold the first admitted
window's aggregate once `closed_windows >= 1`. They use the same encodings as the corresponding
`operating_points` columns. `settled_sample_count`, `closed_windows`, `rejected_windows`, and
`missed_polls` are non-negative and monotone within an epoch; a contradiction closes the epoch rather
than decrementing them.

`operating_points` keeps its columns. Its status enum splits `unobservable` and gains `starved`:

```text
entered, validated, no_gain, unstable, starved, unobservable, thermal, power, vr_hot
```

`starved` means no admissible window was produced and carries no measurement. Every other terminal
status requires a positive `entry_attempt_id`, a closed epoch with `outcome = 'validated'` or
`'rejected'`, and complete measurement columns.

`mutation_attempts` is unchanged. It is the reference implementation for this design.

## Non-Goals

- Changing the exploration policy, frontier ordering, candidate selection, or the finite-pass
  contract from `RFC_LONG_TERM_OPT.md`. This RFC changes how evidence is accumulated and how
  transitions are authorized, not which points are tried.
- Changing the poll transport, `metricsInterval`, HTTP timeouts, or the discovery contract.
- Changing hourly accounting semantics or the 384-hour retention bound.
- Adding a scheduler, work queue, retry framework, or configuration switch. Every new quantity is a
  counter on an existing or single new durable row.
- Preserving readability of schema-version-6 databases.

## Verification

Scope-driven, expanding to the boundaries this change touches.

- `lib` boundary tests for schema version 7: exact column set, index set, enum validation, epoch
  monotonicity, the one-open-epoch partial index, and rejection of version 6.
- Controller tests reproducing both production failures from the dataset:
  - a poll sequence with a 0.673 clean-interval rate must produce an admitted window and a validated
    epoch, where the current code produces `unobservable`;
  - a rollback that reaches `restart_requested_at` followed by unreadable polls must leave the
    attempt open and the pending authority intact, and must complete when the device returns.
- An ordering test asserting that a miner whose `CooldownUntil` has elapsed and whose live point
  differs from durable current exits `COOLDOWN` — the exact mineira deadlock.
- A starvation test asserting that `starved` `HOLD` exits automatically once `windowMinSamples`
  consecutive samples are admitted, and that `rejected` `HOLD` does not.
- A restart test asserting that an epoch with `closed_windows = 1` resumes against the durable stored
  window and loses at most the partial window.
- Safety-write canary and mining-write canary per `README.md`, unchanged, before any hardware run.

## Complete Cutover Scope

Delete, without compatibility paths:

- `MinerState.RampUntil`, `MinerState.EvidenceDeadlineAt`, `MinerState.ConsecutiveBadWindows` and
  every read, write, validation, and schema reference;
- `handleEvidenceDeadline` (`optimizer.go:1206-1258`) and all four call paths into it;
- `minerRuntime.firstWindow` and `minerRuntime.deferredWindows`, the `allowOptimization` deferral
  machinery at `optimizer.go:369-391`, and the two-window retention comment it carries — a durable
  first window makes deferral unnecessary;
- the exact-spacing resets at `optimizer.go:1282-1298`, replaced by window-closure validation;
- `resetRuntime`'s authority over durable progress at all 23 sites, retaining only sample-buffer
  clearing;
- the `overheatCooldown` wall-clock gate at `optimizer.go:311-322` and its four construction sites,
  replaced by `recovery_healthy_count`; and
- the `HoldBlocked` constant and every branch that treats measurement failure and measurement
  rejection identically.

Update in the same change: `README.md` §State and §Optimizer for the version-7 contract and the
`starved`/`rejected` split; `AGENTS.md` §Thermal-Control and State Invariants to state the
time-is-not-authority rule and the unreadable-poll rule; `settings.example.yaml` comments where they
describe `rampUpSeconds` and `evaluationWindowMinutes` as deadlines rather than as derived counts.

## Logical Commit Sequence

1. Schema version 7: `evidence_epochs`, `optimizer_miners` column changes, enum changes, validation,
   and boundary tests. No behavior change; the optimizer still writes the old fields' replacements
   without reading them.
2. Ramp as `settled_sample_count`. Delete `ramp_until`.
3. Window closure by validity predicate; durable first window; delete `firstWindow`,
   `deferredWindows`, and the deferral machinery.
4. Attempt budget replaces `evidence_deadline_at`; delete `handleEvidenceDeadline`.
5. Unreadable polls become non-events; `unreadable_poll_count`; reboot-in-flight suppression.
6. `recovery_healthy_count` replaces the cooldown clock; `OverheatCount` moves to candidate
   restriction.
7. Control-flow ordering fix and ledger-aware live-point reconciliation.
8. `starved`/`rejected` split across `hold_reason` and `operating_points`, with the starvation exit
   predicate.
9. Documentation cutover.

Commits 3 and 4 are one inseparable cutover if the budget cannot be expressed without durable
windows; keep them together rather than staging a compatibility path.

## Material Uncertainties

### Cooldown exit predicate and thermal safety

Replacing a 120-minute escalating gate with a three-poll thermal predicate is the highest-risk change
here and touches a thermal invariant. If `recoveryTemp: 61` plus a `rampSamples` dwell is
insufficient to prove the board has actually shed heat rather than momentarily dipped, the controller
could re-enter a thermal cycle faster than today and oscillate. Consequence: more overheat episodes
and more restarts, the exact pathology `RFC_LONG_TERM_OPT.md` was written to stop. Resolve by
instrumenting the existing cooldowns first: log `safeToRecover` transitions and temperature slope for
a full week under the current timer without acting on them, and set the dwell from the measured
settling time. Do not ship commit 6 before that data exists. Per `AGENTS.md`, this item requires a
dedicated architect agent before implementation.

### `windowMinSamples` and measurement confidence

`ceil(0.8 × targetSampleCount)` = 24 samples is asserted, not derived. `RFC_LONG_TERM_OPT.md`
already carries an open uncertainty on five-minute hash measurement confidence at the full 30
samples; admitting a window at 24 widens the median's confidence interval by roughly 12% and could
promote a point that a stricter window would reject. Consequence: worse selections, not unsafe ones.
Resolve by recomputing the historical medians in `bug_optimizer3.db`'s predecessor databases at 24
and 30 samples and comparing selection outcomes. If the difference is material, raise the floor and
accept slower epochs rather than lowering confidence.

### Whether the observed poll yield is steady state

The 0.673 clean-interval rate is measured over 5.9 hours on one network with two miners. It may
reflect a transient condition — WiFi contention, a specific AxeOS build, thermal throttling of the
host — rather than the environment this design must tolerate. Consequence: the design is correct
either way, but `maxRejectedWindows = 6` and `windowMinSamples = 24` are calibrated against a
possibly unrepresentative sample. Resolve by extending `optimizer_hourly` interpretation across the
older databases and confirming the rate is stable before fixing the constants. The constants are
cheap to change; the shape of the design is not.

### `unreadablePollLimit` against real reboot duration

`ceil(defaultRebootDeadline / MetricsTime)` = 12 assumes AxeOS reboots complete inside 2 minutes and
that a booting device returns unreadable rather than plausible-but-wrong telemetry. The mineira
evidence shows a booting device returning a *successful* HTTP response with a non-canonical grid at
17 s, which is exactly the ambiguous case. Consequence: if a booting device can return a canonical
grid with wrong telemetry, suppression during reboot could mask a genuine fault. Resolve during the
safety-write canary by capturing every poll across ten deliberate reboots and classifying what AxeOS
actually returns at each stage.

### Interaction with the finite-pass contract

`RFC_LONG_TERM_OPT.md` defines pass semantics, `pass_reference_*` snapshots, and settlement against
the current phase and deadline model. This RFC changes what "an evaluation completed" means, and the
AB/BA report boundary logic consumes settlement timestamps. Consequence: report-mode historical
boundaries could become unavailable or subtly wrong across the cutover. Resolve by enumerating every
`pass_reference_settled_at` producer and consumer before commit 1 and asserting the boundary
semantics in the schema-version-7 boundary tests.

### Poll-loop reliability remains unaddressed

This RFC deliberately treats tick drops as environment. If the 25% undelivered-tick rate is caused by
something inside the process — the store mutex, serialized HTTP per miner, or SQLite contention
during `CompareAndSetHourly` — then fixing it would recover most of the loss at a fraction of this
cost, and this design would be over-engineered for the actual fault. Consequence: wasted effort, not
incorrectness. Resolve cheaply and first: instrument per-cycle wall time for one hour and attribute
it across HTTP, store, and coordination. Do that before commit 1. The design still stands if the poll
loop improves, because a system whose correctness requires a perfect poll loop is the defect this RFC
names.

## Conclusion

The controller already contains a correct answer to this problem. `mutation_attempts` records
irreversible facts in monotone columns, admits nothing from an unreadable poll, and can be
reconstructed by any process at any later time. It is why the mineira rollback is diagnosable to the
millisecond and why safety behavior has been trustworthy across both prior RFCs.

The optimizer instead records deadlines and forgets work. Six hours of runtime produced one failed
mutation, zero measurements, two absorbing states, and two miners pinned at the ASIC floor — and the
database cannot say whether the controller came close even once.

Extending the ledger pattern to evidence collection removes five clocks, two absorbing states, one
deadlock, and one dead column, and replaces them with counters that survive restarts and that state
plainly how far the system got. The result degrades under a bad network by taking longer, which is
the correct failure mode, instead of by terminating into a state it cannot describe or leave.
