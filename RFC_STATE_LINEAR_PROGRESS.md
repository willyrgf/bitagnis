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

| Miner | Phase | Entered | `phase_started_at` | Current point | Terminal cause |
|---|---|---|---|---|---|
| mineiro | `HOLD` / `blocked` | 14:09:52 (derived) | 13:48:52 | 400/1000 | baseline evidence deadline expired |
| mineira | `COOLDOWN` | 15:42:12 | 15:42:12 | 490/1060 (stale) | cooldown expiry unreachable |

mineiro's entry time is not read from the database. It is derived by applying the 1260 s budget to
the 13:48:52 bootstrap, because the transition into `blocked` `HOLD` never stamped `PhaseStartedAt`
and the durable row still reports the bootstrap six hours later. See *Terminal transitions do not
record their own time*.

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
- classification of unreadable polls as non-events for the optimizer, distinct from the safety
  assessment that continues on every poll;
- exit predicates for `blocked` `HOLD` and `COOLDOWN`;
- the constructions that make the new invariants unrepresentable-when-violated rather than restated
  at every call site;
- the schema-version-7 contract and complete cutover scope; and
- the control-flow ordering defect that makes `COOLDOWN` unreachable to its own expiry.

This RFC does not weaken, delay, or rate-limit any safety control. Instantaneous safety assessment
remains on every metrics poll, including polls whose ASIC grid or identity failed validation, at
every value of every counter introduced here. Hard-limit rollback, host containment, firmware
recovery, and emergency hold remain immediate and remain independent of those counters. *Unreadable
Polls Are Non-Events for the Optimizer* states branch by branch which escalations survive, because
that is the one place where a careless reading of this RFC would weaken a safety control.

This RFC does not change the poll transport, the metrics interval, or the AxeOS HTTP contract. The
poll loop's tick-drop behavior is the perturbation this design must tolerate, not the defect it must
fix. That behavior does appear to have an in-process cause (*The tick loss is inside the process*),
and fixing it is worthwhile, cheap, and separable — it is listed as commit 0 because it calibrates
the constants here, not because this design depends on it. A design that only works when the poll
loop is perfect is the defect.

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

`resetRuntime` is called at 22 sites in `optimizer.go`, and is handed to the mutation coordinator as
a callback at `main.go:210`, so the mutation lifecycle erases optimizer progress too. Each call
destroys `samples`, `firstWindow`, and `deferredWindows` together. `addSample` destroys the same
three fields inline at two more sites when consecutive scheduled times differ by more than
`MetricsTime`. None of this is observable after the fact, because none of it is written anywhere.

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
predicate as `addSample` and persists its verdict. Per-hour, for mineiro (`94:a9:90:12:fb:38`):

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

mineira's totals over the same interval are 10759.66 s observed and 10480.10 s unknown: within 0.09%
of mineiro's. Both miners lose the same fraction of the same wall clock, which is treated separately
in *The tick loss is inside the process*.

Every gap is an integer multiple of `MetricsTime`, because the sample time is the ticker's tick
rather than the read time (`main.go:263-269`). Solving `10n + 20m = 21239.76` against 10769.66 s of
clean coverage therefore gives n ≈ 1077 clean intervals and m ≈ 523 doubled intervals: 1600
delivered cycles against 2124 nominal ticks, a clean-interval rate of 0.673 and a delivered-tick
rate of 0.753. The two-parameter solve assumes no gap exceeded 20 s; a longer gap would shift the
split without changing the delivered-tick rate.

Baseline evidence needs 60 consecutive clean intervals. `0.673⁶⁰ ≈ 5 × 10⁻¹¹`. The 1260 s budget
affords roughly 95 polls at the observed 13.3 s mean interval, so at most one full attempt and a
fraction of a second. Six hours produced zero windows, which is the expected outcome.

Raising the tolerance to 15 s might lift the clean rate to 0.95. `0.95⁶⁰ ≈ 0.046`. The system would
then fail 95% of the time for reasons still invisible in the database. The tolerance is not the
variable that matters.

### The tick loss is inside the process

Two miners on independent links lost the same fraction of the same wall clock to within 0.09%. A
network cause would have to be that precisely shared. A host cause is that shared by construction.

`main.go:222-269` calls `pollMiners` synchronously from the `select` loop, and `time.Ticker` drops
ticks when the receiver is slow: its channel holds one. A cycle exceeding `MetricsTime` therefore
drops the next tick for every miner at once. `pollMiners` performs the HTTP fan-out, hourly
accounting against SQLite, safety enforcement, mutation coordination, and rendering before it
returns, and it stamps every sample with the tick time rather than the read time. A cycle exceeding
10 s roughly one time in three reproduces the observed distribution exactly: two thirds of intervals
at 10 s, one third at 20 s, a 13.3 s mean, and both miners losing the same ticks.

This is a defect in the poll loop and it is not what this RFC fixes. It bears on this RFC in one way
that matters: 0.673 is the signature of a fixable in-process bug rather than a measurement of the
environment, so it must not be used to fix constants. See *Constants are calibrated against a
defect*.

### Terminal transitions do not record their own time

`handleEvidenceDeadline` (`optimizer.go:1206-1258`) sets `Phase`, `HoldReason`, `SettledAt`, and
`EvidenceDeadlineAt`, and never touches `PhaseStartedAt`. mineiro's durable `phase_started_at` is
therefore still the 13:48:52 bootstrap, six hours after it entered a state it cannot leave.

The ledger cannot say how far the controller got, and it cannot say when it stopped either. Every
state introduced here stamps its own entry: `evidence_epochs.closed_at`, written in the same
transaction as the outcome, is the authoritative record of when an evaluation ended, and every phase
transition sets `PhaseStartedAt`.

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
2. Terminal states are entered by clock expiry and carry no record of what expired, how close it
   came, or when it was entered, so no exit predicate can be expressed and no post-hoc account is
   possible.
3. "Could not measure" and "measured and rejected" collapse into the same `unobservable` status and
   the same `blocked` hold, which are the two outcomes that most require different handling.
4. A degraded environment produces terminal failure rather than slower progress.
5. Unreadable polls mutate state, so transport quality is indistinguishable from hardware condition.
6. Independent clocks interact by evaluation order, and at least one ordering is already deadlocked
   in production.

## Decision

Adopt one rule and derive the design from it.

> **Time may be an input to a predicate. Time must never be the authority for a transition.**

"Temperature has been at or below `recoveryTemp` on `recoveryHealthyPolls` consecutive polls" uses
time and is state-linear. "120 minutes have elapsed" is time as authority. The first degrades
correctly under a degraded poll yield by taking longer; the second degrades into a wrong transition.

Three corollaries govern the cutover:

- **Progress is durable and monotone.** Any quantity a transition depends on is a column and
  advances only forward. It is never rolled back — an observation that contradicts it ends the
  record that holds it and starts a new one. Nothing is discarded by elapsed time, by a failed read,
  or by process restart.
- **A poll that yields no information causes no transition.** It advances no counter that gates a
  decision and clears no counter that records progress. It may advance a diagnostic counter.
- **A budget is a count of failed attempts, not a duration.** Exhausting it produces a state with a
  named cause and a matching exit predicate, never an absorbing state.

## Correctness by Construction

The rule and its corollaries are stated as rules, and a rule applied at 23 call sites is the failure
mode this RFC diagnoses. `ConsecutiveBadWindows` is persisted, read back, schema-validated, and
range-validated, and no path increments it, because "increment it here" was left to discipline. Six
constructions move these obligations out of control flow and into representation.

**A poll is parsed into a closed outcome at the boundary.** Introduce a `readablePoll` that can only
be constructed from a response with confirmed identity, a canonical ASIC grid, and validated
telemetry, and have every progress-advancing function accept it in place of `lib.Info`. "An
unreadable poll advances no counter" then stops being a rule to remember at 23 sites: an unreadable
poll produces no value that any progress function will accept. Safety assessment keeps taking the
weaker telemetry-only value, which is exactly the separation *Unreadable Polls Are Non-Events for
the Optimizer* depends on.

**Epoch progress has one owner.** `settled_sample_count`, `closed_windows`, `rejected_windows`, and
`missed_polls` are not four integers that callers update. They are one type whose only exported
methods are the legal advances — `observeSample`, `closeWindow` — so advancing one and forgetting
another is unrepresentable rather than merely wrong. The type has no reset method, because
*Contradiction ends the epoch* means there is no in-place reset to expose.

**The open epoch has one representation.** `evidence_epochs_one_open` is that representation. A
mirroring `optimizer_miners.evidence_epoch_id` would be a second, requiring both to be written in
one transaction forever after — the same two-controllers hazard diagnosed above, reintroduced one
layer down. It is not added. The open epoch is
`SELECT id FROM evidence_epochs WHERE mac_addr = ? AND closed_at = 0`.

**New enums are closed in Go, not only in SQL.** `purpose` and `outcome` are named string types with
an exhaustiveness check called at every decode and durable-load path. The `CHECK` constraints remain
as the storage-layer statement of the same fact; neither replaces the other, because SQLite cannot
make a value un-constructible in the process and Go cannot enforce it in a file another tool wrote.
The `Point*` statuses (`lib/state.go:39-47`) are bare untyped string constants today, while
`OptimizerPhase`, `HoldReason`, and `SafetyReason` are named types. This RFC changes that enum, so
typing it is part of this cutover.

**The stored window is optional at the boundary, not fifteen correlated columns in the core.** The
row carries `window_*` columns and a `CHECK` correlating them with `closed_windows`. The load path
turns them into an optional aggregate — present exactly when a window has closed, built by a
fallible constructor that requires the complete set — so no consumer can read `window_median_hash`
from an epoch that has closed no window. The bounded exception granted in *Exact Schema-Version-7
Contract* stops at the storage layer.

**Durable state changes only through transitions.** `OptimizerStore` exposes twenty write methods
today. Fifteen of them are optimizer state transitions, each with its own transaction, its own
validation call, and its own hand-written multi-table write. Every rule of the form "these two rows
must change together" is restated in fifteen places, and each new rule this RFC adds — close the
epoch in the same transaction as the state change that caused it — would be restated fifteen more
times.

Replace them with one write path over a closed set of transition values:

```go
type Transition interface{ isOptimizerTransition() }

func (store *OptimizerStore) Apply(transition Transition, at time.Time) error
```

Each variant is a struct carrying exactly the facts its transition needs; the unexported marker
method closes the set to `lib`; `Apply` holds the only `BeginTx`, the only validation pass, and one
exhaustive switch mapping variant to writes. "The epoch and the miner change together" stops being a
rule and becomes a property of there being one transaction. A new transition that the switch does
not handle fails the exhaustiveness check rather than silently writing half a state.

The four mutation-milestone appends (`StartMutationAttempt`, `AdvanceMutationAttempt`,
`RecordConfiguredVerification`, `RecordFirstPositive`) stay as they are: they are monotone
single-table appends with no optimizer-state coupling, and they are the design this RFC is copying.
The five writes that *are* coupled — supersession, quarantine, completion, and the two
mutation-failure paths — become transitions, because that coupling is precisely where *Two
controllers hold incompatible models of one device* comes from.

Each of these replaces a repeated obligation with one construction site. None adds a type more
complex than the invalid states it removes, which is the condition under which the mechanism is
worth its cost. The transition set is the largest of them and also the one that deletes the most:
fifteen public methods become one.

## Durable Evidence Contract

### Evidence epochs

Introduce `evidence_epochs`, shaped after `mutation_attempts`: one open row per miner, monotone
columns, a terminal outcome. It replaces `evidence_deadline_at`, `ramp_until`, the in-memory
`firstWindow`, and the in-memory `deferredWindows`.

An epoch is opened by exactly the events that today set an evidence deadline: bootstrap, baseline
entry, trial entry, trial return, hold validation, safety validation after recovery, manual
adoption, and operator retune — plus starvation, which opens a `probation` epoch (see *Terminal
States Get Exit Predicates*). It records the complete operating point under evaluation, the purpose,
and the number of windows required.

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

Replace exact-spacing admission with a closure rule and an admission predicate, which are two
different things and must not be stated as one.

A window closes when either bound is reached:

- `sample_count` reaches `targetSampleCount`; or
- `span` reaches `windowMaxSpan = 2 × EvaluationWindowTime`.

A closed window is admitted if:

- `sample_count >= windowMinSamples`, where `windowMinSamples = ceil(0.8 × targetSampleCount)`; and
- no individual gap exceeds `windowMaxGap = 3 × MetricsTime`; and
- `validateWindowSummary` passes.

Otherwise it is rejected, `rejected_windows` increments, and the sample buffer clears. Sampling
jitter becomes a data-quality attribute of a window instead of a fatal event between two samples. A
single missed poll costs one sample and extends the span.

Two bounds rather than one is what makes the degradation graceful. Today a window is exactly 30
samples spaced exactly 10 s apart, and any deviation is fatal. Under this rule the sample count is
what normally closes the window and the span is only a backstop: at the measured 0.753
delivered-tick rate, 30 samples arrive in ≈ 398 s, well inside the 600 s cap, so windows close with
the full 30 samples exactly as they do today. `windowMinSamples` binds only when the span cap fires
first, which requires a delivered-tick rate below 0.5. The median is therefore computed over 30
samples in the ordinary case and over at least 24 in the degraded one, instead of over 30 samples or
never.

### The first closed window is durable

`combineWindowSummaries` pairs exactly two windows (`optimizer.go:498-509`, `optimizer.go:675-681`),
and no evaluation requires more: the resume path derives a `windowCount` of 1 or 2 for every phase
(`mutation.go:546-560`). The epoch therefore needs one durable window slot, not a table and not a
serialized telemetry blob. On the first admitted window the epoch row stores its aggregate columns —
the same shape `operating_points` already stores — and sets `closed_windows = 1`.

The second admitted window is combined against the stored aggregate by the controller, because the
predicates that consume the combined value — `qualityHealthy`, `trialWindowPredicate`,
`entryMarginPositive` — are controller decisions and one of them reads the store. The combine is
arithmetic, not persistence. What must be atomic is the *decision*: closing the epoch and writing
the resulting `operating_points` record are one transition, so no state exists in which the epoch
closed and the record did not.

Volatility is now bounded to at most one partial window. A process restart, a `resetRuntime`, or a
mutation gate costs a maximum of `EvaluationWindowTime` of exposure instead of the entire evaluation.

This does not violate the schema-version-6 prohibition on a serialized telemetry window: no raw
samples are persisted, only the same closed aggregate the frontier already stores.

### The budget is a count

Delete `evidence_deadline_at`. An epoch ends as `starved` when
`rejected_windows >= maxRejectedWindows`, proposed as 6. A degraded environment makes an evaluation
take longer; it does not make it fail. It fails only after the environment has demonstrably failed to
produce admissible evidence six separate times, and the epoch row records exactly that.

### Contradiction ends the epoch

An epoch records the point, phase, and device it is evidence about. When any of those changes, the
accumulated evidence is no longer evidence about anything current, and the epoch is closed with
outcome `contradicted`. A successor epoch opens for the new subject if one is warranted. Progress is
never reset in place, because an epoch whose subject changed is a different epoch — and because an
in-place reset would be a second mechanism for the same fact, discoverable only by reading every
caller.

The contradictions are exactly:

- an observed operating point differing from the epoch's point;
- a phase change;
- a device identity change; and
- a safety transition that takes ownership of the miner.

Everything else leaves the epoch open and its counters intact: a failed HTTP read, an incomplete
telemetry payload, a non-canonical ASIC grid, an implausible sample, a mutation gate, or a process
restart. `resetRuntime` retains only its sample-buffer clearing and loses its authority over durable
progress.

This is why `EpochProgress` exposes no reset. A closed epoch is closed by a transition; there is no
method that empties a live one.

## Recovery Predicates Replace Cooldown Timers

`safeToRecover` (`optimizer.go:1707`) already encodes the physical exit condition, including
`Temp <= RecoveryTemp` at the configured `recoveryTemp: 61`. It is currently subordinated to
`overheatCooldown = OverheatCooldownMins × OverheatCount`, so the clock overrides the thermometer.

Replace the gate:

- `COOLDOWN` exits when `safeToRecover` holds on `recoveryHealthyPolls` consecutive polls, tracked
  in a durable `recovery_healthy_count` that is reset by any non-satisfying poll and is not advanced
  by an unreadable poll. `recoveryHealthyPolls = rampSamples`: one constant, sized as a physical
  dwell so the exit cannot fire on a single cool reading during a thermal transient. There is
  deliberately no second, smaller threshold beside it — the larger would always subsume the smaller,
  and a constant that no decision reads is how `ConsecutiveBadWindows` began.
- `OverheatCount` stops gating *when the controller may look* and starts gating *what it may try
  next*: after an overheat episode the next epoch is restricted to points at or below the last safe
  validated point, and the restriction relaxes by one grid step per validated epoch. The restriction
  level is **derived, not stored**: it is the last safe validated point relaxed by the number of
  epochs closed `validated` since the episode's `PhaseStartedAt`, which is one query against
  `evidence_epochs` — the reason `evidence_epochs_mac_opened` exists. A column would be a second
  representation of a fact the ledger already carries, and it would need its own reset rule.
- When the restriction is already at the minimum advertised pair there is no lower point to restrict
  to and no backoff left to apply. That case is already owned and stays owned: unsafe telemetry at
  the exact minimum with no firmware flag is a mutation-free durable emergency hold. The ladder ends
  there rather than resuming exploration.

This preserves the escalation's purpose — repeated overheating must make the controller more
conservative — while removing an authority that currently holds a demonstrably cool miner idle for
two hours and then, because of the ordering defect, indefinitely.

`AGENTS.md` §Thermal-Control states "Repeated overheats extend cooldown, capped at 24 hours. Do not
remove this backoff accidentally." This RFC removes it deliberately and replaces it with the
candidate restriction above. That bullet is rewritten in the same change rather than left to
contradict the code, and `overheatCooldownMinutes` is deleted from settings entirely, since nothing
reads it once `overheatCooldown` is gone.

## Unreadable Polls Are Non-Events for the Optimizer

Grid canonicality and telemetry validity are independent facts about one response, and
`optimizer.go:172-232` conflates them. It treats any `canonicalASICGrid` failure as grounds to clear
authority, and it is simultaneously the only path that escalates a dangerous reading accompanied by
a malformed grid. Splitting the two is what makes this section safe; collapsing them into "an
unreadable poll changes nothing" would silence a thermal emergency for up to `unreadablePollLimit`
polls.

**An unreadable identity or grid is a non-event for the optimizer.** It produces no phase
transition, no supersession, no `ClearPendingMutation`, no `SetFallbackPoint`, and no clearing of
settled, ramp, or epoch state. It increments a durable `unreadable_poll_count` and does nothing
else. `optimizer.go:180-184` and `optimizer.go:220-232` are deleted outright.

**Telemetry that validated is assessed on every poll regardless.** `assessInstantaneousSafety` runs
on the fields that did validate, and its `safetyHostContainment`, `safetyFirmwareRecovery`,
`safetyEmergencyHold`, and `safetyRollback` outcomes act exactly as they do today. A device
reporting 74 °C with a malformed ASIC grid is a thermal emergency, not a read failure. Of the four
branches at `optimizer.go:186-219`:

- the `firmwareOverheat || firmwareTrip` branch survives unchanged; `info.OverHeatMode` and
  `info.Frequency == 50` do not depend on the grid;
- the final `else` branch survives unchanged; it fires precisely when the assessment is unsafe,
  which is the case that must never be suppressed;
- the `safetyOwned` branch is deleted; it escalates on the controller's own prior state rather than
  on any reading, and it is what superseded the mineira rollback; and
- the `safetyNormal || safetyUnavailable` branch is deleted and replaced by the counter; it is what
  produced mineiro's absorbing `blocked` `HOLD` out of a poll that carried no information.

Escalation for a persistently unreadable device is by count, not by elapsed time. Only
`unreadablePollLimit` consecutive unreadable polls escalate to a safety-unknown episode, which is
what covers the legitimate residue of the deleted `safetyOwned` branch: a safety-owned miner that
goes unreadable and stays unreadable is escalated by the count, not by its own history. The limit
must exceed the longest expected legitimate unreadable interval, which is a reboot;
`defaultRebootDeadline` is 2 minutes, so
`unreadablePollLimit = ceil(defaultRebootDeadline / MetricsTime)` = 12 keeps the two bounds derived
from one number. Instantaneous safety assessment is not subject to this count; it runs on every poll
at every count value.

An unfinished mutation attempt with `restart_requested_at` set and `reboot_verified_at` zero
suppresses escalation entirely for the duration of that attempt. The ledger already states that the
device is expected to be unreadable; a second subsystem must not contradict it. This is the specific
change that would have prevented the mineira supersession.

## Terminal States Get Exit Predicates

`blocked` `HOLD` is currently absorbing: `optimizer.go:339-342` returns immediately and nothing
re-arms. Split it by cause, because the two causes need opposite handling.

- **Blocked by starvation** (epoch outcome `starved`): the controller could not measure. The miner is
  healthy and the environment failed. Exit automatically once `windowMinSamples` consecutive samples
  have been admitted at the current point, which is a direct observation that the environment
  recovered. No timer.
- **Blocked by rejection** (epoch outcome `rejected`): the controller measured, and the point failed
  the quality or headroom predicate. This is a real conclusion about hardware. It remains terminal
  until an operator retune, exactly as today.

The starvation exit needs somewhere durable to count, and a closed epoch cannot hold it. Rather than
add a fifth counter to `optimizer_miners`, closing an epoch as `starved` immediately opens a
successor with `purpose = 'probation'` and `required_windows = 1`. Its `settled_sample_count` is the
recovery count, using machinery that already exists; reaching `windowMinSamples` closes it as
`validated` and opens the real epoch that starvation interrupted. A probation epoch that starves
again simply opens another, so the ledger records every recovery attempt instead of a counter that
records only the latest.

This is the only place where an epoch's `settled_sample_count` is compared against
`windowMinSamples` rather than `rampSamples`. That is deliberate: probation is asking whether the
*environment* can deliver a window's worth of samples, not whether the *hardware* has settled.

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

The authority order matters and must not be inverted. The live observation is the authority for what
the device is running; the ledger supplies only the *cause*, which decides whether the change is
controller-owned or foreign. Reading it the other way — treating `configured_verified_at` as proof
of the running configuration — is what the AxeOS Mutation Constraint forbids, because that column
records a pre-restart NVS readback.

Reconciliation also requires the same `manualConfirmationPolls = 2` confirmation that an external
change requires. A single poll from a device that is still booting must not adopt anything. The
ledger lookup changes what a confirmed observation *means*; it does not change how many observations
are needed.

## What Time Remains Authoritative For

Enumerated exhaustively, so the boundary is testable:

- **Hourly accounting.** `optimizer_hourly` is a wall-clock time series by definition. It is
  reporting, not control, and it keeps its current semantics.
- **Mutation stage deadlines.** A device that never answers must not hold an attempt open forever.
  `defaultRebootDeadline` remains, and it remains the only place a duration terminates anything.
  Its expiry must produce a retryable failure, never a supersession of durable authority.
- **`PhaseStartedAt`, `PendingSince`, `SettledAt`, `opened_at`, `closed_at`.** Retained as
  observability. No control predicate may read them. Being observability-only does not make them
  optional: every transition into a state stamps that state's entry, which is the defect in
  *Terminal transitions do not record their own time*. A timestamp no predicate reads and no
  transition writes is worse than no timestamp, because it reads as a fact.

Every other duration in the optimizer becomes a derived count or is deleted.

## Exact Schema-Version-7 Contract

Schema version 7 replaces version 6. Opening any other nonzero version fails with an explicit
incompatible-schema error. There is no migration, dual reader, or silent reinterpretation, per the
replace-do-not-preserve rule. Existing databases are moved aside; the analysis value of
`bug_optimizer3.db` is preserved by keeping the file, not by reading it.

`optimizer_miners` removes:

- `ramp_until` — replaced by `evidence_epochs.settled_sample_count`;
- `evidence_deadline_at` — replaced by `evidence_epochs.rejected_windows` and the budget;
- `cooldown_until` — replaced by `recovery_healthy_count`; `COOLDOWN` remains a phase, its clock
  does not remain an authority; and
- `consecutive_bad_windows` — dead since introduction.

Removing `evidence_deadline_at` orphans nine state invariants across two validators
(`lib/state.go:4095-4118` and `lib/state.go:4337-4350`). They are not merely deleted. They are
replaced at the same load path by the successors the epoch and the `starved`/`rejected` split make
expressible:

- a `rejected` `HOLD` has no open epoch;
- a `starved` `HOLD` has a closed epoch whose `outcome` is `starved`;
- `settled_at` is nonzero only when the miner has no open epoch;
- `OVERHEAT` and a pending mutation both imply no open epoch; and
- an open epoch's `frequency` and `core_voltage` equal the miner's current point.

`optimizer_miners` adds:

- `recovery_healthy_count INTEGER NOT NULL`, zero outside `COOLDOWN` and `OVERHEAT`, never exceeding
  `recoveryHealthyPolls`; and
- `unreadable_poll_count INTEGER NOT NULL`, zero after any readable poll, never exceeding
  `unreadablePollLimit`.

It does not gain an `evidence_epoch_id`. That column and `evidence_epochs_one_open` would be two
representations of one fact, kept consistent by writing both in one transaction forever after — the
same two-controllers hazard diagnosed in *Two controllers hold incompatible models of one device*,
reintroduced one layer down. The partial unique index is the single representation, and the open
epoch is `SELECT id FROM evidence_epochs WHERE mac_addr = ? AND closed_at = 0`. See *Correctness by
Construction*.

`recovery_healthy_count` and `unreadable_poll_count` are bounded by `recoveryHealthyPolls` and
`unreadablePollLimit`, but those bounds are not encoded as `CHECK` constraints below: the constants
already have one canonical representation, in Go configuration, and a `CHECK` constraint would
create a second one that a constant change could silently desynchronize from. The shape invariants
that are encoded as `CHECK` constraints below — `outcome` populated only once closed, `window_*`
populated only once a window has closed, `purpose` and `required_windows` drawn from closed sets —
are permanent facts about the table's structure, not tunable values, which is the distinction that
decides which mechanism applies.

`hold_reason` replaces `blocked` with exactly `starved` and `rejected`. The enum becomes
`optimized`, `safety`, `manual`, `starved`, `rejected`.

New table `evidence_epochs`:

```sql
CREATE TABLE evidence_epochs (
	id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	mac_addr TEXT NOT NULL,
	frequency INTEGER NOT NULL,
	core_voltage INTEGER NOT NULL,
	purpose TEXT NOT NULL
		CHECK (purpose IN ('baseline', 'trial', 'hold_validation', 'safety_validation', 'probation')),
	required_windows INTEGER NOT NULL CHECK (required_windows IN (1, 2)),
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
		CHECK (outcome IN ('', 'validated', 'rejected', 'starved', 'contradicted')),
	CHECK ((closed_at = 0) = (outcome = '')),
	CHECK ((closed_windows = 0) = (window_sample_count = 0))
);

CREATE UNIQUE INDEX evidence_epochs_one_open
	ON evidence_epochs(mac_addr)
	WHERE closed_at = 0;

CREATE INDEX evidence_epochs_mac_opened
	ON evidence_epochs (mac_addr, opened_at);
```

`purpose`, `required_windows`, and `outcome` are constrained to their closed sets by the `CHECK`
clauses above, not by write-path discipline. `outcome = ''` and `closed_at = 0` are tied by the same
mechanism: an epoch cannot be closed without an outcome, and cannot carry an outcome while open. In
Go, `purpose` and `outcome` are named string types with an exhaustiveness check on every decode and
durable load; the `CHECK` clauses and the Go types state the same fact at the two boundaries where
each is the only mechanism available.

The `window_*` columns are zero or NULL while `closed_windows = 0` and hold the first admitted
window's aggregate once `closed_windows >= 1`; the table-level `CHECK` above enforces that
correlation instead of leaving it to the writer. This is a deliberate, bounded exception to making
window state fully unrepresentable as an illegal value — collapsing "no window yet" and "one window
closed" onto one row is only safe because at most two windows ever exist per epoch (see *The first
closed window is durable*); a third state would need a real table, not a wider constraint. They use
the same encodings as the corresponding `operating_points` columns. `settled_sample_count`,
`closed_windows`, `rejected_windows`, and `missed_polls` are non-negative and monotone within an
epoch; a contradiction closes the epoch rather than decrementing them.

`operating_points` gains one column, `evidence_epoch_id INTEGER NOT NULL`, holding the closed epoch
that produced the record's measurement and zero for `entered` and `starved`. Without it there is no
path from a terminal status back to the evidence behind it, and the requirement stated below is
unenforceable prose: a point can be evaluated by many epochs over its life, so
`(mac_addr, frequency, core_voltage)` does not identify which one. It references only closed epochs,
so it records a completed fact rather than mirroring a live one — the distinction that makes
`evidence_epoch_id` wrong on `optimizer_miners` and right here.

Its status enum splits `unobservable` and gains `starved`:

```text
entered, validated, no_gain, unstable, starved, unobservable, thermal, power, vr_hot
```

`starved` means no admissible window was produced and carries no measurement. Every other terminal
status requires a positive `evidence_epoch_id` whose epoch is closed with `outcome = 'validated'` or
`'rejected'`, and complete measurement columns. It does not require a positive `entry_attempt_id`:
that column is positive only for points the controller entered by mutation, and a validated baseline
is measured at a point the controller never entered, so it legitimately carries zero. The `Point*`
constants become a named `PointStatus` type with an exhaustiveness check at every decode and durable
load, matching `OptimizerPhase` and `HoldReason`.

`mutation_attempts` is unchanged. It is the reference implementation for this design.

## Non-Goals

- Changing the exploration policy, frontier ordering, candidate selection, or the finite-pass
  contract. `RFC_LONG_TERM_OPT.md` was deleted in `f50d087`; the contract it proposed is live in
  `lib/state.go`'s `pass_*` and `pass_reference_*` columns, in `main.go`'s report boundary logic,
  and in `README.md` §Optimizer, which are the current references. This RFC changes how evidence is
  accumulated and how transitions are authorized, not which points are tried.
- Changing the poll transport, `metricsInterval`, HTTP timeouts, or the discovery contract.
- Changing hourly accounting semantics or the 384-hour retention bound.
- Adding a scheduler, work queue, retry framework, or configuration switch. Every new quantity is a
  counter or a reference on an existing row or on the single new durable row.
- Adding a retention or pruning rule for `evidence_epochs`. It grows like `mutation_attempts`, which
  is also unpruned, and at a comparable rate; if either needs a bound, both do, and that is one
  change about ledger retention rather than a clause in this one.
- Preserving readability of schema-version-6 databases.

## Verification

Scope-driven, expanding to the boundaries this change touches.

- `lib` boundary tests for schema version 7: exact column set, index set, epoch monotonicity, the
  one-open-epoch partial index, the `CHECK`-constrained outcome/closed_at and window/closed_windows
  correlations, and rejection of version 6.
- Decode-rejection tests for the new closed sets, per the constructor-and-decode rule: an unknown
  `purpose`, `outcome`, `hold_reason`, or point status in a stored row is rejected at load rather
  than carried into the core, and the exhaustiveness check fails to compile or fails a test when a
  variant is added without updating it.
- A load-path test asserting that an epoch with `closed_windows = 0` yields no window aggregate, and
  that one with `closed_windows = 1` yields a complete one — the invariant the fifteen `window_*`
  columns would otherwise leave to the reader.
- Replacement-invariant tests for each of the five successors to the nine deleted
  `evidence_deadline_at` invariants.
- Controller tests reproducing both production failures from the dataset:
  - a poll sequence with a 0.673 clean-interval rate must produce an admitted window and a validated
    epoch, where the current code produces `unobservable`;
  - a rollback that reaches `restart_requested_at` followed by unreadable polls must leave the
    attempt open and the pending authority intact, and must complete when the device returns.
- Two window-closure tests: at a 0.753 delivered-tick rate a window closes on `targetSampleCount`
  with 30 samples, and below a 0.5 delivered-tick rate it closes on `windowMaxSpan` and is admitted
  at `windowMinSamples` — the two bounds must be exercised separately.
- A safety test asserting that a poll with a non-canonical ASIC grid and unsafe telemetry still
  escalates on that same poll, at every value of `unreadable_poll_count`. This is the boundary the
  unreadable-poll rule must not cross, so it is tested on both sides.
- An ordering test asserting that a miner whose recovery predicate is satisfied and whose live point
  differs from durable current exits `COOLDOWN` — the exact mineira deadlock.
- A recovery test asserting that `COOLDOWN` does not exit before `recoveryHealthyPolls` consecutive
  satisfying polls, that one non-satisfying poll resets the count, and that an unreadable poll
  neither advances nor resets it.
- A starvation test asserting that `starved` `HOLD` exits automatically once `windowMinSamples`
  consecutive samples are admitted, and that `rejected` `HOLD` does not.
- A restart test asserting that an epoch with `closed_windows = 1` resumes against the durable
  stored window and loses at most the partial window.
- A consistency test asserting that every non-`starved` terminal `operating_points` row resolves
  through `evidence_epoch_id` to a closed epoch with `outcome` `validated` or `rejected`, and that a
  miner has at most one open epoch across normal closure, contradiction, and process restart.
- Safety-write canary and mining-write canary per `README.md`, unchanged, before any hardware run.

## Complete Cutover Scope

Delete, without compatibility paths:

- `MinerState.RampUntil`, `MinerState.EvidenceDeadlineAt`, `MinerState.CooldownUntil`,
  `MinerState.ConsecutiveBadWindows` and every read, write, validation, and schema reference;
- `handleEvidenceDeadline` (`optimizer.go:1206-1258`), its single call site at `optimizer.go:356`,
  and the four terminal outcomes inside it, each of which becomes an epoch outcome instead;
- `minerRuntime.firstWindow` and `minerRuntime.deferredWindows`, the `allowOptimization` deferral
  machinery at `optimizer.go:369-391`, and the two-window retention comment it carries — a durable
  first window makes deferral unnecessary;
- the exact-spacing resets at `optimizer.go:1282-1298`, replaced by window-closure validation;
- `resetRuntime`'s authority over durable progress at all 23 call sites, including the coordinator
  callback passed at `main.go:210`, retaining only sample-buffer clearing;
- the `overheatCooldown` wall-clock gate at `optimizer.go:311-322`, the `overheatCooldown` helper at
  `optimizer.go:1738-1744`, and all eight construction sites (`optimizer.go:189`, `202`, `218`,
  `1121`, `1131`; `mutation.go:1508`, `1904`, `1917`), replaced by `recovery_healthy_count`;
- `Settings.OverheatCooldownMins`, its default, its range validation, its override path, and its
  `settings.example.yaml` entry, which are dead once that helper is gone; and
- the `HoldBlocked` constant and every branch that treats measurement failure and measurement
  rejection identically; and
- fifteen `OptimizerStore` write methods — `BootstrapMiner`, `ResetOptimizationPass`, `AdmitTrial`,
  `FinalizeTrial`, `FinalizeBaseline`, `FailMutationAndFinalizeTrial`, `AdoptManualPoint`,
  `AdoptExternalPoint`, `SaveMiner`, `CompleteMiningResume`, `PersistSafetyTransition`,
  `FailMutationAndSave`, `QuarantineMutation`, `SupersedeMutation`, `CompleteMutationAttempt` —
  replaced by `Apply` over the transition set.

Add, per *Correctness by Construction*: the `readablePoll` boundary type and its fallible
constructor; the single epoch-progress type owning `settled_sample_count`, `closed_windows`,
`rejected_windows`, and `missed_polls`; the optional window aggregate returned by the epoch load
path; named `PointStatus`, `EpochPurpose`, and `EpochOutcome` types with one exhaustiveness check
each, called at every decode and durable-load path; and the closed `Transition` set with its single
`Apply`. Five types, three enums, and one write path replace six loose counters, fifteen correlated
columns, fifteen bespoke transactions, and a reset rule restated at 23 sites.

Update in the same change: `README.md` §Optimizer, §Safety, §State, and §Configuration for the
version-7 contract, the `starved`/`rejected` split, and the removed setting; `AGENTS.md`
§Thermal-Control and State Invariants to state the time-is-not-authority rule and the
unreadable-poll rule, and to rewrite the "Repeated overheats extend cooldown, capped at 24 hours"
bullet that this RFC deliberately removes; `settings.example.yaml` where it describes
`rampUpSeconds` and `evaluationWindowMinutes` as deadlines rather than as derived counts, and where
it documents `overheatCooldownMinutes`.

## Logical Commit Sequence

0. Poll-loop attribution: instrument per-cycle wall time and attribute it across HTTP, store, and
   coordination. Fix what it finds. This is a prerequisite for calibrating constants, not for the
   design, and it is separable from everything below.
1. Transitions: replace the fifteen optimizer write methods with the closed `Transition` set and one
   `Apply`. No schema change, no behavior change, no new concept — the same writes in the same order
   inside one transaction boundary. This lands first because every commit below adds a state change,
   and adding them to one write path is a variant each rather than a signature change each.
2. Schema version 7 and the evidence epoch as one cutover: `evidence_epochs`, the `optimizer_miners`
   column changes, `operating_points.evidence_epoch_id`, the typed enums and their exhaustiveness
   checks, the replacement state invariants, boundary tests — together with ramp as
   `settled_sample_count`, window closure by the two bounds, the durable first window, and the
   rejected-window budget. Deletes `ramp_until`, `evidence_deadline_at`, `cooldown_until`,
   `consecutive_bad_windows`, `handleEvidenceDeadline`, `firstWindow`, `deferredWindows`, and the
   deferral machinery.
3. Unreadable polls become non-events for the optimizer; `unreadable_poll_count`; reboot-in-flight
   suppression; instantaneous safety assessment explicitly retained on validated telemetry.
4. `recovery_healthy_count` replaces the cooldown clock; `OverheatCount` moves to the derived
   candidate restriction; `overheatCooldownMinutes` is deleted from settings.
5. Control-flow ordering fix and ledger-aware live-point reconciliation.
6. `starved`/`rejected` split across `hold_reason` and `operating_points`, with the probation epoch
   as the starvation exit.
7. Documentation cutover.

Commit 1 is separable from commit 2 and must stay separate. It is a pure concentration of existing
writes with no schema or behavior change, so it is verifiable against the existing test suite
unchanged — which is exactly the property that makes it worth doing first. It is not a compatibility
shim: it leaves one write path, not two.

Commit 2 is deliberately large. An earlier draft staged the schema ahead of the code that reads it,
which cannot leave a coherent tree: `optimizer.go` reads `state.RampUntil` and
`state.EvidenceDeadlineAt` directly, so a commit that drops those columns and a commit that stops
reading them are the same commit. The durable window and the rejected-window budget are likewise
inseparable — the budget counts rejected windows, which do not exist as a durable concept until the
window closure rule does. Keeping them together is preferred over staging a compatibility path.

## Material Uncertainties

### The scope of the transition refactor

Concentrating fifteen write methods into one `Apply` is correct by the one-owner rule and it is what
makes every later commit a variant rather than a signature change. It is also the largest mechanical
change here, it touches `mutation.go`'s call sites, and it is not required by anything the evidence
section proves. Consequence: if the boundary between "transition" and "milestone append" is drawn
wrong, the exhaustive switch becomes a place where unrelated writes accumulate, and the
concentration turns into a god function. Resolve by holding the line stated above — the four
mutation-milestone appends and `CompareAndSetHourly` stay outside — and by treating any pressure to
add a sixteenth variant that is not an optimizer state change as evidence the boundary was drawn
wrong. If commit 1 cannot pass the existing test suite unchanged, it is not the pure refactor it
claims to be; stop and report rather than adjusting tests.

### Whether the per-poll epoch write belongs in the poll transition

`settled_sample_count` and `missed_polls` advance on every admitted sample, which is one small write
per miner per `MetricsTime` on top of `CompareAndSetHourly`'s existing per-poll write. If commit 0
finds that store time is what pushes the poll cycle past 10 s, this design adds to the very problem
*The tick loss is inside the process* diagnoses. Consequence: a modest regression in tick delivery,
not incorrectness. Resolve with commit 0's attribution: if store time dominates, the sample-advance
transition carries the hourly accounting delta so the cycle emits exactly one transaction per miner
per poll instead of two — which is strictly better than today. The transition set makes that fold a
change to one function; it is the concrete reason to sequence commit 1 first.

### Cooldown exit predicate and thermal safety

Replacing a 120-minute escalating gate with a `rampSamples` thermal dwell is the highest-risk change
here, touches a thermal invariant, and deletes a documented non-negotiable. If `recoveryTemp: 61`
plus a 60-second dwell is insufficient to prove the board has actually shed heat rather than
momentarily dipped, the controller could re-enter a thermal cycle faster than today and oscillate —
a 120× shorter gate is not a small parameter change. Consequence: more overheat episodes and more
restarts, the exact pathology the long-term optimization work was written to stop. Resolve by
instrumenting the existing cooldowns first: log `safeToRecover` transitions and temperature slope
for a full week under the current timer without acting on them, and set `recoveryHealthyPolls` from
the measured settling time rather than from `rampSamples` if the two disagree. Do not ship commit 4
before that data exists. Per `AGENTS.md`, this item requires a dedicated architect agent before
implementation.

### `windowMinSamples` and measurement confidence

`ceil(0.8 × targetSampleCount)` = 24 samples is asserted, not derived. There is a standing open
question about five-minute hash measurement confidence at the full 30 samples; admitting a window at
24 widens the median's confidence interval by roughly 12% and could promote a point that a stricter
window would reject. Consequence: worse selections, not unsafe ones. The two-bound closure rule
confines this to the tail — `windowMinSamples` binds only when the span cap fires first, which
requires a delivered-tick rate below 0.5 — so the ordinary window still carries 30 samples. Resolve
by recomputing the historical medians in `bug_optimizer3.db`'s predecessor databases at 24 and 30
samples and comparing selection outcomes. If the difference is material, raise the floor and accept
slower epochs rather than lowering confidence.

### Constants are calibrated against a defect

*The tick loss is inside the process* makes the in-process cause the leading explanation for the
0.673 clean-interval rate: two miners on independent links losing identical fractions, a ticker that
drops ticks when the receiver is slow, and a synchronous poll cycle. That is strong evidence but not
proof — it has not been measured directly, and it does not exclude a concurrent network component.

Consequence: the design is correct either way, and this is why it deliberately does not assume a
good poll yield. But `maxRejectedWindows = 6` and `windowMinSamples = 24` are calibrated against a
rate that a fix to `pollMiners` may move substantially, and constants tuned to a bug tend to survive
the bug. Resolve by running commit 0 first: instrument per-cycle wall time for one hour, attribute
it across HTTP, store, and coordination, fix what it finds, and re-measure the clean-interval rate
before fixing the constants. Also confirm the rate against the older databases to rule out a
one-session artifact. The constants are cheap to change; the shape of the design is not.

### `unreadablePollLimit` against real reboot duration

`ceil(defaultRebootDeadline / MetricsTime)` = 12 assumes AxeOS reboots complete inside 2 minutes and
that a booting device returns unreadable rather than plausible-but-wrong telemetry. The mineira
evidence shows a booting device returning a *successful* HTTP response with a non-canonical grid at
17 s, which is exactly the ambiguous case. Consequence: if a booting device can return a canonical
grid with wrong telemetry, suppression during reboot could mask a genuine fault. Resolve during the
safety-write canary by capturing every poll across ten deliberate reboots and classifying what AxeOS
actually returns at each stage.

### Interaction with the finite-pass contract

The finite-pass contract — pass semantics, `pass_reference_*` snapshots, and settlement — is defined
by the code, not by a document: `RFC_LONG_TERM_OPT.md` was deleted in `f50d087`, so `lib/state.go`,
`main.go:544-578`, and `README.md` §Optimizer are the only current statements of it. That is itself
part of the uncertainty: the contract's rationale is now only in git history, and it was written
against the phase-and-deadline model this RFC replaces. This RFC changes what "an evaluation
completed" means, and the AB/BA report boundary logic consumes settlement timestamps. Consequence:
report-mode historical boundaries could become unavailable or subtly wrong across the cutover.
Resolve by enumerating every `pass_reference_settled_at` producer and consumer before commit 2 and
asserting the boundary semantics in the schema-version-7 boundary tests.

## Conclusion

The controller already contains a correct answer to this problem. `mutation_attempts` records
irreversible facts in monotone columns, admits nothing from an unreadable poll, and can be
reconstructed by any process at any later time. It is why the mineira rollback is diagnosable to the
millisecond and why safety behavior has been trustworthy across both prior RFCs.

The optimizer instead records deadlines and forgets work. Six hours of runtime produced one failed
mutation, zero measurements, two absorbing states, and two miners pinned at the ASIC floor — and the
database cannot say whether the controller came close even once, or when it stopped trying.

Extending the ledger pattern to evidence collection removes five clocks, two absorbing states, one
deadlock, and one dead column, and replaces them with counters that survive restarts and that state
plainly how far the system got. Making those counters unrepresentable-when-wrong — one owner for
epoch progress, one representation of the open epoch, a poll that cannot be mistaken for evidence —
is what keeps the replacement from becoming the next `ConsecutiveBadWindows`. The result degrades
under a degraded poll yield by taking longer, which is the correct failure mode, instead of by
terminating into a state it cannot describe or leave.
