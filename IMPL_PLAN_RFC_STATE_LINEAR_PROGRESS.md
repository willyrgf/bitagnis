# Implementation Plan: State-Linear Optimizer Progress

Status: Ready for implementation

Date: 2026-08-07

Source: `RFC_STATE_LINEAR_PROGRESS.md`

Audience: implementing engineer agent

## How to use this document

Read `RFC_STATE_LINEAR_PROGRESS.md` first. It states *what* changes and *why*. This document states
*where* and *in what order*, with concrete signatures, sketches, deletions, and tests per commit.
Where the two disagree, the RFC is the design authority and this plan is wrong — say so rather than
silently following the plan.

Each commit lists work items in dependency order and a checkpoint that must pass before moving on.
Do not reorder work items within a commit unless you can state why the dependency does not exist.

Commit 4 is gated on data that does not exist yet and on an architect agent. Do not implement it in
the first pass. Everything else is unblocked.

Code in this document is a sketch of shape and ordering, not a patch. Names, error strings, and
formatting follow the surrounding file.

## Before you start

Read, in this order:

- `AGENTS.md` — all of it. §Correctness by Construction and §Thermal-Control and State Invariants
  decide several choices below, and this change edits both.
- `README.md` §Optimizer, §Safety, §State — the operator contract you are changing.
- `optimizer.go:146-450` — `controlMiner`, `enforceMinerSafety`, `controlMinerAfterSafety`,
  `observeExternalPoint`. This is the code the RFC is about.
- `lib/state.go:3140-3350` (`saveMinerWithValidation`, `queryMiner`) and `lib/state.go:4237-4400`
  (`validateMinerState*`). This is where the invariants live.
- `lib/state.go:1546-1800` (`StartMutationAttempt`, `AdvanceMutationAttempt`) — the ledger pattern
  you are copying. `evidence_epochs` should read like `mutation_attempts`.

Sanity-check the baseline before touching anything:

```sh
go build ./... && go vet ./... && go test ./...
```

## Ground rules that change decisions here

- **Complete cutover, no compatibility paths.** No dual readers, no feature flags, no "keep the old
  field for now." Schema 6 databases are rejected, not migrated. `bug_optimizer3.db` stays in the
  repo as an artifact and is never opened by the new code.
- **Never weaken a safety control.** Commit 3 is the one that could. Its rule is: an unreadable
  *identity or grid* stops optimizer progress; it never stops the instantaneous safety assessment
  over telemetry that validated. If you find yourself deleting a branch that escalates on an unsafe
  reading, stop.
- **`OperatingPoint` travels as a pair.** Every new API takes `OperatingPoint`, never a loose
  frequency and voltage. `evidence_epochs` stores two columns because SQL has no pair type; the Go
  boundary reconstitutes the pair immediately.
- **Make it unrepresentable before you make it checked.** The six constructions in the RFC's
  §Correctness by Construction are requirements, not suggestions. If a runtime check is easier, say
  why the construction does not work rather than substituting the check.
- **No method without a caller.** If you finish a commit and a type has an exported method nothing
  calls, that is the `ConsecutiveBadWindows` pattern. Delete it or find the missing caller.
- **Do not create commits.** Stage the work as described; the user commits.

## What replaces what

| Today | Replacement | Owner after |
|---|---|---|
| `MinerState.RampUntil` | `EpochProgress.settledSamples` vs `rampSamples` | `evidence_epochs` |
| `MinerState.EvidenceDeadlineAt` | `EpochProgress.rejectedWindows` vs `maxRejectedWindows` | `evidence_epochs` |
| `MinerState.CooldownUntil` | `optimizer_miners.recovery_healthy_count` | `optimizer_miners` |
| `MinerState.ConsecutiveBadWindows` | deleted, nothing replaces it | — |
| `minerRuntime.firstWindow` | `EpochProgress.window` (durable) | `evidence_epochs` |
| `minerRuntime.deferredWindows` | deleted; a durable first window makes deferral unnecessary | — |
| exact-spacing sample resets | window closure bounds + admission predicate | `optimizer.go` |
| `HoldBlocked` | `HoldStarved` and `HoldRejected` | `lib.HoldReason` |
| `PointUnobservable` (two meanings) | `PointStarved` (no evidence) and the measured statuses | `lib.PointStatus` |
| `overheatCooldown()` duration | `recoveryHealthyPolls` consecutive `safeToRecover` polls | `optimizer.go` |
| `Settings.OverheatCooldownMins` | deleted | — |
| bare `Point*` string constants | named `PointStatus` type + exhaustiveness check | `lib/state.go` |
| 15 `OptimizerStore` write methods | one `Apply` over a closed `Transition` set | `lib/state.go` |

Reference counts for the three deleted clocks, as a scope check before and after: `optimizer.go` 48,
`lib/state.go` 56, `mutation.go` 16, `main.go` 3, tests 27. Roughly 150 sites. When you are done,
`grep -rn "RampUntil\|EvidenceDeadlineAt\|CooldownUntil" .` returns nothing.

## Commit 0 — poll-loop attribution

Independent of everything else. Do it first because it calibrates constants used in commit 2, and
because it may be the cheapest real fix in the repository.

The hypothesis, from *The tick loss is inside the process*: `pollMiners` is called synchronously
from the `select` loop at `main.go:263-269`, `time.Ticker` buffers one tick, so any cycle exceeding
`MetricsTime` drops the next tick for every miner simultaneously.

1. Instrument `pollMiners` (`main.go:673`) to record per-cycle wall time attributed across four
   segments: HTTP fan-out (`workers.Wait()`), hourly accounting (`accountHourly`), safety and
   control (`enforceMinerSafety` + `controlMinerAfterSafety`), and mutation coordination +
   rendering.
2. Log a one-line summary per cycle and a percentile summary per hour. Keep it credential-free.
3. Run for one hour against the real fleet, or against a fake transport reproducing the observed
   latencies if hardware time is not available.
4. Report the attribution. If one segment dominates, fix that segment — the obvious candidate is
   serialized store work, not the HTTP fan-out, which is already a bounded worker pool.
5. Re-measure the clean-interval rate afterwards and record it. Commit 2's constants are calibrated
   from that number, not from 0.673.

Do not change `metricsInterval`, the discovery contract, or the worker pool bounds. Do not make
`pollMiners` asynchronous — a poll cycle that overlaps itself breaks the sample-ordering assumptions
this whole design rests on. If the fix requires that, stop and report.

Checkpoint: an attribution report, a named fix or an explicit "no in-process cause found," and a
re-measured clean-interval rate.

## Commit 1 — one write path over transitions

Pure refactor. No schema change, no behavior change, no new concept. It lands first because every
commit below adds a durable state change, and adding one to a single write path is a variant each
rather than a signature change each.

### 1.1 The transition set

`OptimizerStore` has twenty write methods. Fifteen are optimizer state transitions and become
variants; four are monotone single-table mutation-milestone appends and stay; one is accounting and
stays.

```go
// Transition is one durable optimizer state change. The unexported method closes the set to this
// package: a transition cannot be constructed elsewhere, and Apply's switch is exhaustive over it.
type Transition interface{ isOptimizerTransition() }
```

| Variant | Replaces | Line |
|---|---|---|
| `Bootstrap` | `BootstrapMiner` | 432 |
| `ResetPass` | `ResetOptimizationPass` | 541 |
| `AdmitTrial` | `AdmitTrial` | 707 |
| `FinalizeTrial` | `FinalizeTrial` | 845 |
| `FinalizeBaseline` | `FinalizeBaseline` | 1092 |
| `AdoptManualPoint` | `AdoptManualPoint` | 1198 |
| `AdoptExternalPoint` | `AdoptExternalPoint` | 1264 |
| `SaveState` | `SaveMiner` | 1396 |
| `CompleteResume` | `CompleteMiningResume` | 1830 |
| `SafetyTransition` | `PersistSafetyTransition` | 2239 |
| `FailMutation` | `FailMutationAndSave` | 1918 |
| `FailMutationFinalizeTrial` | `FailMutationAndFinalizeTrial` | 1006 |
| `QuarantineMutation` | `QuarantineMutation` | 2010 |
| `SupersedeMutation` | `SupersedeMutation` | 2108 |
| `CompleteMutation` | `CompleteMutationAttempt` | 2387 |

Staying as they are: `StartMutationAttempt` (1546), `AdvanceMutationAttempt` (1648),
`RecordConfiguredVerification` (1746), `RecordFirstPositive` (1793), `CompareAndSetHourly` (2713).
All reads stay. If you find yourself wanting a sixteenth variant that is not an optimizer state
change, the boundary was drawn wrong — stop and report rather than widening it.

Each variant carries exactly the facts its transition needs, as data, with no methods beyond the
marker:

```go
type SafetyTransition struct {
	Expected MinerState
	State    MinerState
	Record   *OperatingPointRecord // nil when the transition records no measurement
}

func (SafetyTransition) isOptimizerTransition() {}
```

### 1.2 `Apply`

```go
// Apply commits one transition. It is the only write path for optimizer state: the only BeginTx,
// the only validation pass, and the only place two rows are made to change together.
func (store *OptimizerStore) Apply(transition Transition, at time.Time) (TransitionResult, error) {
	if at.IsZero() {
		return TransitionResult{}, fmt.Errorf("apply transition: timestamp is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready("apply transition"); err != nil {
		return TransitionResult{}, err
	}
	tx, err := store.conn.BeginTx(context.Background(), nil)
	// ... existing rollback-defer idiom from FinalizeBaseline ...

	var result TransitionResult
	switch value := transition.(type) {
	case Bootstrap:
		result, err = applyBootstrap(tx, value, at)
	case SafetyTransition:
		result, err = applySafetyTransition(tx, value, at)
	// ... one case per variant ...
	default:
		return TransitionResult{}, fmt.Errorf("apply transition: unhandled %T", transition)
	}
	if err != nil {
		return TransitionResult{}, err
	}
	rollback = false
	return result, tx.Commit()
}
```

`TransitionResult` carries what today's methods return through pointer mutation and multiple returns
— the updated `MinerState`, a created row id where one is created. Returning it instead of mutating
a `*MinerState` argument in place removes a class of half-updated-state bug, and it is why the
signature is not `error` alone. Callers that today pass `state *MinerState` and read it back after
the call assign the result instead.

The `default` case is a backstop, not the closure mechanism. The closure is the unexported marker
plus an exhaustiveness test (1.4).

Each `applyX` is an unexported function in `lib/state.go` holding exactly the SQL that method holds
today, in the same order, against `tx`. This is a move, not a rewrite: if you find yourself changing
what a transition writes, you have left the scope of commit 1.

### 1.3 Call-site migration

`optimizer.go` and `mutation.go` change from method calls to `Apply` with a variant. Mechanical:

```go
// before
if err := minerController.states.PersistSafetyTransition(&expected, state, &record, now); err != nil {

// after
result, err := minerController.states.Apply(lib.SafetyTransition{
	Expected: expected, State: *state, Record: &record,
}, now)
if err != nil {
	return err
}
*state = result.State
```

Work through `optimizer.go` first, then `mutation.go`, then `main.go`. The compiler is your
worklist: delete a method, fix what breaks, repeat.

### 1.4 Tests

- An exhaustiveness test that enumerates every `Transition` variant and asserts `Apply` handles each
  without hitting `default`. Use a table of zero-valued variants; the test fails when someone adds a
  variant and forgets the case. This is the enforcement point AGENTS.md requires for a closed set.
- A test that `Apply` rolls back completely on a failure in any multi-row variant — pick
  `CompleteMutation`, force the second write to fail, assert neither row changed.
- **Every existing test passes unchanged.** This is the acceptance criterion for the whole commit.
  If a test needs editing, this is not the pure refactor it claims to be. Stop and report.

### Commit 1 checkpoint

```sh
gofmt -l . && go build ./... && go vet ./... && go test -race ./...
grep -n "func (store \*OptimizerStore)" lib/state.go   # 5 writers + reads, not 20
```

Report the method count before and after and the LOC delta.

## Commit 2 — the evidence epoch cutover

The large one. The RFC explains why it cannot be split further: `optimizer.go` reads
`state.RampUntil` and `state.EvidenceDeadlineAt` directly, so the commit that drops those columns
and the commit that stops reading them are the same commit. The tree will not build until 2.10.

### 2.1 lib: closed sets become named types

In `lib/state.go`, alongside the existing `OptimizerPhase` / `HoldReason` / `SafetyReason` blocks
(`lib/state.go:27-92`):

```go
type PointStatus string

const (
	PointEntered      PointStatus = "entered"
	PointValidated    PointStatus = "validated"
	PointUnstable     PointStatus = "unstable"
	PointNoGain       PointStatus = "no_gain"
	PointStarved      PointStatus = "starved"
	PointUnobservable PointStatus = "unobservable"
	PointThermal      PointStatus = "thermal"
	PointPower        PointStatus = "power"
	PointVRHot        PointStatus = "vr_hot"
)

type EpochPurpose string

const (
	EpochBaseline         EpochPurpose = "baseline"
	EpochTrial            EpochPurpose = "trial"
	EpochHoldValidation   EpochPurpose = "hold_validation"
	EpochSafetyValidation EpochPurpose = "safety_validation"
	EpochProbation        EpochPurpose = "probation"
)

type EpochOutcome string

const (
	EpochOpen         EpochOutcome = ""
	EpochValidated    EpochOutcome = "validated"
	EpochRejected     EpochOutcome = "rejected"
	EpochStarved      EpochOutcome = "starved"
	EpochContradicted EpochOutcome = "contradicted"
)
```

`OperatingPointRecord.Status` changes from `string` to `PointStatus`; `validPointStatus`
(`lib/state.go:4656`) takes `PointStatus`. Add `validEpochPurpose` and `validEpochOutcome` in the
exhaustive-switch style of `validMutationKind` (`lib/state.go:4616`). Call all of them at every
decode and durable-load path — `scanPointRows` (`lib/state.go:2923`), `queryMiner`
(`lib/state.go:3272`), and the new epoch scanner. A closed set only checked on write is not closed.

**`Status` blast radius.** The type change is mechanical but wide: every
`record.Status == lib.PointX` comparison still compiles, but every place a raw string is assigned or
bound does not. `insertPoint` (`lib/state.go:1474`) binds `record.Status` — add the `string()`
conversion at the bind, not at the call sites. Let the compiler find the rest.

`PointStarved` and `EpochProbation` are added here but nothing produces them until commit 6. An
unreachable enum variant is inert; a stringly-typed status is a defect. Do not add them to a write
path yet.

### 2.2 lib: the window aggregate moves into lib

`windowSummary` (`optimizer.go:112-124`), `combineWindowSummaries` (`optimizer.go:1418`), and
`validateWindowSummary` (`optimizer.go:1455`) move into `lib` as one type.

The reason is persistence, not the combine: the aggregate is stored in `evidence_epochs` and read
back by the epoch load path, so `lib` must own the type. The combine itself stays a controller-side
call — see 2.8 — because the predicates that consume the combined value are controller decisions.

```go
// WindowAggregate is a closed evaluation window. It carries the sample count and span that admitted
// it, so a consumer cannot use the measurement without the evidence of its quality.
type WindowAggregate struct { /* unexported fields */ }

func NewWindowAggregate(
	sampleCount int,
	span time.Duration,
	medianHash, expectedHash, attainment float64,
	meanTemp, p95Temp, p95VRTemp, p95Power float64,
	errorPercent *float64,
	acceptedDelta, rejectedDelta int,
) (WindowAggregate, error)

func (aggregate WindowAggregate) Combine(next WindowAggregate) (WindowAggregate, error)
```

Fields unexported with accessors; `NewWindowAggregate` subsumes today's `validateWindowSummary` and
is the only constructor. `optimizer.go` keeps `telemetrySample`, `summarizeWindow`, `percentile`,
and `mean`; `summarizeWindow` returns `(lib.WindowAggregate, error)` from the constructor.

This moves responsibility across a package boundary, so `AGENTS.md` §Repository Boundaries is
updated in commit 7. Before committing to it, confirm no `lib` consumer needs `telemetrySample`; if
one does, the split is in the wrong place — report rather than forcing it.

### 2.3 lib: `EpochProgress`, the single owner

```go
type EpochProgress struct {
	settledSamples  int
	closedWindows   int
	rejectedWindows int
	missedPolls     int
	window          *WindowAggregate // non-nil exactly when closedWindows >= 1
}

// newEpochProgress is the only path from stored columns to a progress value. It rejects the
// combinations the CHECK constraints also reject, so a hand-edited row cannot enter the core.
func newEpochProgress(
	settled, closed, rejected, missed int, window *WindowAggregate,
) (EpochProgress, error)

func (progress EpochProgress) SettledSamples() int
func (progress EpochProgress) ClosedWindows() int
func (progress EpochProgress) RejectedWindows() int
func (progress EpochProgress) ClosedWindow() (WindowAggregate, bool)

// ObserveSample advances settled-sample progress. It takes the count of polls missed since the
// previous sample so the diagnostic counter cannot drift from the progress it describes.
func (progress *EpochProgress) ObserveSample(missedSincePrevious int)

// CloseWindow records an admitted or rejected window. The first admitted window is stored.
func (progress *EpochProgress) CloseWindow(admitted bool, aggregate WindowAggregate) error
```

**There is no reset method.** Per the RFC's *Contradiction ends the epoch*, a change of point,
phase, device identity, or safety ownership closes the epoch as `contradicted` and opens a
successor. An epoch whose subject changed is a different epoch. If you find yourself needing to
empty a live progress value, you are implementing the wrong transition.

No other code increments or zeroes these numbers. If a call site needs a counter changed and no
method fits, add a method — do not export a field.

### 2.4 lib: `EvidenceEpoch` and schema version 7

```go
type EvidenceEpoch struct {
	ID              int64
	MacAddr         string
	Point           OperatingPoint
	Purpose         EpochPurpose
	RequiredWindows int
	OpenedAt        time.Time
	Progress        EpochProgress
	ClosedAt        time.Time
	Outcome         EpochOutcome
}
```

Schema changes in `createOptimizerSchema` (`lib/state.go:3480`), `optimizerSchemaVersion`
(`lib/state.go:18`, 6 → 7), `PRAGMA user_version` (`lib/state.go:3548`), the expected-column map
(`lib/state.go:3380-3444`), and `validateOptimizerIndexes` (`lib/state.go:3684`).

`optimizer_miners`: drop `ramp_until`, `evidence_deadline_at`, `cooldown_until`,
`consecutive_bad_windows`; add `recovery_healthy_count INTEGER NOT NULL` and
`unreadable_poll_count INTEGER NOT NULL`.

`operating_points`: add `evidence_epoch_id INTEGER NOT NULL`.

`evidence_epochs`: exactly as specified in the RFC's §Exact Schema-Version-7 Contract, including
both table-level `CHECK` clauses, `probation` in the `purpose` set, and both indexes. Do not add
`CHECK` constraints for `recoveryHealthyPolls` or `unreadablePollLimit` — the RFC explains why those
bounds stay in Go.

Add `evidence_epochs_one_open` and `evidence_epochs_mac_opened` to the `expected` map in
`validateOptimizerIndexes`; it compares the full index set and fails loudly if you forget.

The two new `optimizer_miners` columns get their writers in commits 3 and 4. Until then
`validateMinerState` requires both to be exactly zero, and each later commit relaxes its own bound
to its constant. A column pinned to zero is not a dual path; an unconstrained one would be.

**The positional-column hazard — read this before editing.** `optimizer_miners` threads its columns
through six parallel positional lists with no compile-time alignment check:

| List | Line |
|---|---|
| `INSERT` column names | `lib/state.go:3157-3172` |
| `VALUES` placeholders | `lib/state.go:3173-3178` |
| `ON CONFLICT DO UPDATE` excluded-set | `lib/state.go:3180-3210` |
| argument slice | `lib/state.go:3211-3250` |
| `SELECT` column names in `queryMiner` | `lib/state.go:3265-3271` |
| scan destination slice | `lib/state.go:3300-3345` |

Removing four columns and adding two means six aligned edits. Do them in this order: change the
expected-column map first and run `go test ./lib` so the schema-validation test names the exact
mismatch, then fix each list against that error, re-running after each. Do not batch the six edits
and hope; a transposed pair of `INTEGER` columns compiles, passes the column-set check, and silently
swaps two values at runtime.

### 2.5 lib: epoch transitions

Commit 1's set gains three variants and several existing variants gain epoch fields. No new store
method appears — that is the point of commit 1.

New variants:

```go
// OpenEpoch opens an epoch for a miner that has none. Enforced by evidence_epochs_one_open.
type OpenEpoch struct {
	State           MinerState
	Purpose         EpochPurpose
	Point           OperatingPoint
	RequiredWindows int
}

// AdvanceEpoch persists accumulated progress. This is the per-poll transition.
type AdvanceEpoch struct {
	Epoch    EvidenceEpoch
	Progress EpochProgress
}

// CloseEpoch ends an epoch with no accompanying record. Contradiction and probation use it;
// decisions that produce a measurement use FinalizeBaseline or FinalizeTrial instead.
type CloseEpoch struct {
	State     MinerState
	Epoch     EvidenceEpoch
	Outcome   EpochOutcome
	Successor *OpenEpoch // opened in the same transaction when the closure implies one
}
```

Existing variants that now also close or open an epoch, in the same transaction, by carrying it:
`FinalizeBaseline`, `FinalizeTrial`, `FailMutationFinalizeTrial` (close + write the record with its
`evidence_epoch_id`); `Bootstrap`, `ResetPass`, `AdmitTrial`, `AdoptManualPoint`,
`AdoptExternalPoint`, `CompleteResume` (open); `SafetyTransition`, `SupersedeMutation`,
`QuarantineMutation`, `FailMutation`, `CompleteMutation` (close as `contradicted`).

Signature changes outside the transition set:

- `BootstrapMiner`'s `rampUp` and `evaluationWindow` parameters disappear — bootstrap no longer
  knows any duration. Update its `main.go` callers.
- The `windowCount` derivation at `mutation.go:545-560` moves to one `lib` helper so the phase →
  epoch shape mapping exists once:

```go
func EpochShapeForPhase(phase OptimizerPhase, reason HoldReason) (EpochPurpose, int, bool)
```

One read method is added, because the open epoch has no mirroring column:

```go
func (store *OptimizerStore) OpenEvidenceEpochFor(macAddr string) (EvidenceEpoch, bool, error)
```

### 2.6 lib: replacement validation

Delete the nine `evidence_deadline_at` invariants at `lib/state.go:4095-4118` and
`lib/state.go:4337-4350`. Add, in `validateCrossTableState` (`lib/state.go:3866`) and
`validateStoredPhaseShape` (`lib/state.go:4082`):

- a `rejected` `HOLD` has no open epoch (assert the `blocked` equivalent until commit 6);
- a `starved` `HOLD` has a closed epoch whose outcome is `starved` (same);
- `settled_at` is nonzero only when the miner has no open epoch;
- `OVERHEAT` and a pending mutation both imply no open epoch;
- an open epoch's point equals the miner's current point;
- every non-`starved` terminal `operating_points` row has a positive `evidence_epoch_id` resolving
  to a closed epoch with outcome `validated` or `rejected`.

The last one must **not** also require a positive `entry_attempt_id`. Validated baselines are
measured at points the controller never entered and legitimately carry zero — confirm against the
bootstrap path before writing the assertion.

Add `validateEvidenceEpoch(epoch EvidenceEpoch, requireID bool) error` modeled on
`validateMutationAttempt` (`lib/state.go:4473`).

### 2.7 main: `readablePoll` at the boundary

```go
// readablePoll is a poll whose identity, ASIC grid, and telemetry all validated. Only a readable
// poll can advance optimizer progress; construction is the proof, so no progress path re-checks.
// Safety assessment deliberately does not require one.
type readablePoll struct { /* unexported: info, asic, point */ }

func newReadablePoll(info lib.Info, asic lib.ASICSettings) (readablePoll, bool)
```

Returns false when `canonicalASICGrid` fails, `supportedSafetyIdentity` fails,
`completeSafetyTelemetry` fails, the live point is invalid, or the point is off-grid.

The signature chain, precisely:

```go
// unchanged — safety runs on whatever validated
func (c *controller) enforceMinerSafety(
	ctx context.Context, state *lib.MinerState, info lib.Info,
	asic lib.ASICSettings, settings lib.Settings, now time.Time,
) (bool, error)

// changed — progress requires proof
func (c *controller) controlMinerAfterSafety(
	ctx context.Context, state *lib.MinerState, poll readablePoll,
	settings lib.Settings, now time.Time, allowOptimization bool,
) error
```

`asic` and `info` fold into `poll`. Callers: `controlMiner` (`optimizer.go:158`) constructs the poll
and returns early if it fails; `main.go:751` does the same. `observeExternalPoint`, `addSample`, and
`evaluateWindow` take `readablePoll`. This asymmetry is the design; do not "clean it up."

### 2.8 optimizer: the per-poll epoch lifecycle

This is the ordering the RFC is about. Implement `controlMinerAfterSafety` to this shape:

```
given (state, poll, settings, now):

 1. epoch, open := store.OpenEvidenceEpochFor(state.MacAddr)

 2. if open and contradicted(epoch, state, poll):
        # point, phase, or device identity changed under the epoch
        Apply(CloseEpoch{Outcome: contradicted, Successor: shapeFor(state)})
        return

 3. reconcile the live point                       # commit 5 moves this here from optimizer.go:294
 4. phase and recovery handling                    # optimizer.go:305-354, minus the deadline branch

 5. if not open:
        # the phase handler decides whether this state warrants evidence; if so it emits
        # OpenEpoch and returns. Nothing accumulates without an epoch.
        return

 6. if epoch.Progress.SettledSamples() < rampSamples(settings):
        Apply(AdvanceEpoch{Progress: progress.ObserveSample(missed)})
        return                                     # still ramping; no window buffering

 7. buffer := addSample(poll)                      # 2.9
    if buffer did not close a window:
        Apply(AdvanceEpoch{Progress: progress.ObserveSample(missed)})
        return

 8. admitted, aggregate := buffer.close(settings)
    if not admitted:
        progress.CloseWindow(false, aggregate)
        if progress.RejectedWindows() >= maxRejectedWindows:
            Apply(CloseEpoch{Outcome: starved, Successor: probationFor(state)})
        else:
            Apply(AdvanceEpoch{Progress: progress})
        return

 9. if epoch.Progress.ClosedWindows()+1 < epoch.RequiredWindows:
        progress.CloseWindow(true, aggregate)      # stores the first window durably
        Apply(AdvanceEpoch{Progress: progress})
        return

10. stored, _ := epoch.Progress.ClosedWindow()
    combined, err := stored.Combine(aggregate)     # controller-side arithmetic
    dispatch on epoch.Purpose:
        baseline           -> evaluateBaseline(combined)
        trial              -> evaluateTrial(combined)
        hold_validation    -> finishManualHold / finishFinalPlacement
        safety_validation  -> finishSafetyHold
        probation          -> Apply(CloseEpoch{validated, Successor: the interrupted shape})
    each dispatch ends in exactly one transition that closes the epoch and writes its record.
```

Three properties to preserve, and to assert in tests:

- Steps 6–9 emit **at most one** transition per poll per miner. If a poll can emit two, the write
  path has re-forked.
- No step reads a wall clock to decide anything. `now` is passed to `Apply` for stamping only.
- Step 2 precedes everything. A contradiction discovered after progress was advanced has already
  written evidence about the wrong subject.

Deletions this implies: `optimizer.go:324-333` (cooldown ramp/deadline construction, becomes an
`OpenEpoch`); `optimizer.go:355-357` (deadline expiry); `optimizer.go:358-360`
(`now.Before(RampUntil)` becomes step 6); `optimizer.go:369-391` (the `allowOptimization` deferral
machinery — a gated window is closed and stored like any other). Keep the `allowOptimization`
parameter: it still gates *starting a mutation*, not accumulating evidence.

`handleEvidenceDeadline` (`optimizer.go:1206-1258`) and its single call site at `optimizer.go:356`
are deleted; its four terminal outcomes become `starved` epoch closures.

`resetRuntime` (`optimizer.go:1328`) keeps its 23 call sites and loses all durable authority: after
this commit it clears `samples`, `lastSampleAt`, `lastPoint`, `lastPhase`, and nothing else. Delete
`firstWindow` and `deferredWindows` from `minerRuntime` (`optimizer.go:39-47`).

### 2.9 optimizer: `addSample` rewrite

Derived constants next to `targetSampleCount` (`optimizer.go:1700`):

```go
func rampSamples(settings lib.Settings) int             // ceil(RampUpTime / MetricsTime)   = 6
func windowMinSamples(settings lib.Settings) int        // ceil(0.8 * targetSampleCount)    = 24
func windowMaxSpan(settings lib.Settings) time.Duration // 2 * EvaluationWindowTime         = 600s
func windowMaxGap(settings lib.Settings) time.Duration  // 3 * MetricsTime                  = 30s
const maxRejectedWindows = 6
```

The current function slices exactly `targetSampleCount` samples off the front and retains the
remainder (`optimizer.go:1300-1310`). Variable-size windows change that contract: a window is the
whole buffer at closure, and the buffer empties.

```go
// addSample appends one readable sample and reports a closed window when either bound is reached.
// Jitter is a data-quality attribute of a window, not a fatal event between two samples.
func (c *controller) addSample(
	poll readablePoll, state lib.MinerState, settings lib.Settings, now time.Time,
) (closedWindow, bool) {
	runtime := c.runtimeFor(state.MacAddr)

	// Contradiction, not jitter: these samples describe a different subject.
	if len(runtime.samples) > 0 {
		previous := runtime.samples[len(runtime.samples)-1]
		if previous.point != poll.point() || previous.phase != state.Phase || !now.After(previous.scheduledAt) {
			runtime.samples = nil
			runtime.maxGap = 0
			runtime.missed = 0
		}
	}

	// Gap accounting replaces the exact-spacing reset. A gap advances the diagnostic counter and
	// is remembered for the admission predicate; it never discards the buffer.
	if !runtime.lastSampleAt.IsZero() {
		gap := now.Sub(runtime.lastSampleAt)
		if gap > runtime.maxGap {
			runtime.maxGap = gap
		}
		if settings.MetricsTime > 0 && gap > settings.MetricsTime {
			runtime.missed += int(gap/settings.MetricsTime) - 1
		}
	}

	runtime.samples = append(runtime.samples, sampleFrom(poll, state, now))
	runtime.lastSampleAt = now

	span := now.Sub(runtime.samples[0].scheduledAt)
	if len(runtime.samples) < targetSampleCount(settings) && span < windowMaxSpan(settings) {
		return closedWindow{}, false
	}

	window := closedWindow{
		samples: runtime.samples, span: span, maxGap: runtime.maxGap, missed: runtime.missed,
	}
	runtime.samples, runtime.maxGap, runtime.missed = nil, 0, 0
	return window, true
}

// admit reports whether a closed window carries usable evidence.
func (window closedWindow) admit(settings lib.Settings) (lib.WindowAggregate, bool) {
	if len(window.samples) < windowMinSamples(settings) || window.maxGap > windowMaxGap(settings) {
		return lib.WindowAggregate{}, false
	}
	aggregate, err := summarizeWindow(window.samples)
	return aggregate, err == nil
}
```

`minerRuntime` gains `maxGap time.Duration` and `missed int` and loses `firstWindow` and
`deferredWindows`. Implausible telemetry no longer reaches here at all — `newReadablePoll` rejected
it — so the `finitePositive` guard block at `optimizer.go:1261-1268` moves into the constructor and
its `resetRuntime` call disappears with it.

### 2.10 optimizer: `evaluateBaseline` and `evaluateTrial`

Both currently branch on `runtime.firstWindow == nil` and combine in process. After the change the
first-window branch is gone — step 9 of the lifecycle handled it — and these functions are only
reached with a combined aggregate, ending in exactly one transition.

`evaluateBaseline` (`optimizer.go:489-556`) becomes:

```go
func (c *controller) evaluateBaseline(
	ctx context.Context, state *lib.MinerState, epoch lib.EvidenceEpoch,
	combined lib.WindowAggregate, poll readablePoll, settings lib.Settings, now time.Time,
) error {
	point := state.CurrentPoint()
	if !qualityHealthy(combined, settings) {
		return c.finalize(state, epoch, lib.EpochRejected,
			baselineRecord(state, point, combined, lib.PointUnstable, now), settings, now)
	}
	records, err := c.states.ListPoints(state.MacAddr)
	// ... existing rebaseline / headroom / next-candidate logic, unchanged ...
}
```

`evaluateTrial` (`optimizer.go:650-702`) is the subtle one. Today it runs `qualityHealthy` and
`trialWindowPredicate` on the *first* window and again on the combined, so a trial can terminate
after one window. Preserve that: the per-window predicates run at step 9, before the first window is
stored, and a failure there closes the epoch as `rejected` with `closed_windows = 1`. Only the
promote/return decision needs the combined value.

Concretely, split it:

```go
// trialWindowAdmissible runs the per-window predicates at step 9. A failure ends the trial without
// waiting for a second window, exactly as the current code does.
func (c *controller) trialWindowAdmissible(
	state *lib.MinerState, window lib.WindowAggregate,
	entered, prior lib.OperatingPointRecord, settings lib.Settings,
) (lib.PointStatus, bool)

// evaluateTrial runs only on the combined aggregate and always ends in one transition.
func (c *controller) evaluateTrial(
	ctx context.Context, state *lib.MinerState, epoch lib.EvidenceEpoch,
	combined lib.WindowAggregate, settings lib.Settings, now time.Time,
) error
```

`entryMarginPositive` still reads the store; it runs in `evaluateTrial`, before the transition is
built, not inside `Apply`.

### 2.11 rendering and accounting

- `classifyAccountingState` (`optimizer.go:79`): `evidencePending` becomes "has an open epoch,"
  passed in rather than derived from a timestamp. Hourly accounting semantics do not change.
- `formatWindow` (`optimizer.go:1334`): the ramp branch at `optimizer.go:1354-1356` becomes
  `ramp %d/%d` over settled samples; the sample-count branch stays. Update the formatting tests.
- `main.go:943-944`: the retune-readiness predicate reads all three deleted clocks. Replace with "no
  open epoch and `recovery_healthy_count` at rest." This predicate authorizes a hardware-affecting
  operation — be careful, and test both sides.

### 2.12 Tests

Add one shared helper, because three tests need the same thing and hand-rolled variants will not be
comparable:

```go
// pollSequence yields deterministic tick times at a given delivered-tick rate: every Nth tick is
// dropped, no RNG, so a failure reproduces exactly. rate 1.0 delivers every tick; 0.75 drops one
// in four.
func pollSequence(start time.Time, settings lib.Settings, rate float64, ticks int) []time.Time
```

`lib/state_test.go`:

- schema-version-7 boundary: exact column set, exact index set, rejection of a version-6 file
  (extend the reopen tests near `lib/state_test.go:420`);
- `newEpochProgress` rejection table: closed windows without a window, a window without closed
  windows, negative counters, counters exceeding `RequiredWindows`;
- `NewWindowAggregate` rejection table: non-finite values, zero sample count, negative span;
- decode rejection for unknown `purpose`, `outcome`, `hold_reason`, and point status, in the style
  of `TestReopenRejectsInvalidPassReferenceSnapshot`;
- one open epoch per miner across closure, contradiction, and reopen;
- every non-`starved` terminal `operating_points` row resolves through `evidence_epoch_id`.

`main_test.go`:

- `TestBaselineEvidenceDeadlineTerminalizesBootstrapRow` (`main_test.go:424`) is the direct inverse
  of this change. Rewrite as `TestDegradedPollYieldStillProducesAValidatedEpoch` using
  `pollSequence` at the commit-0 rate, asserting an admitted window and a validated epoch;
- window closure on `targetSampleCount` at `rate = 0.75`, and closure on `windowMaxSpan` with
  admission at `windowMinSamples` at `rate = 0.45` — two tests, two bounds;
- a rejected window increments `rejectedWindows` without discarding a stored first window;
- `maxRejectedWindows` exhaustion produces a `starved` epoch;
- restart mid-epoch with `closed_windows = 1` resumes against the durable window;
- a trial that fails its per-window predicate on window one closes the epoch with
  `closed_windows = 1` and never waits for a second;
- **at most one transition per poll per miner** — assert against a counting fake store.

Existing safety tests pass unchanged. If one needs editing, you changed behavior the RFC said you
would not.

### Commit 2 checkpoint

```sh
gofmt -l . && go build ./... && go vet ./... && go test -race ./...
grep -rn --include="*.go" \
  "RampUntil\|EvidenceDeadlineAt\|CooldownUntil\|ConsecutiveBadWindows\|firstWindow\|deferredWindows" .
```

The grep must return nothing. Report the LOC delta and the deleted symbols.

## Commit 3 — unreadable polls become non-events for the optimizer

Small, surgical, highest safety risk. Follow the RFC's branch-by-branch table exactly.

In `enforceMinerSafety` (`optimizer.go:161-259`), inside the `canonicalASICGrid(asic) != nil` block:

- **Delete** `optimizer.go:180-184` — `ClearPendingMutation`, `SetFallbackPoint`, and the timestamp
  clears. This is the demolition path that desynchronized mineira.
- **Delete** the `safetyOwned` branch (`optimizer.go:199-209`). It escalates on the controller's own
  prior state, not on a reading.
- **Delete** the `safetyNormal || safetyUnavailable` branch (`optimizer.go:210-212`). It produced
  mineiro's absorbing `blocked` `HOLD` from a poll that carried no information.
- **Keep unchanged** the `firmwareOverheat || firmwareTrip` branch (`optimizer.go:186-198`) —
  `info.OverHeatMode` and `info.Frequency == 50` do not depend on the grid.
- **Keep unchanged** the final `else` branch (`optimizer.go:213-219`) — it fires exactly when the
  assessment is unsafe. If your diff removes it, revert.
- **Delete** the supersession path at `optimizer.go:220-232`.

Add `unreadable_poll_count`: increment when `newReadablePoll` fails, zero when it succeeds, escalate
to a safety-unknown episode only at `unreadablePollLimit`. Relax commit 2's pinned-zero bound.

```go
func unreadablePollLimit(settings lib.Settings) int // ceil(defaultRebootDeadline / MetricsTime) = 12
```

Reboot-in-flight suppression: when `UnfinishedMutationAttempt` (`lib/state.go:2617`) returns an
attempt with `RestartRequestedAt` set and `RebootVerifiedAt` zero, suppress escalation for that
attempt's duration. The attempt's own `defaultRebootDeadline` (`mutation.go:1358`) bounds it —
verify that bound is enforced before relying on it.

Also delete `resetRuntime` on `safetyUnavailable` (`optimizer.go:251-253`).

Tests, mineira regression first: a reboot-in-flight attempt suppresses escalation and still
completes when the device returns. Then: non-canonical grid + unsafe telemetry escalates on that
same poll at every counter value; non-canonical grid + safe telemetry changes nothing but the
counter; twelve consecutive unreadable polls escalate.

## Commit 4 — recovery predicate replaces the cooldown clock

**Blocked.** Do not implement in the first pass. `AGENTS.md` §When Architecture Is Unclear requires
a dedicated architect agent for an item touching the design contract, and this deletes a documented
non-negotiable. The RFC's material uncertainty requires a week of instrumented `safeToRecover`
transitions and temperature slopes before the dwell can be set.

Land the instrumentation now: log `safeToRecover` transitions and temperature slope per poll during
`COOLDOWN` and `OVERHEAT` without acting on them.

When unblocked: `recovery_healthy_count` advances on a satisfying poll, resets on a non-satisfying
one, is not advanced by an unreadable poll, and `COOLDOWN` exits at
`recoveryHealthyPolls = rampSamples`.

`OverheatCount` moves to restricting the next epoch's candidate. **The restriction level is derived,
not stored:** the last safe validated point, relaxed by the count of epochs closed `validated` since
the episode's `PhaseStartedAt` — one query against `evidence_epochs`, which is what
`evidence_epochs_mac_opened` is for. Do not add a column; it would be a second representation of a
fact the ledger already carries, with its own reset rule to get wrong.

Delete `overheatCooldown` (`optimizer.go:1738`), all eight construction sites (`optimizer.go:189`,
`202`, `218`, `1121`, `1131`; `mutation.go:1508`, `1904`, `1917`), and
`Settings.OverheatCooldownMins` with its default, validation, override path, and example entry.

## Commit 5 — control-flow ordering and ledger-aware reconciliation

**Ordering.** The live-point reconciliation at `optimizer.go:294-296` must not precede the phase and
recovery handling at `optimizer.go:311-333` — it becomes step 3 of the 2.8 lifecycle. Make it set a
flag the phase handlers consult rather than returning early. Regression test: a miner whose recovery
predicate is satisfied and whose live point differs from durable current exits `COOLDOWN`.

**Reconciliation.** In `observeExternalPoint` (`optimizer.go:395`), before treating a differing live
point as an external manual change, consult `ListMutationAttempts` for a failed or superseded
attempt whose target equals the live point and whose `ConfiguredVerifiedAt` is nonzero. Adopt it as
reconciliation of the controller's own ledger.

Two constraints that are easy to invert:

- The live observation is the authority for what the device is running; the ledger supplies only the
  cause. Do not treat `ConfiguredVerifiedAt` as proof of the running configuration — it records a
  pre-restart NVS readback, and the AxeOS Mutation Constraint forbids that reading.
- Reconciliation still requires `manualConfirmationPolls = 2` confirmations. A single poll from a
  booting device adopts nothing.

## Commit 6 — the `starved` / `rejected` split

`lib.HoldReason` loses `HoldBlocked` and gains `HoldStarved` and `HoldRejected`. `PointStarved` and
`EpochProbation`, added inert in commit 2, become reachable. Every branch that treats measurement
failure and measurement rejection identically splits by epoch outcome.

Exit predicates:

- `starved` `HOLD`: closing an epoch as `starved` opens a `probation` epoch with
  `RequiredWindows = 1` in the same transition. Its `settled_sample_count` is the recovery count;
  reaching `windowMinSamples` closes it `validated` and opens the epoch starvation interrupted. This
  is the one place `settled_sample_count` is compared against `windowMinSamples` rather than
  `rampSamples` — probation asks whether the *environment* can deliver a window's worth of samples,
  not whether the *hardware* has settled. A probation epoch that starves again opens another, so the
  ledger records every recovery attempt.
- `rejected` `HOLD`: terminal until an operator retune, exactly as `blocked` is today.

Tighten the commit-2 validations that referenced `blocked` to their final form. Test both exits, and
test that the `rejected` exit does *not* fire. Measure the probation cycle rate at `rate = 0.4` —
see the material uncertainty below.

## Commit 7 — documentation

- `README.md` §Optimizer, §Safety, §State, §Configuration — the version-7 contract, the
  `starved`/`rejected` split, ramp and window as sample counts, the removed
  `overheatCooldownMinutes`.
- `AGENTS.md` §Thermal-Control and State Invariants — add the time-is-not-authority rule and the
  unreadable-poll rule; rewrite the "Repeated overheats extend cooldown" bullet that commit 4
  removes; update §Repository Boundaries for the window aggregate moving into `lib` and for
  `lib/state.go` owning the transition set.
- `settings.example.yaml` — `rampUpSeconds` and `evaluationWindowMinutes` as derived counts rather
  than deadlines; `overheatCooldownMinutes` removed.

If commit 4 is still blocked, the AGENTS.md overheat bullet stays as-is and you say so explicitly in
the report.

## Verification

| Scope | Command |
|---|---|
| `lib` only (1.1–1.2, 2.1–2.6) | `go test ./lib` |
| optimizer / controller (2.7–2.11, 3, 5, 6) | `go test .` |
| any commit here | `go test ./...` — every commit crosses a package boundary |
| commits 1 and 2, anything touching runtime maps | `go test -race ./...` |
| before reporting done | `gofmt -l .`, `go vet ./...`, `go build ./...` |

Never contact a real Bitaxe during automated verification. Before any hardware run, the safety-write
canary and mining-write canary in `README.md` run unchanged, with recorded pre-change state and a
recovery plan.

## Do not

- Do not widen `MetricsTime` tolerance as a shortcut. The RFC's first section explains why that
  fixes nothing.
- Do not add `optimizer_miners.evidence_epoch_id`, and do not add a column for the post-overheat
  restriction level. Both are second representations of facts the ledger already carries.
- Do not add a reset method to `EpochProgress`.
- Do not persist raw telemetry samples. Only closed aggregates are durable.
- Do not make `pollMiners` asynchronous.
- Do not let commit 1 change behavior, and do not edit existing tests to make it pass.
- Do not split commit 2 to make it smaller. It is one cutover.
- Do not implement commit 4 before its data and its architect review exist.
- Do not silently reduce scope. If something here is wrong or blocked, finish everything else and
  say precisely what you left and why.

## Material uncertainties

**The transition boundary.** Fifteen methods into one `Apply` is right by the one-owner rule, but if
the line between "transition" and "milestone append" is drawn wrong, the exhaustive switch becomes
where unrelated writes accumulate and the concentration turns into a god function. Consequence: a
worse structure than the fifteen methods it replaced. Resolve by holding the stated line — the four
mutation-milestone appends and `CompareAndSetHourly` stay outside — and by treating pressure to add
a variant that is not an optimizer state change as evidence the boundary is wrong. Hard signal: if
commit 1 cannot pass the existing suite unchanged, it is not a pure refactor.

**The per-poll epoch write.** `AdvanceEpoch` fires on every admitted sample, on top of
`CompareAndSetHourly`'s existing per-poll write. If commit 0 finds store time is what pushes the
cycle past 10 s, this adds to the diagnosed problem. Consequence: a modest tick-delivery regression,
not incorrectness. Resolve with commit 0's attribution: if store time dominates, `AdvanceEpoch`
carries the hourly accounting delta so the cycle emits one transaction per miner per poll instead of
two — strictly better than today. The transition set makes that a change to one function, which is
the concrete reason commit 1 goes first.

**Moving the window aggregate into `lib`.** Required because the aggregate is persisted, but it
moves responsibility across a documented boundary and is the only structural change the RFC does not
explicitly call for. Consequence: if it belongs in `main`, unwinding touches every window call site
again. Resolve during 2.2 by confirming no `lib` consumer needs `telemetrySample`.

**Whether probation can oscillate.** A `starved` epoch opens a probation epoch that can itself
starve in a persistently degraded environment, and nothing bounds the cycle rate. Consequence:
ledger and log churn, no hardware action. Resolve by measuring the cycle rate in the commit-6 test
at `rate = 0.4`; if it churns, the fix is a count of consecutive starved probations gating reopening
— still a count, not a timer.

**`unreadablePollLimit` against real reboot behavior.** Unchanged from the RFC: mineira shows a
booting device returning a successful HTTP response with a non-canonical grid at 17 s, so
"unreadable" and "booting" overlap. If a booting device can return a canonical grid with wrong
telemetry, commit 3's suppression could mask a genuine fault. Consequence: a real fault unnoticed
for up to `defaultRebootDeadline`. Resolve during the safety-write canary by capturing every poll
across ten deliberate reboots. Commit 3 is not fully validated until that data exists.

**Commit 4 is unresolved by construction.** The cooldown dwell is not derivable from anything
currently measured, and the commit deletes a documented non-negotiable. It needs a week of
instrumentation and a dedicated architect agent.
