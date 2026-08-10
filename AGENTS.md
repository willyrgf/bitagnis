# Bitagnis Development Guide for AI Agents

This document is the source of truth for AI-agent behavior and contribution rules in this
repository. It opens with design and engineering principles that apply to any change here, then
states the bitagnis-specific facts and constraints those principles must respect. `README.md`
describes the current controller behavior and operator contract; the source and tests are the
executable description of the current system.

## Design and Engineering Principles

### One Current Design and Complete Cutovers

Optimize the repository as a whole for fewer concepts, code paths, public types and schemas,
duplicated responsibilities, places a future change must touch, and LOC. Each responsibility must
have one clear owner, one canonical representation, and one implementation path.

Reuse code when behavior and ownership are genuinely shared. Prefer extending the existing owner or
extracting a small shared helper over copying logic, adding a second service, or introducing another
representation. An abstraction must reduce total concepts, duplication, future change sites, or LOC
after its call sites are considered. Do not add indirection, interfaces, generic frameworks,
configuration switches, or extension points for hypothetical future reuse. Add a dependency only
with strong justification; prefer the standard library and the existing dependency set.

Reducing LOC is valuable when it deletes duplication, indirection, obsolete behavior, or unnecessary
surface area. Do not make code smaller by compressing readable logic, combining unrelated
responsibilities, or removing validation, safety controls, error context, tests, or necessary
documentation. Clear direct code is simpler than clever short code.

Prefer the best current design over backward compatibility with an inferior internal design.
Breaking internal APIs, CLI contracts, schemas, persisted formats, and documented behavior is
allowed when the replacement is deliberate and complete. When a design changes, complete the cutover
and delete the superseded implementation, types, entry points, aliases, adapters, feature flags,
readers and writers, tests, fixtures, and documentation. Do not deprecate old paths, hide them, or
retain compatibility shims, dual paths, legacy modes, or fallbacks for old behavior — git history is
the archive. Update every current producer and consumer in the same logical change. For a changed
persisted contract, deliberately update or reset its baseline and reject incompatible old data
explicitly; never silently reinterpret old bytes or retain a legacy reader unless an externally
required migration contract has been approved. A breaking change never relaxes correctness, safety,
secret handling, or data integrity.

Fix the underlying design or add missing support properly. Do not introduce hacks, monkey patches,
partial workarounds, fragile schema shims, duplicated wrappers, or parallel implementations. Do not
split an inseparable cutover merely to make individual commits smaller — keep it together instead of
staging a compatibility path. If a correct complete solution is not possible, report the blocker
instead of approximating it.

### Correctness by Construction

Make illegal states unrepresentable. Move correctness obligations out of repeated control flow and
programmer discipline and into representation and construction. A trusted-core value should carry
evidence of the facts its consumers rely on, not just the data those facts were derived from.

Use the smallest Go mechanism that expresses the guarantee:

- A struct for facts that must always travel together, so no path can construct or use one without
  the other.
- A closed set of typed constants for alternatives, paired with an exhaustiveness check that stays
  current whenever a variant is added, removed, or renamed, called at every deserialization and
  durable-state load path — Go cannot make an arbitrary string un-constructible, so that check is
  the enforcement point.
- A distinct named type with an unexported field and a fallible constructor when a value carries an
  invariant beyond its primitive. Every public constructor, `Default`, decode path, and store read
  must go through that constructor so the invariant cannot be bypassed by a second entry point.
- Structural proof over a checked wrapper where it is cheap: a head element plus a slice tail proves
  non-emptiness at compile time; a length check only proves it where that check happens to run.
- An unexported interface method to close a set of implementations to the package when a switch over
  alternatives must stay exhaustive, and single-owner discipline when a value must be written by
  exactly one path.

Parse untrusted YAML, HTTP, CLI, and store data into domain types at the boundary; do not validate a
primitive and then keep passing the primitive through the core. Fallible construction returns a
typed, redaction-safe error; methods on the constructed type preserve the invariant; internal APIs
accept the domain type, never the raw primitive it was built from.

Use a runtime check only for an ambient or changing fact that no single value can prove by
construction alone. Put each such check at the capability that owns the fact, and return an explicit
checked outcome that downstream code is required to consume before it may act. Do not add type
machinery that is more complex than the invalid states, branches, or future change sites it removes.

Test constructor and decode rejection paths and invariant-preserving transformations as part of the
feature's tests, not as separate follow-up coverage.

### When Architecture Is Unclear

Read `README.md`, the relevant implementation, and its tests before deciding that architecture is
unclear. If architecture, ownership, or design-contract direction is still unclear, spawn a
dedicated architect agent before implementation. Pass it the relevant context and these
requirements explicitly: minimize concepts, code paths, public types, duplicated responsibilities,
future change sites, and LOC; allow breaking changes; delete superseded code without compatibility
paths or fallbacks; and divide the work into logical commits.

Ask the architect agent for one target design, the complete cutover and deletion scope, affected
contracts and tests, and a logical commit sequence. Resolve the ambiguity before adding code; do not
use parallel implementations as a substitute for a decision.

Every planning, architecture, and design response must include a `Material uncertainties` section
before implementation begins. List each material choice or assumption for which confidence is not
high. For each, state the choice or assumption, why it is uncertain, the consequence if it is
wrong, and how to resolve or validate it. Write `none` when no material uncertainty remains. Do not
pad the section with routine, low-impact choices. Any item involving architecture, ownership, or
the design contract triggers the architect-agent rule above.

### Commits, Verification, and Reporting

Divide non-trivial work into ordered logical commits; each commit must leave one coherent current
design. Do not create commits unless the user asks. Write commit subjects in lower case, for example
`preserve thermal history across overheat recovery`. Preserve unrelated user changes and never
commit local runtime state.

Verification is scope-driven: run the narrowest command that exercises the changed behavior, then
expand when the affected boundary or risk requires it.

Report what changed, what was deleted, which verification ran, and any remaining unverified risk or
blocker. If verification was intentionally omitted, say so explicitly.

## Bitagnis Non-Negotiables

- Never weaken ASIC-temperature, voltage-regulator-temperature, power, or overheat protections to
  make optimization faster or tests easier.
- Treat every AxeOS mutation as hardware-affecting and high risk: validate it before any request,
  preserve enough durable state to recover, and test failure paths (see AxeOS Mutation Constraint
  below).
- Always change ASIC frequency and core voltage as one complete `OperatingPoint`; never tune or
  apply either field independently. This is Correctness by Construction applied: the type itself
  makes changing one field without the other unrepresentable.
- Never log, print, commit, or persist secrets such as Stratum passwords. Keep real credentials out
  of YAML, tests, fixtures, errors, terminal output, and SQLite.
- Preserve package boundaries and keep `lib` usable without the executable.

## Repository Boundaries

- `main.go` owns process startup, network discovery orchestration, polling, and terminal rendering.
  Keep it thin; domain decisions belong in the optimizer or `lib`.
- `optimizer.go` owns thermal-frontier search, the per-poll evidence-epoch lifecycle (ramp,
  window admission, starvation/probation recovery), safety evaluation, rollback, cooldown, overheat
  policy, and operating-point target selection.
- `mutation.go` owns mutation priority, durable-intent coordination, preflight checks, PATCH/restart
  ordering, durable mutation-attempt milestones, same-MAC rediscovery, reboot proof, readback,
  healthy-mining resumption, startup mining reconciliation, and the optimization gate.
- `lib/bitaxe.go` owns AxeOS HTTP transport, response validation, advertised ASIC settings, and local
  network discovery primitives.
- `lib/settings.go` owns strict YAML decoding, defaults, hostname overrides, and safety-setting
  validation.
- `lib/state.go` owns durable optimizer and pending-mutation state, the evidence-epoch ledger, the
  durable `WindowAggregate` measurement type, evaluated operating-point records, mutation-attempt
  history, exclusive SQLite ownership, and exact schema validation. `Apply` over a closed
  `Transition` set is the one write path for optimizer state; `optimizer.go` and `mutation.go`
  construct transitions, they never write SQL directly.
- `*_test.go` files own executable behavior contracts. Keep controller/optimizer tests in the root
  package and library boundary tests in `lib`.

If responsibility moves between these boundaries, update this guide, current documentation, and
tests in the same change.

## Thermal-Control and State Invariants

These invariants are high risk if violated:

- Automated requests may use only complete frequency/voltage pairs advertised by AxeOS. Off-grid
  manual points may be observed, but must never become automated request targets.
- Check instantaneous safety on every metrics poll, including ramp-up and pending-point
  confirmation. Safety is not deferred until an evaluation window completes.
- Emergency and AxeOS overheat recovery take priority over normal rollback, pending trial
  reconciliation, and upward exploration.
- `PhaseOverheat` is the canonical durable emergency episode and fleet safety block. Only a typed
  pending mutation kind authorizes hardware; optimizer phase and live telemetry never do.
- Never learn or persist AxeOS v2.8.1 emergency configured state `50 MHz / 1000 mV` as a normal
  operating point.
- Host cutoff containment immediately applies the exact minimum advertised complete pair without
  clearing the firmware flag, then retains `PhaseOverheat` until recovery telemetry is safe.
- AxeOS-overheat recovery waits until telemetry is safe, applies the exact minimum advertised
  complete pair, clears the firmware overheat flag with that pair, resets samples, and enters
  cooldown.
- Unsafe telemetry at the exact minimum with no firmware flag creates a mutation-free durable
  emergency hold; never replay PATCH or restart for the same pair.
- Normal safety rollback chooses a validated point with thermal, VR-temperature, and power
  headroom; if no validated point qualifies, use the minimum advertised pair.
- Persist a pending operating-point request before touching the device. Do not evaluate it until
  the same MAC returns after proven reboot and exact complete-pair readback.
- Persist one mutation-attempt record before hardware work, record PATCH and restart milestones
  before their requests, atomically complete the attempt with durable state reconciliation, and
  close mining resumption only after two consecutive safe positive-hash polls.
- A manual operating-point change requires two consecutive observations before adoption. Adoption
  starts a fresh baseline ramp and telemetry window.
- Telemetry samples remain in memory. Durable state contains optimizer control state, the
  evidence-epoch ledger's settled-sample and window counters, evaluated summaries, and
  credential-free mutation lifecycle timestamps — not raw polling history. A closed epoch's first
  admitted window is the one durable exception: it is a stored aggregate, never raw samples.
- Preserve evaluated-point history across overheat recovery and ordinary process restarts unless a
  deliberate schema or policy change says otherwise.
- `COOLDOWN` exits on a durable count of consecutive polls proving the device is actually safe to
  recover (`recoveryHealthyPolls`, a physical dwell derived from AxeOS's own autonomous overheat
  recovery cadence — see `power_management_task.c`), never a timer. Any non-satisfying poll resets
  the count to zero; the poll that reaches the threshold clears `SafetyReason` and opens the
  `safety_validation` epoch in the same transition. This recovery predicate is the sole owner of
  opening that epoch: mutation completion and healthy-mining resumption must not open it. Every open
  epoch must match its exact phase/reason, current point, required-window count, pending-authority,
  settlement, and safety-recovery shape; enforce that invariant both as an `Apply` postcondition and
  on reopen. A validated `safety_validation` epoch atomically opens a fresh two-window baseline for
  the same pass; it never creates a safety-derived `HOLD`, deletes point history, or retries a
  consumed candidate. Schema version 9 rejects version 8 and earlier unchanged; do not repair or
  reinterpret prior semantic contracts. The wall-clock cooldown timer this replaced (and the
  durable field it wrote) was removed as part of the schema-version-7 cutover; do not reintroduce a
  duration-based gate here — prefer a monotone, restart-surviving count, as below. A second,
  distinct emergency interrupting a `COOLDOWN` dwell in progress must reset the count to zero, not
  carry a prior episode's partial progress into the new one — every site that begins a fresh or
  escalating emergency episode (`transitionEmergencyState`'s new-episode branch and its callers'
  equivalents) resets `RecoveryHealthyCount` for exactly this reason.
- Repeated overheats extending cooldown (an `OverheatCount`-derived exploration-restriction ladder
  after repeated episodes) is a distinct, separately-tracked, **not-yet-implemented** invariant from
  the `COOLDOWN` exit predicate above — do not conflate the two. `OverheatCount` itself is durable
  and increments correctly, but no restriction currently reads it, and the RFC's stated derivation
  (an "episode anchor" read from `MinerState.PhaseStartedAt`) does not hold: `PhaseStartedAt` is
  overwritten by ordinary transitions (`ResetPass`, `CompleteBaseline`, `FinalizeTrial`,
  `ResumePassAfterSafety`, `AdoptManualPoint`, mutation completion) that can occur between an
  overheat episode and a later exploration pass, so it cannot serve as a stable anchor for "how
  recently did this miner overheat."
  This is a known, reported gap requiring a design decision (e.g., deriving the anchor from
  `mutation_attempts` history instead), not license to guess at a replacement unilaterally.
- Time may be an input to a predicate; time must never be the authority for a transition. A durable
  count of consecutive satisfying polls degrades correctly under a degraded poll yield by taking
  longer; a wall-clock deadline racing that same yield degrades by failing invisibly. Prefer a
  monotone, restart-surviving count over any new duration-based gate.
- An unreadable poll (unsupported device identity, non-canonical ASIC grid, or incomplete
  telemetry) is a non-event for the optimizer: no phase transition, no cleared pending authority,
  no cleared evidence progress, and no supersession — only a durable count that escalates after
  twelve consecutive misses. It must never suppress instantaneous safety assessment over whatever
  telemetry did validate: a device reporting an unsafe reading alongside a malformed ASIC grid is a
  thermal emergency, not a read failure, and that assessment runs at every value of that count.
- A blocked `HOLD` splits by cause: `starved` (the environment never delivered a usable evaluation
  window) exits automatically once it proves it can, with no timer and no operator step; `rejected`
  (the controller measured the point and it failed) stays terminal until an explicit retune. Do not
  collapse them back into one absorbing state.
- Actual sustained hash rate is the objective. Expected hash rate, attainment, ASIC error
  percentage, and share deltas are diagnostics or quality constraints; expected hash alone must not
  veto a faster safe point.
- Keep SQLite access serialized and keep shared runtime/ASIC caches protected by their existing
  mutex boundaries.

The configured thresholds have distinct meanings: exploration target, hard rollback limit,
emergency cutoff, maximum board power, and maximum VR temperature. Preserve those distinctions in
code, settings, output, and tests.

## AxeOS Mutation Constraint

AxeOS v2.8.1 `PATCH /api/system` writes frequency and voltage separately to NVS, and the running
power task may observe and apply either write before restart. Complete every validation before
PATCH. All current hardware writes use the one coordinator-owned lifecycle:

Do not expand the write surface or claim that PATCH success or pre-restart configured readback
proves active configuration. The configured pair is verified only after this complete lifecycle:

```text
validate -> persist intent -> re-read identity and safety -> PATCH -> restart
         -> rediscover the same MAC -> prove a new boot -> verify
         -> clear intent and reset telemetry -> ramp
```

Do not add a direct actuator, accept configured NVS readback without reboot proof, or bypass
per-miner serialization and fleet-wide normal-mutation ordering. Preserve emergency safety
priority, temporary-disappearance tolerance, and exact same-MAC verification. Mining activation
remains canary-first and explicitly authorized because the firmware risks cannot be
resolved by host-side code.

## Go and API Rules

- Use the Go version declared in `go.mod`. Keep `go.mod` and `go.sum` tidy and review dependency
  changes.
- Format Go changes with `gofmt`.
- Prefer explicit, readable code. Avoid panics in production paths; polling and discovery boundaries
  must continue containing dependency panics so one miner cannot stop the controller.
- Propagate `context.Context` through network operations and honor cancellation.
- Keep HTTP clients bounded by timeouts, connection limits, and response-size limits. Validate
  status codes and decoded telemetry before optimizer use.
- Construct typed request payloads. Do not use unreviewed maps or generic payloads for
  hardware-affecting writes.
- Wrap errors with operation context using `%w` where callers may need the cause. Keep messages
  deterministic and free of secrets.
- Reject invalid and non-finite device values. Do not silently reinterpret malformed AxeOS
  responses as safe telemetry.
- Keep lock scopes small and never hold controller or store mutexes across network requests.
- Comments explain safety constraints, invariants, firmware behavior, or non-obvious tradeoffs—not
  the obvious code or the current task.
- Document new exported APIs and keep the one current public API internally consistent.

## Surface-Specific Rules

- Optimizer: favor deterministic helpers for candidate selection, window summaries, thresholds,
  and cooldown calculations. Add table-driven tests for new state transitions and boundary values.
- Device API: test complete payloads, validation-before-request, non-2xx responses, malformed or
  oversized bodies, timeouts, and recovery payload semantics without contacting a real miner.
- Persistence: validate before every write, preserve crash-recovery state, and test reopen behavior
  for schema or state-machine changes.
- Settings: retain strict `KnownFields` decoding. Pointer-based overrides distinguish omission from
  explicit zero/false values; do not collapse those semantics. Keep examples generic.
- Discovery and polling: preserve bounded worker pools, deterministic output ordering, cancellation,
  and per-miner failure isolation. Expanding beyond local IPv4 `/24` discovery requires an explicit
  design decision.
- Terminal output: never expose credentials or sensitive mining identifiers. Update formatting
  tests when columns, units, colors, or aggregation behavior change.

## Development and Verification

Use standard Go tooling from the repository root:

```sh
gofmt -w <changed-go-files>
go test ./...
go vet ./...
go build ./...
```

Map verification to scope:

- `go test ./lib` for isolated AxeOS client, settings, or persistence changes.
- `go test .` for optimizer, controller, polling, or rendering changes.
- `go test ./...` whenever a package boundary, shared model, configuration contract, or module
  dependency changes.
- `go test -race ./...` for mutex, worker-pool, cache, store, or other concurrency changes.
- `go vet ./...` and `go build ./...` for broad or release-facing changes.
- `go mod tidy -diff` after dependency or import-graph changes.

Do not contact or mutate a real Bitaxe during automated verification. Device-level tests require
explicit user authorization, a named canary, recorded pre-change state, and a recovery plan. Never
turn a canary test into an implicit fleet rollout.

## Tests and Documentation

- Prefer boundary-focused unit, regression, and integration tests. If a bug can recur, add a
  regression test.
- Test safety boundaries on both sides of each threshold and exercise persistence or device errors
  before happy-path expansion.
- Use fake device APIs, in-memory stores, temporary SQLite databases, and controlled HTTP
  transports. Tests must be deterministic and independent of the LAN.
- Keep only small local smoke tests inline; place substantial behavioral coverage in the existing
  test files.
- Update `README.md` when current thermal behavior, settings, output, or run commands change.
- Update `settings.example.yaml` with any current configuration change and ensure its load test
  continues to pass.
- Documentation is part of the change, not follow-up work.

## Repository Hygiene

- `settings.yaml`, `optimizer.db`, and the local `bitagnis` binary are runtime artifacts and must
  remain ignored.
- Never copy a live optimizer database or local settings into tests, fixtures, examples, or
  diagnostics.
- Keep example hostnames, addresses, pool users, and URLs synthetic unless a document explicitly
  records an approved device observation.
- Do not add generated binaries, coverage output, editor state, module caches, or temporary
  artifacts.

## Next Time

- Look for an existing owner and implementation path before adding a new abstraction or type.
- Delete superseded code and compatibility paths as part of the same complete cutover.
- Read the safety tests and check the AxeOS restart constraint before changing mutation behavior.
- Favor construction that makes an invalid state unrepresentable over a new runtime check; add the
  runtime check only when the fact is genuinely ambient or changing.
