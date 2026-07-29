# Bitagnis Development Guide for AI Agents

This document is the source of truth for AI-agent behavior and contribution rules in this
repository. `README.md` describes the current controller behavior and operator contract. The source
and tests are the executable description of the current system.

## Non-Negotiables

- Never weaken ASIC-temperature, voltage-regulator-temperature, power, or overheat protections to
  make optimization faster or tests easier.
- Treat every AxeOS mutation as hardware-affecting and high risk. Validate it before any request,
  preserve enough durable state to recover, and test failure paths.
- Always change ASIC frequency and core voltage as one complete `OperatingPoint`; never tune or
  apply either field independently.
- Never log, print, commit, or persist secrets such as Stratum passwords. Keep real credentials out
  of YAML, tests, fixtures, errors, terminal output, and SQLite.
- Preserve package boundaries and keep `lib` usable without the executable.
- Add dependencies only with strong justification. Prefer the standard library and the existing
  dependency set.
- Verification is scope-driven. Run the narrowest command that exercises the changed behavior, then
  expand when the affected boundary or risk requires it.
- Divide non-trivial work into ordered logical commits. Each commit must leave one coherent current
  design; keep inseparable cutovers together instead of staging compatibility paths.
- Do not create commits unless the user asks.
- Write commit subjects in lower case, for example
  `preserve thermal history across overheat recovery`.
- Preserve unrelated user changes and never commit local runtime state.

## When Architecture Is Unclear

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

## One Current Design

Optimize the repository as a whole for fewer concepts, code paths, public types and schemas,
duplicated responsibilities, places a future change must touch, and LOC. Each responsibility must
have one clear owner, one canonical representation, and one implementation path.

Reuse code when behavior and ownership are genuinely shared. Prefer extending the existing owner or
extracting a small shared helper over copying logic, adding a second service, or introducing another
representation. An abstraction must reduce total concepts, duplication, future change sites, or LOC
after its call sites are considered. Do not add indirection, interfaces, generic frameworks,
configuration switches, or extension points for hypothetical future reuse.

Reducing LOC is valuable when it deletes duplication, indirection, obsolete behavior, or unnecessary
surface area. Do not make code smaller by compressing readable logic, combining unrelated
responsibilities, or removing validation, safety controls, error context, tests, or necessary
documentation. Clear direct code is simpler than clever short code.

### Replace; Do Not Preserve

Prefer the best current design over backward compatibility with an inferior internal design.
Breaking internal APIs, CLI contracts, schemas, persisted formats, and documented behavior is
allowed when the replacement is deliberate and complete.

When a design changes, complete the cutover and delete the superseded implementation, types, entry
points, aliases, adapters, feature flags, readers and writers, tests, fixtures, and documentation.
Do not deprecate old paths, hide them, or retain compatibility shims, dual paths, legacy modes, or
fallbacks for old behavior. Git history is the source archive.

Update every current in-repository producer and consumer in the same logical change. For a changed
persisted contract, deliberately update or reset its baseline and reject incompatible old data
explicitly. Never silently reinterpret old bytes or retain a legacy reader unless an externally
required migration contract has been approved.

A breaking change never relaxes correctness, hardware safety, secret handling, data integrity, or
the thermal-control invariants in this guide.

### Complete Changes

Fix the underlying design or add missing support properly. Do not introduce hacks, monkey patches,
partial workarounds, fragile schema shims, duplicated wrappers, or parallel implementations. Do not
split an inseparable cutover merely to make individual commits smaller. If a correct complete
solution is not possible, report the blocker instead of approximating it.

## Repository Boundaries

- `main.go` owns process startup, network discovery orchestration, polling, and terminal rendering.
  Keep it thin; domain decisions belong in the optimizer or `lib`.
- `optimizer.go` owns thermal-frontier search, telemetry windows, safety evaluation, rollback,
  cooldown, overheat policy, and operating-point target selection.
- `mutation.go` owns mutation priority, durable-intent coordination, preflight checks, PATCH/restart
  ordering, durable mutation-attempt milestones, same-MAC rediscovery, reboot proof, readback,
  healthy-mining resumption, startup mining reconciliation, and the optimization gate.
- `lib/bitaxe.go` owns AxeOS HTTP transport, response validation, advertised ASIC settings, and local
  network discovery primitives.
- `lib/settings.go` owns strict YAML decoding, defaults, hostname overrides, and safety-setting
  validation.
- `lib/state.go` owns durable optimizer and pending-mutation state, evaluated operating-point
  records, mutation-attempt history, exclusive SQLite ownership, and exact schema validation.
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
- Telemetry samples remain in memory. Durable state contains optimizer control state, evaluated
  summaries, and credential-free mutation lifecycle timestamps, not raw polling history.
- Preserve evaluated-point history across overheat recovery and ordinary process restarts unless a
  deliberate schema or policy change says otherwise.
- Repeated overheats extend cooldown, capped at 24 hours. Do not remove this backoff accidentally.
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

Choose verification by scope:

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

## Reporting

Report what changed, what was deleted, which verification ran, and any remaining unverified risk or
blocker. If verification was intentionally omitted for a documentation-only change, say so
explicitly.

## Next Time

- Look for an existing owner and implementation path before adding a new abstraction or type.
- Delete superseded code and compatibility paths as part of the same complete cutover.
- Read the safety tests and check the AxeOS restart constraint before changing mutation behavior.
