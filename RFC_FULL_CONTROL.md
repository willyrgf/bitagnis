# RFC: Startup Bitaxe Mining Configuration

- Status: Implemented and canary-accepted; `mineiro` remains explicitly disabled
- Date: 2026-07-27
- Scope: Bitagnis and the two deployed AxeOS v2.8.1 BM1370 board-601 devices

## Summary

Bitagnis should reconcile each explicitly enabled Bitaxe's primary and fallback
Stratum settings during startup.

Mining configuration is low frequency; thermal safety is continuous. Bitagnis
will start fleet polling immediately in a safety-only mode, reconcile mining
settings on one miner at a time, and enable evaluation and upward exploration
only after every selected miner is safe and healthy.

Every AxeOS setting change, including operating-point trials, will use one
controller-owned path:

```text
validate
    -> persist intent
    -> re-read identity and safety
    -> PATCH
    -> restart
    -> rediscover the same MAC
    -> prove a new boot
    -> verify readback
    -> transactionally clear intent and set ramp
    -> reset in-memory telemetry
```

The design adds no mining subcommands, background reconciliation,
mining-specific state table, stored payload, or recoverable secret store. Current
settings plus named environment variables are the desired state. One minimal
pending mutation record is the crash-recovery state. The cutover has no legacy
schema migration, compatibility reader, deprecated API, alias, adapter, or dual
actuation path.

## Material uncertainties

none.

## Resolved canary risks

- **Device-side password logging:** Bitagnis will never log a resolved secret,
  but v2.8.1 logs both NVS key and value when a string write fails. A failed
  password write can therefore expose the password in AxeOS logs. On
  2026-07-27, the owner explicitly accepted this device-side risk for the named
  `mineira` canary and the actions necessary to complete its procedure. This is
  a known accepted security limitation, not a capability Bitagnis can suppress.
- **Fallback-password assurance:** readback can verify fallback host, port, and
  user, but positive hashing verifies only the pool currently in use. It cannot
  prove a write-only fallback password while primary is active. On 2026-07-27,
  the owner authorized a `mineira` fallback failover. The canary subsequently
  selected fallback and produced positive safe hash before primary restoration,
  resolving this assurance item for the observed configuration.
- **v2.8.1 persistence:** the PATCH handler performs independent NVS writes,
  ignores their results, and returns success; its NVS wrapper has no explicit
  `nvs_commit`. A response alone therefore does not prove persistence. The
  `mineira` canary completed same-MAC restarts with exact operating-point and
  readable-mining readback for primary, failover, and restored-primary
  configurations. This resolves persistence for the observed device/config,
  without making a general firmware guarantee.
- **External mutation ownership:** the owner identified and stopped the old
  `../settle` controller. Bitagnis then held the recovered `400/1000` point for
  22 consecutive polls. Two same-MAC snapshots advanced uptime from 299 to 311
  seconds with the point unchanged, safe telemetry, positive hash rate, and
  healthy primary mining. This resolves single-writer ownership for the canary
  run. Process-exclusive SQLite ownership still cannot exclude a controller
  using another database or host; such writers remain unsupported.

No architecture or ownership uncertainty remains: one mutation owner, one
pending-record representation, one startup safety gate, and one transport path
per AxeOS operation.

The code, automated tests, restart-verified mining canary, fallback proof, and
stable single-writer closure are complete. The temporary 24-hour safety-monitor
window was removed, the normal five-minute policy was restored, and a fresh
startup opened the safety gate without PATCH or restart. Acceptance criteria 13
and 14 are complete. `mineiro` remains disabled and must not be enabled without
separate authorization after this accepted canary.

## Authorized canary record

On 2026-07-27, the owner named `mineira` as the canary and explicitly authorized
the read-only inspection, accepted firmware risks, primary verification,
fallback failover if needed, and the single canary mutation/restart procedure.
No authorization extends to `mineiro`.

The read-only preflight at `2026-07-27T20:21:27Z` recorded:

- hostname `mineira`, MAC `d8:3b:da:4b:57:14`, and then-current IP
  `192.168.7.118`;
- AxeOS `v2.8.1`, ASIC `BM1370`, and board `601`;
- uptime `89510` seconds and operating point `400 MHz / 1060 mV`;
- a complete AxeOS-advertised operating point, positive hash rate, primary pool
  selection, complete safety telemetry below every hard limit, no firmware
  overheat mode, and no power fault; and
- no other running Bitagnis controller observed on the host.

Two subsequent direct reads of the same MAC, 5.03 seconds apart, advanced uptime
by 5 seconds with unchanged identity. This validates the deployed uptime unit
and supports the implementation's conservative 5-second uninterrupted-uptime
tolerance.

The detailed non-secret recovery snapshot is held outside the repository with
owner-only permissions. No pool endpoint, user, or credential was copied into
this document.

The operator confirmed that both pools use a required non-secret password
placeholder. An ignored `0600` runtime settings file was generated from the
private readable snapshot, with defaults disabled and only the `mineira`
hostname override enabled. The two placeholder values are held in an ignored
`0600` environment file and never entered YAML, logs, SQLite, tests, or this
document.

The live canary produced the following evidence:

- matching readable settings opened the startup gate without PATCH, restart, or
  secret lookup;
- named force-reapply persisted `mining_pending` before a complete
  primary-plus-fallback PATCH, restarted once, proved a same-MAC uptime
  discontinuity, preserved the complete operating point, verified exact
  readable mining fields and primary selection, cleared durable intent, reset
  telemetry, ramped, and passed two safe positive-hash primary polls;
- a separately authorized failover used an intentionally unreachable temporary
  primary while preserving the recorded fallback. After restart, `mineira`
  selected fallback, produced positive hash, remained safe, and preserved its
  complete operating point. The intentionally unverified rollout retained
  `mining_pending`;
- restoring the original primary configuration replayed that durable obligation
  through a fresh PATCH and same-MAC restart, verified primary selection, and
  cleared the obligation before the final ramp;
- evaluated-point history survived the restarts and safety recovery; and
- `mineiro` was never selected for polling or mutation. Broad same-MAC
  rediscovery observed it read-only while waiting for `mineira`, as designed.

The live run also exposed four production gaps that are now fixed and covered
by regression tests:

- relative `optimizer.db` paths previously produced an invalid SQLite URI;
- the first same-MAC post-boot read could contain transient zero-temperature
  telemetry, causing an unnecessary replay instead of bounded warm-up polling;
- startup health could advance while a manual point still awaited durable
  two-observation adoption instead of waiting for adoption and its fresh ramp;
  and
- substring redaction with a one-character pool placeholder could corrupt
  unrelated error text. Short secret matches now collapse to one deterministic
  generic error instead.

During the canary, an externally requested high-frequency point crossed the
hard ASIC-temperature limit. Bitagnis persisted a complete advertised
`400/1000` rollback, restarted, proved the same MAC and new boot, waited through
transient post-boot telemetry, verified the pair, retained evaluated history,
and passed post-ramp health. This was a successful real-device safety-path
exercise, not part of the planned mining mutation.

Before the old controller was stopped, it moved the device among complete
advertised pairs without local pending intent. Its final `400/1150` and
`550/1060` points each crossed the 66°C hard ASIC-temperature limit. Bitagnis
persisted and verified a `400/1000` safety rollback after each event. Once the
owner stopped `../settle`, the temporary `mineira`-only safety monitor observed
22 stable polls at `400/1000`; temperatures settled at 53°C ASIC and 43°C VR,
board power at 12 W, and hash rate remained positive. Two direct snapshots
confirmed the same MAC, increasing uptime, unchanged point, and healthy primary
mining.

The monitor exited cleanly, its temporary 24-hour evaluation override was
deleted, and the ignored runtime settings again use the normal five-minute
window. SQLite passed its integrity check with phase `BASELINE`, no pending
mutation or overheat recovery, current point `400/1000`, and three preserved
evaluated-point records. A newly built normal-policy process then matched the
deployed mining configuration, opened the startup safety gate after its
required healthy observations, and was stopped before an evaluation window
could complete. A final same-MAC snapshot at 413 seconds uptime retained
`400/1000`, healthy primary mining, and positive hash rate. No canary-closure
step selected or mutated `mineiro`.

## Firmware facts and current defect

The initial devices are:

| Hostname | ASIC | Board | AxeOS |
| --- | --- | --- | --- |
| `mineira` | BM1370 | 601 | v2.8.1 |
| `mineiro` | BM1370 | 601 | v2.8.1 |

`GET /api/system/info` exposes firmware, ASIC, board, hostname, MAC, uptime,
configured operating point, primary/fallback host-port-user fields, fallback-use
status, hash rate, thermal telemetry, overheat mode, and optional `power_fault`.
Passwords are write-only. The exact response and PATCH fields are in the pinned
[OpenAPI schema][axeos-v281-openapi].

The v2.8.1 UI requires a bare pool host without `stratum+tcp://` or a port, and
firmware passes that value directly to `gethostbyname`. Bitagnis must use the
same contract despite the OpenAPI example containing a scheme.
[The UI validation][axeos-v281-pool-ui] and
[startup code][axeos-v281-system] are the source of truth.

The [PATCH handler][axeos-v281-handler] writes fields separately and does not
restart or reload the miner. `GET /api/system/info` reads configured values from
NVS, so immediate readback is not evidence that the ASIC or Stratum process
loaded them.

The former `BitaxeClient.SetOperatingPoint` and
`BitaxeClient.RecoverOperatingPoint` PATCH-only actuators have been deleted.
Operating-point, rollback, overheat-recovery, and mining writes now use the
single restart-verified path in this RFC. Mining writes nevertheless remain
operationally disabled until the material uncertainties and named canary gate
are resolved.

The same handler ignores individual NVS write failures. The pinned
[NVS wrapper][axeos-v281-nvs] also logs failing string values and does not call
`nvs_commit`, although ESP-IDF says explicit commit is required for guaranteed
persistence. [ESP-IDF NVS documentation][esp-idf-nvs] defines those limits.

## Goals

1. Configure one primary and one fallback pool on the required devices.
2. Preserve fleet-wide overheat, ASIC-temperature, VR-temperature, and power
   protection throughout startup reconciliation.
3. Make no mining PATCH or restart when readable fields already match and no
   recovery or explicit password reapply is pending.
4. Recover every interrupted mutation or durably supersede it with a
   higher-priority safety action.
5. Verify same-MAC reboot, post-boot operating point, readable mining fields,
   and current-primary health before ramping.
6. Preserve evaluated operating-point history and cooldown state after the new
   schema baseline is established.
7. Keep real credentials out of YAML, Bitagnis output, errors, SQLite, and
   committed tests.

## Non-goals

The first implementation will not add background drift reconciliation, mining
status/plan/apply subcommands, management modes, automatic pool rollback,
fallback disabling, multiple-controller coordination, firmware adapters,
Stratum V2/TLS, or unrelated AxeOS settings.

It will not detect a password-only change without explicit operator intent or
claim to verify a password AxeOS does not return.

## One mutation owner

Add one private root-package owner, `mutation.go`, and update `AGENTS.md` in the
same change:

- `main.go` starts discovery, polling, and the startup gate.
- `optimizer.go` decides safety actions and operating-point targets.
- `mutation.go` owns priority, durable intent, PATCH/restart ordering,
  rediscovery, reboot proof, readback, and reset.
- `lib/bitaxe.go` owns validated typed HTTP primitives and local discovery.
- `lib/state.go` owns pending intent and exclusive store ownership.
- `lib/settings.go` owns strict decoding, merging, and static validation.

The mutation owner handles only three concrete kinds:

- `operating_point`
- `overheat_recovery`
- `mining_configuration`

This changes actuation, not thermal policy. Automated operating points remain
complete advertised pairs. Normal rollback still chooses a validated point with
thermal, VR-temperature, and power headroom or the minimum advertised pair.
Overheat recovery still waits for `recoveryTemp`, applies the minimum advertised
pair while clearing the firmware flag, rejects emergency sentinel values as
normal points, resets samples, and retains cooldown backoff.

There is at most one active normal mutation across the fleet. Overheat recovery
and hard-safety rollback are never delayed behind normal mining or exploration.
No restart, rediscovery, ramp, or health wait may block the fleet polling
cadence; the coordinator advances from poll and discovery evidence. No store,
controller, or cache lock may span a network request or wait.

Delete direct APIs and controller paths that treat PATCH or NVS readback as
completed actuation. `optimizer.go` must not implement its own restart lifecycle.

## Complete replacement and deletion scope

Implementation is one breaking internal cutover. Delete, rather than wrap or
deprecate:

- `BitaxeClient.SetOperatingPoint` and `RecoverOperatingPoint` as complete
  actuator APIs;
- `controller.reconcilePending`, `pendingTimeout`, and every path that confirms
  or abandons a request from configured NVS readback without reboot proof;
- `PendingRecovery` and mutation-kind inference from
  `PendingFrequency != 0`;
- direct optimizer calls to hardware PATCH methods;
- the discovery path that reduces identity to `[]string` IP addresses;
- GORM `AutoMigrate` as the optimizer schema authority;
- old device-interface methods, fakes, fixtures, formatting branches, and tests
  tied to those contracts; and
- documentation that describes PATCH-only actuation or the old pending-state
  behavior.

Update every current producer and consumer in the same logical change. Do not
retain forwarding methods, deprecated fields, build flags, schema translators,
fallback readers, temporary alternate paths, or tests for deleted behavior. Git
history is the archive.

## Durable pending record and state cutover

Replace the operating-point-only pending representation in `optimizer_miners`
with:

```text
pending_kind
pending_frequency
pending_core_voltage
pending_since
mining_pending
```

The invariants are:

- empty kind: zero pair and zero timestamp;
- operating-point or overheat-recovery kind: one complete validated advertised
  pair and a timestamp; and
- `mining_pending`: an independent Boolean obligation to replay current enabled
  mining settings.

Delete `PendingRecovery`. Do not persist pool data, passwords, hashes, revisions,
phases, retries, or errors. `mining_pending` is not a second workflow or queue:
it preserves the one fact that readback cannot rediscover after a password-only
request. Mining replay requires mining to remain enabled and resolves the current
effective settings and environment values. If any input is unavailable, the
obligation stays pending, mining stays blocked, and safety polling continues.

Persist the applicable operating-point intent or mining obligation before
PATCH. Keep it after cancellation, PATCH failure, restart ambiguity, wrong
identity, reboot-proof failure, or readback mismatch. A later process revalidates
and replays the complete idempotent mutation; it never infers active state from
NVS alone.

After same-MAC reboot proof and readback:

1. transactionally clear intent, update verified durable state, clear manual
   observations, and set `RampUntil`;
2. reset in-memory samples before accepting another sample; and
3. preserve evaluated points, overheat count, and cooldown.

A higher-priority action may replace a normal operating-point intent only in the
same transaction that records the replacement. It never clears
`mining_pending`. This deliberately cancels an unsafe exploration target while
preserving any mining or password-reapply obligation across emergency recovery
and process loss.

Set one new SQLite schema baseline with `PRAGMA user_version`. An empty database
with no application tables creates only the new schema. An exact
current-version/current-layout database opens normally. Any populated database
with a pre-cutover or unknown version, missing or unexpected application tables,
or missing or unexpected columns fails before device discovery or reads with a
deterministic instruction to move aside or remove the runtime database and start
from a fresh baseline.

Bitagnis must not migrate, copy, reinterpret, delete, or modify an incompatible
database. This deliberately discards old optimizer control state and evaluated
history when the operator chooses the new baseline. It preserves all history
across ordinary process and device restarts after that point.

## Process ownership

`OpenOptimizerStore` must acquire process-lifetime exclusive SQLite ownership
before schema work, discovery, or device reads:

1. use the store's sole SQLite connection;
2. set `PRAGMA locking_mode=EXCLUSIVE`;
3. force bounded acquisition with an exclusive transaction; and
4. retain the connection and lock until `Close`.

This needs no dependency or lock file. It excludes another Bitagnis using the
same database, not a process using a different database or a manual AxeOS
writer. Those writers are unsupported. The immediate pre-PATCH recheck narrows
but cannot eliminate that external race.

## Configuration

Mining settings use the existing defaults and hostname overrides:

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
  mineira:
    mining:
      enabled: true
      primary:
        user: worker-mineira
      fallback:
        user: worker-mineira

  mineiro:
    mining:
      enabled: false
      primary:
        user: worker-mineiro
      fallback:
        user: worker-mineiro
```

`enabled` is the only enabling condition and defaults to `false`. Common pool
settings can be defined once while each hostname is authorized deliberately.
Enable the canary first; enable the second miner only after the canary gate.

When enabled, primary and fallback must both be complete after pointer-based
nested merging. Unknown fields, including a literal `password`, fail strict YAML
decoding. Restart and health deadlines are bounded shared mutation policy, not
mining-specific configuration.

### Password-only reapply

A secret-only change uses an ephemeral flag:

```sh
bitagnis --reapply-mining mineira
bitagnis --reapply-mining mineira mineiro
```

The flag requires explicit hostname arguments, applies only to enabled miners,
and creates ordinary durable intent before PATCH. Unknown flags fail. There is
no persistent reapply setting that can accidentally restart miners on every
launch.

Without the flag, matching readable fields cause no mutation even though
Bitagnis cannot know whether existing passwords match the environment.

### Validation and secrets

Before discovery, require:

- complete primary and fallback blocks for each enabled effective config;
- bare DNS hosts or IPv4 addresses without scheme, port, path, control
  characters, or surrounding whitespace;
- ports from 1 through 65535;
- non-empty users and environment names without control characters or
  surrounding whitespace;
- portable environment names matching `[A-Za-z_][A-Za-z0-9_]*`;
- host, user, and resolved-password values of at most 255 UTF-8 bytes; and
- a complete encoded PATCH below the pinned 10,240-byte request buffer.

Do not trim or normalize valid values. Compare observed host, port, and user
bytes exactly.

After discovery and immediately before PATCH, require the same MAC, supported
firmware/ASIC/board, valid uptime and telemetry, no higher-priority mutation, no
overheat or `power_fault`, and values below the distinct hard ASIC-temperature,
VR-temperature, and power limits.

Resolve both password variables only after drift, recovery intent, or the flag
requires a PATCH. Both must exist and be non-empty. Hold them only in the private
typed payload and encoded request body; never pass either to formatting or
logging.

## Device and discovery contracts

Keep observed AxeOS fields flat in `lib.Info`; do not add duplicate public
pool/mining models. Extend `Info` only with:

- `version`, `ASICModel`, and `boardVersion`;
- primary/fallback host, port, and user;
- `isUsingFallbackStratum`; and
- optional `power_fault`.

Desired nested settings are the sole pool representation. The secret-bearing
PATCH type is private.

Change discovery to return one canonical record containing IP and validated
`Info`, ordered by MAC. Do not discard identity into `[]string`. Rediscovery may
update IP only after the same MAC is proven.

`lib/bitaxe.go` should expose typed primitives for complete operating-point
PATCH, complete recovery PATCH, complete primary-plus-fallback mining PATCH,
restart, information reads, and discovery. Delete `SetOperatingPoint` and
`RecoverOperatingPoint` APIs that imply PATCH completes actuation.

The client must reject every redirect; validate before request construction;
bound time, connections, and bodies; honor cancellation; and return status-only
errors for secret-bearing requests. It must never include a secret request or
untrusted response body in an error.

## Startup and reconciliation

After settings load, exclusive store acquisition, and discovery:

1. Require every explicitly named hostname to map to exactly one MAC before a
   normal mining mutation.
2. Load optimizer state and begin bounded fleet polling in safety-only mode.
3. On every poll, handle in order:
   1. firmware overheat and emergency recovery;
   2. hard-safety rollback;
   3. pending operating-point recovery;
   4. the current mining startup action; and
   5. read-only startup health.
4. Suppress telemetry-window evaluation, candidate selection, and upward
   exploration until the startup gate opens.

Enabled miners reconcile in MAC order:

1. Read identity, uptime, operating point, readable mining fields, and safety.
2. If readable fields match and neither `mining_pending` nor the force flag
   applies, send no mining PATCH or restart.
3. Otherwise resolve secrets, validate the complete payload, and persist mining
   obligation.
4. Re-read immediately before PATCH; stop the normal action if identity,
   operating point, readable settings, or safety changed.
5. PATCH complete primary and fallback configuration, then request restart.
6. Rediscover until the same MAC returns, tolerating temporary disappearance and
   DHCP movement.
7. Prove a new boot from uptime discontinuity relative to pre-restart uptime and
   elapsed monotonic wall time. Offline observation alone is not proof.
8. Verify that the complete post-boot operating point exactly matches the
   pre-restart point before preserving optimizer state. Route an unsafe,
   emergency, or mismatching point through safety policy and keep mining
   pending.
9. Verify exact readable mining fields and require primary, not fallback, during
   normal recovery.
10. Clear `mining_pending` and start a fresh ramp.
11. After ramp-up, require two consecutive valid polls with positive hash rate,
    no fault or overheat, and safe instantaneous telemetry.
12. Continue to the next miner only after health succeeds.

Readable readback plus reboot proof shows that the new boot saw the returned
non-secret NVS values. It does not verify either password. Positive hashing while
fallback is false verifies only the active primary path.

When every selected miner is safe and healthy, discard startup samples, set a
fresh per-host ramp, and open optimization.

## Failure behavior

A normal mining rollout gets one attempt per miner per launch. Bounded polling
inside that attempt is not a retry. Emergency behavior keeps its independent
safety-driven retry rules. After a normal failure, an in-memory attempt latch
prevents the still-durable mining obligation from replaying again in the same
process; the next launch may replay it.

- Validation or intent-persistence failure sends no mutation.
- PATCH failure sends no intentional restart and leaves intent pending because
  fields may have changed partially.
- Restart ambiguity, wrong identity, failed reboot proof, readback mismatch, or
  primary health failure leaves rollout blocked before the next miner.

While blocked, Bitagnis continues safety polling, preserves optimizer history,
reports the affected miner without pool users or secrets, and avoids a normal
restart loop. Cancellation exits with durable intent intact. The next launch
revalidates and replays it.

Automatic pool rollback is omitted because AxeOS cannot return old passwords.
The recovery source is the current validated settings and environment values.

## Security limits

Pool configuration redirects hash power and carries credentials. Bitagnis must
remain on the trusted local discovery scope, verify MAC after restart, reject
redirects and unknown firmware, redact pool users, use only synthetic test
secrets, and never persist resolved secrets.

The v2.8.1 transport is plain HTTP, so confidentiality depends on the trusted
LAN. The device-side NVS logging risk is an implementation gate, not something
host-side redaction can solve.

## Verification

Automated tests must cover:

- strict nested settings merge, disabled-by-default behavior, hostname-scoped
  enablement, literal-password rejection, validation boundaries, and no
  environment resolution on a no-op;
- exact typed payloads, validation-before-request, restart transport, redirect
  rejection, bounded bodies/timeouts, cancellation, and status-only secret
  errors;
- exclusive second-store failure, serialized access, exact current-schema
  reopen, rejection of every old/unknown/partial schema without modifying it,
  post-baseline history preservation, persist-before-PATCH, ambiguous-failure
  retention, replay of both operating-point intent kinds, and preservation of
  the mining obligation through safety preemption;
- canonical MAC discovery/order, named-host completeness, DHCP movement,
  wrong-MAC rejection, uptime reboot proof, and rejection of offline-only or
  NVS-only proof;
- fleet safety polling during every restart/ramp/blocked state, emergency
  preemption, first-miner failure stopping the normal rollout, and exploration
  remaining gated;
- crash injection after intent save, around PATCH/restart, around reboot proof,
  and around durable clear/reset; and
- synthetic secret sentinels absent from formatting, errors, logs,
  panic-recovery output, SQLite bytes, and terminal output.

No automated test contacts a real miner.

### Authorized canary

With explicit authorization, a named canary, recorded pre-change state, and a
recovery plan:

1. Record exact identity, uptime behavior, non-secret pools, and operating point.
2. Resolve the device-side password-logging decision.
3. Confirm matching settings cause no mutation.
4. Enable only the canary with its recorded desired pool fields and
   environment-supplied passwords; do not invent an unprovisioned worker.
5. Confirm one complete PATCH, one restart, uptime discontinuity, same MAC, and
   exact readable fields.
6. Confirm primary selection and two safe positive-hash polls.
7. If required, perform a separately authorized fallback failover.
8. Confirm a fresh optimizer ramp with evaluated history preserved.
9. Restore the recorded primary settings through the same path after any
   deliberate failover.
10. Exercise `--reapply-mining` once for the named canary.

Do not enable the second miner until the canary succeeds.

## Delivery plan

1. **`enforce one controller process per optimizer store`** — acquire exclusive
   SQLite ownership and test bounded second-opener failure; no device change.
2. **`make device mutations durable and restart-verified`** — cut over the
   schema baseline, canonical discovery, mutation owner, restart proof, every
   operating-point/rollback/overheat path, tests, `AGENTS.md`, and `README.md`;
   reject old databases and delete every old API, field, reader, writer, and test
   in the same inseparable safety commit.
3. **`add restart-verified startup mining configuration`** — add
   disabled-by-default settings, flat observed fields, comparison, typed payload,
   redaction, force flag, safety-only gate, sequential rollout, replay, readback,
   primary health, blocked behavior, `README.md`, and
   `settings.example.yaml` as one complete cutover.

The authorized canary is an operational gate, not a code commit.

## Acceptance criteria

The RFC is implemented when:

1. Existing settings files remain valid and mining remains inactive unless
   explicitly enabled.
2. Every mutation uses durable pending state and proven restart semantics.
3. A matching mining config causes no PATCH, restart, or secret resolution.
4. Drift or named `--reapply-mining` produces a durable mining obligation,
   complete PATCH, and verified restart.
5. Every crash boundary safely replays or remains blocked.
6. Safety polling continues while only optimization is gated.
7. Emergency actions outrank normal mining, and first-miner failure prevents
   mutation of the second.
8. Restart proof requires the same MAC and uptime discontinuity.
9. Primary health requires safe telemetry, primary selection, and two positive
   hash polls, not accepted shares.
10. Real passwords never enter YAML, Bitagnis output, errors, SQLite, or
    committed tests.
11. The state cutover creates only the new baseline, rejects old databases
    without modifying them, and contains no migration or old representation.
12. A second Bitagnis using the same optimizer database cannot start.
13. The material uncertainties are resolved and recorded.
14. The named canary passes before the second miner is enabled.

## Future work

Background reconciliation, mining subcommands, rollback profiles, stored mining
revisions, other secret providers, distributed coordination, fallback disabling,
newer-firmware adapters, Stratum V2/TLS, and firmware or fleet upgrades require
separate designs.

[axeos-v281-openapi]: https://github.com/bitaxeorg/ESP-Miner/blob/v2.8.1/main/http_server/openapi.yaml
[axeos-v281-handler]: https://github.com/bitaxeorg/ESP-Miner/blob/v2.8.1/main/http_server/http_server.c
[axeos-v281-nvs]: https://github.com/bitaxeorg/ESP-Miner/blob/v2.8.1/main/nvs_config.c
[axeos-v281-system]: https://github.com/bitaxeorg/ESP-Miner/blob/v2.8.1/main/system.c
[axeos-v281-pool-ui]: https://github.com/bitaxeorg/ESP-Miner/blob/v2.8.1/main/http_server/axe-os/src/app/components/pool/pool.component.ts
[esp-idf-nvs]: https://docs.espressif.com/projects/esp-idf/en/v5.1.3/esp32/api-reference/storage/nvs_flash.html
