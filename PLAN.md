# AAS-Camel-DMP — Plan

Companion to [SPEC.md](SPEC.md). This file holds the current state and the open
work only; finished work is described in SPEC and the git history. The previous
plan (milestones M0–M11, workstreams W0–W6, phases R0–R8, the retirement audit
and the P3/P5 work logs) is in the git history.

## Current state (2026-10-22)

- The DMP is one Java process (SPEC §3). The Python management node,
  per-asset containers, standalone file configuration, examples, `--compare`
  mode and BaSyx v1/VAB are removed; the parent workspace's Compose stacks run
  one `dmp` service with the delegation origin `http://dmp:8080`.
- Discovery is need-to-know with an event-only lifecycle (SPEC §4); Kafka and
  MQTT event transports both work; event-sourced AAS Property values use a
  separate value feed. The parent workspace's stacks use Kafka only, BaSyx's
  MQTT eventing is off (2026-10-18; both carry the same submodel events).
- Ordinary and bridge mappings (SPEC §6, §7), Lua and the AID 1.2 bindings for HTTP (including `longpoll`/`sse`
  observation and static `htv_headers`), MQTT, OPC UA (including
  server-described structures), Modbus and SNMP v1/v2c (SPEC §5.2) are
  implemented as specified. Lua receives AAS elements
  in one typed value form (SPEC §5.3) and OPC UA values without an envelope,
  checked against the AID (SPEC §5.2).
- One mapping model with two triggers (2026-10-23): a mapping is triggered by
  its sources (ordinary, SPEC §6) or by an Operation call (bridge, §7), and
  either kind may have a response half (ResponseSources projecting what its
  Sinks were sent, or an action's reply). Sources without a positive polling
  interval are event-based; a mapping evaluates one at a time; an Operation's
  ResponseSinks have one writer. Actions stay reachable from Operations only.
  Each Source's `Evaluation/Mode` (`OnUpdate`, `OnChange`, `Cached`,
  `ReadAtEvaluation`) replaces `ExecutionSemantics/TriggerSourceIds`; Lua sees
  the trigger as `meta.sources.<id>.trigger`.
- TimeSeries LinkedSegments on InfluxDB 2 (SPEC §5.5): Sinks write one
  acknowledged point per row, Sources poll the latest row, both within the
  segment's period; the series' Record is the row shape.
- The project is AAS-Camel-DMP: Java packages and Maven groupId
  `dk.aau.smartproductionlab` (2026-10-22); the `dmp.*` modules keep their
  names.
- Build: Java 21, Camel 4.22, AAS4J 2.0.1, Milo 1.1.6, PLC4X 0.13.1,
  LuaJ 3.0.1, Kafka clients 4.3.1, influxdb-client-java 8.0.0, Netty 4.1.136
  (one line, through the Netty BOM), MQTT 5 only, JUnit 5 only. Only
  `dmp.component` ships a logging backend.

### Verification

```bash
mvn -o clean test                  # unit and route tests (about 2.5 min)
mvn -o clean test -Pintegration    # plus real Kafka, SASL/TLS and packaged-process tests (CI)
mvn -o test -pl dmp.component -Pcapacity   # stress test only; size with -Ddmp.stress.capacity
```

The image (`Dockerfile` at the root, built locally as `dmp:java`) builds the
jar from the sources, so `docker compose build dmp` needs no Maven step.
Tests tagged `integration` or `capacity` are skipped by a plain `mvn test`,
also when selected with `-Dtest`; use the profile. All tests start embedded
brokers (KRaft Kafka, MQTT 5) and need no external services.
Do not run Maven while the IDE re-imports POMs: the IDE can rewrite
`target/classes` underneath a running build.

## Review fixes before the evaluation (2026-10-23)

A review of all modules (2026-10-23) found bugs the tests miss, hot-path
costs that would distort E2–E5 ([EVALUATION.md](EVALUATION.md)), and DataBridge
leftovers. The work is grouped by cause, so that one change settles several
findings. Every step lands with a regression test that fails before it.

Order: H0, then H1 and H2 (independent), H3, H4, H5. H6 follows the paper's
measurements unless decided otherwise. The E2/E3/E4 pilot (EVALUATION §7)
starts after H1–H3; E5 after H4.

### H0. Delete dead DataBridge code — done (2026-10-23)

No behaviour change; the unit suite passes (607 tests, the 40 fewer than
before tested only the removed code), and so does the integration profile. Removed: per-sink transformer chains
(FanOut, bridge, `RouteConfiguration.datasinkMappingConfiguration`), Lua's
non-keyed input modes (`sourceKeys`, `keyedInput`, `valuePath` on `lua:`), the
unmanaged `DmpRouteBuilder` path and `DmpComponent`'s empty orchestrator and
health-check registry, the bridge's GET branch and its fallbacks to the action
name (`OperationContext` is built explicitly; `operationSourceId`,
`commandSinkId`, `responseOrder` and `restMethod` are gone from the trigger
data), `paho-mqtt5-inout`'s `correlationHeader` and inline credentials and with
them the reply registry's 60 s fence (in-flight calls are still bounded at
8192), the OPC UA envelope converters, normalizer and method-metadata
service, the PLC4X option deserializer, `AASComponentUtil`,
`plc4j-driver-bacnet`, `DMP_MANAGED_DATA_PLANE_GROUP_IDS`, stale test
resources and javadoc references. CI runs on `main`. Kept: the one-shot
`DmpComponent.installBundle(String, RoutesConfiguration)`, which 65 tests use.

### H1. One clean evaluation — done (2026-10-23)

- An evaluation runs on an exchange of its own (`FanIn.evaluate`), so no source
  header reaches a sink. A new end-to-end test polls an OPC UA variable and
  writes another one; on the H0 code the sink read the polled node instead.
- Values are frozen once (`JsonCodec.freeze`) when the cache accepts them and
  shared after that by the snapshot, the sinks (`PayloadAssembler` shares
  contributions), the response half and the per-sink base properties. One
  evaluation of a 20 000-item payload allocated 10.8 copies of it at H0 and
  1.0 now (measured with `ThreadMXBean`; the test asserts under 2).
- A trigger the readiness gate holds back stays pending until an evaluation
  passes the gate; only OnChange sources compare values.
- Response writes choose among their endpoints instead of `toD`; FanIn resolves
  its collector endpoint once; the DEBUG log formats only when enabled.
- `ConnectorInvocationException` takes a cause; `DMP_SOURCE_READ_FAILED` keeps
  the read's error and category; `meta.invocation.error.code` falls back to
  `DMP_MAPPING_FAILED`, never a Java class name; failed response writes log
  their exception.
- O3: JFR events `dk.aau.smartproductionlab.dmp.Evaluation` and `.SinkWrite`
  (SPEC §10).

### H2. Lua budget (`dmp.camel-lua`)

One replacement for the hook settles both safety and speed.

- **Hook.** A minimal instruction counter replaces LuaJ's `DebugLib`, which
  runs per instruction and makes all Lua about 3× slower (measured). The
  counter is a `DebugLib` subclass set as `Globals.debuglib` (a public field),
  whose `onCall`/`onReturn` do nothing, so the `debug` table and the `package`
  stand-in are no longer loaded. The timeout becomes an `Error`, not a
  `LuaError`: LuaJ 3.0.1's `pcall` catches `LuaError` and `Exception` only
  (checked in its bytecode). Today `pcall` swallows the timeout and a loop
  around `pcall` never ends.
- **Library calls.** Functions that run as one instruction in Java get caps:
  - the result length of `string.rep` and `table.concat`;
  - the input size of `json`.
- **Patterns.** `find`/`match`/`gmatch`/`gsub` get a matcher of the DMP's own
  that counts steps against the same budget. A length cap does not bound
  backtracking: the review's reproducer has a 2000-character subject.
  LuaJ's matcher is internal to its `StrLib` and the build has no LuaJ
  sources, so this is a port of Lua 5.2's pattern matcher (`lstrlib.c`, MIT),
  checked against LuaJ's results in tests.

  `LuaJsonCodec` gets a depth and cycle guard (today a cyclic result is a
  `StackOverflowError`). `print` goes to the log at DEBUG, not to stdout.
- **Shared prototype.** Identical Lua shares one compiled prototype across
  mappings, as SPEC §8 states (today the cache key includes the mapping).
- **SPEC §8:** the budget is wall time from the start of the invocation.
- **Tests:** the review's reproducers as unit tests: a `pcall` loop,
  `string.rep("x", 2^30)`, a backtracking pattern and a cyclic result.

### H3. Reconciliation and events (`dmp.management`)

- **Structure without values.** One helper strips values recursively
  (Property, SMC, SML, MLP, Range, File).
  - The fingerprint uses it; today only a top-level Property loses its value.
  - `relevant()` drops an SME_UPDATED of any element type whose structure is
    unchanged.
  - This ends the reinstall on every write of a collection sink, and the
    recompiles on value writes.
- **Retry what failed.** Dependency reads that failed transiently, and probes
  of new AIMC ids, go into a retry set that the next pass re-reads. Today the
  retry refreshes an empty set forever, the mapping stays blocked and
  `/health` stays 503.
- **Events during a pass.** Events accepted while a pass runs are checked
  again against the index that pass produces; during the initial scan, all of
  them. Today they are filtered against the old index (an empty one at
  startup) and their offsets committed.
- **Bridge fingerprint.** It includes the Operation when Sources address its
  variables. Today a changed `invocationDelegation` keeps the old REST path.
- **Event types.** Unknown and `AAS_*` event types are ignored, not a full
  rescan. A malformed event is logged.
- **Ingestion.**
  - The event identity is read without parsing the whole event.
  - One parse per event is shared by the control path, the value-feed hub and
    every listener (today 3 + N parses, N = sources on that submodel).
  - The Kafka control consumer polls batches and commits per batch (today
    `maxPollRecords=1` and a synchronous commit per record).
  - SPEC §4.2's lazy parsing then holds.
- **`/health`.** Ready means the initial scan has completed and the event
  transport is connected (SPEC §4.4, README). Mapping failures show only in
  `/management/status`.
- **Swap.**
  - The REST path stays registered and answers "unavailable" during a swap;
    today the caller gets a 404.
  - The fence is checked before the old bundle is retired, so a fence during
    the drain no longer leaves the mapping without a bundle.
- **Cost per pass.** Reverse dependencies are built once per pass, and the
  conflict checks run only over the changed mappings.
- **Tests:**
  - an AID that answers 503 at startup recovers;
  - an AIMC deleted, or an AID created, during the initial scan;
  - an SMC sink written at 1 Hz does not reinstall;
  - a changed `invocationDelegation` reinstalls;
  - reconcile time at N = 1000.

### H4. Connector boundaries

- **AAS client (`dmp.camel-aas`).**
  - One URL builder encodes the idShortPath (`Items[0]` fails on every write
    today).
  - Every data-plane request carries the repository authorization, shared with
    discovery (today discovery succeeds and every sink or read gets 401).
  - SMC/SML sinks are written as ValueOnly, with leaves as text by their
    declared valueTypes. Check against BaSyx first.
  - Reads use `sendAsync` with a timeout instead of a new thread per read.
  - No INFO line per write.
- **Credentials.** Every credential is bound in the Camel registry, as MQTT's
  already are: the OPC UA user and password, HTTP Basic, the SNMP community.
  None goes into a URI. This fixes the encoding bugs (`@`, `&`, `#` in a
  password) and the clear-text exposure (Deferred: the credential redaction
  audit).
- **HTTP.** One option builder for polls, sinks, observation and actions:
  - automatic retries off; today they are disabled only with `htv_headers`,
    which contradicts SPEC §5.2;
  - connect and response timeouts of 30 s, as actions have (today 180 s for
    sinks);
  - HTTP/1.1.

  Long polls back off on connect timeouts; SSE gets an idle timeout.
- **MQTT.**
  - The reply topic is subscribed once per endpoint, not once per call.
  - The publish no longer waits behind the subscription under the endpoint
    lock.
  - Payloads are decoded as JSON whatever the schema's type (today a scalar
    schema hands Lua bytes).
  - The default QoS is decision 2 below.
- **OPC UA.**
  - A failed subscription setup deletes the subscription it created and backs
    off (today it leaks one every 500 ms onto the shared session).
  - Browse paths are resolved once per session for Read and Call (SPEC §5.2).
  - NodeIds are parsed without the regex that cuts at `)`.
  - The device-schema check runs on the first Good sample.
  - Read and Write time out after 30 s.
  - Int64 writes are range-checked.
  - Security is decision 3 below.
- **SNMP.** Printable UTF-8 octet strings come out as text.
- **Tests:**
  - list-index sink and read;
  - a secured repository;
  - a password with `@&`;
  - an HTTP sink to a hung server;
  - a scalar MQTT source;
  - a bad NodeId leaves no subscriptions behind;
  - request/reply with 32 concurrent callers.

### H5. Threads and scale (E4)

- **Evaluations off the consumer thread.** Evaluations leave the Paho, Milo
  and value-feed threads: a shared pool runs each mapping's evaluations one at
  a time, and the consumers only accept. Today one slow sink blocks Paho's
  callback thread (QoS acks, inbound queue of 10), and the value feed runs
  stalled tasks itself (CallerRuns). This also removes the per-mapping lock.
- **Polling** runs on one shared scheduler at a fixed rate, not a Timer
  thread per source.
- **MQTT.** One client per broker and credentials (from Deferred), carrying
  each mapping's subscriptions.
- **PLC4X.** One connection per device (a cached connection manager), not
  one per tag.
- **Test:** the capacity profile at N = 1000 with a bound on the thread
  count.

### H6. The compiler emits route plans directly

`RoutesConfiguration`, the configuration entity classes, `triggerData` and the
ServiceLoader route-creator registry give way to the compiler's typed plan.
A mapping is then compiled once per install; today it is compiled twice,
once for the fingerprint and again when staged. This is the largest change
and has no behaviour change.

### Documents

- SPEC corrections that still apply after the fixes: the default QoS, OPC UA
  security, and large integers and bytes in Lua (opaque values). Also:
  Properties of other valueTypes are blocked today, not passed as lexical text.
- A LICENSE file (MIT, with the DataBridge's notice); README and SPEC §13
  already point to it.

### Decisions needed

1. **Coalescing (H5).** Should triggers that arrive while a mapping evaluates
   collapse into one evaluation (the union of `meta.sources.*.trigger`)?
   Recommended: yes. Today they already evaluate the same snapshot once per
   arrival.
2. **MQTT QoS without `mqv_qos`.** The WoT binding default (0), or today's 2?
   Recommended: 0, with `aas-model` authoring `mqv_qos` where it matters. QoS
   0 blocks invoke-only actions, so those need it authored.
3. **OPC UA security.** Support AID security definitions (Sign,
   SignAndEncrypt, certificates), or document `None`? Recommended: document
   now and defer; the lab servers run without security.
4. **When to do H6.** Before or after the paper's measurements? Recommended:
   after, unless the double compile shows up in E4's time-to-ready.

## Planned work

### 1. Physical stations register `aas-model` exports

Every simulated station registers the Resource `aas-model` authors for it
(2026-10-21): `packml_runtime` publishes the export, with the AID base set to
the broker or OPC UA endpoint of the stack, and writes it to
`AASDescriptions/Resource/exports/`. Halt is served by the shared runtime and
only invokes. The physical stations (IEC 61499 PLCs, the Planar controller)
still register the old way; they are to publish a copy of their resource's
export (`aas-model/scripts/export_resource.py`).

### 2. TimeSeries sources and sinks (InfluxDB 2)

Agreed design (2026-10-21); InfluxDB only, Internal/External segments out of
scope.

**Model (`aas-model`).** `Variables` holds the groups `State`, `Measurements`
and `Log`. A group holds TimeSeries SMCs, or plain elements for data that is
not a time series (lists such as a process queue, files, images). A TimeSeries
SMC carries this project's semanticId with the IDTA TimeSeries id as a
supplemental id; everything inside it (`Metadata/Record`, `Segments`,
`LinkedSegment`, `Endpoint`, `Query`) keeps the IDTA 02008 ids. A TimeSeries
groups what is sampled together (one row = one sample), not one variable each.
The LinkedSegment carries a DMP extension `InfluxDb2Target`: `Bucket`,
`Measurement` and `Tags`. The tags name the AAS that owns the series (an AIMC
writing another resource's series targets that resource's segment). Each
Record field's InfluxDB field is its idShortPath (decided 2026-10-22; a
rename starts a new field), and a ReferenceElement in the Record counts as if
the referenced element were declared inline (SPEC §5.5). `Query` is
generated from the same values for the web UI.

**Measurements.** One measurement per Record schema (`station_state`,
`dispensing_weight`, `operation_occupy`), assets told apart by tag. Tags have
low cardinality (never an invocation id or UUID). aas-model generating the
Records from templates keeps one measurement's fields consistent, and the
reconciler holds back mappings that declare one field with different types
(`timeseries-type-conflict`, SPEC §5.5).

**DMP.** A Source or Sink targets a LinkedSegment; the DMP reads its
TimeSeries container (Record, `InfluxDb2Target`), which counts as referenced for
events and fingerprints. Lua sees the row as a table keyed by Record idShort
(groups nested, `Position.X`); a missing field is not written, an unknown one
or a wrong type fails the sink. `Time` is optional: a Lua value is the point's
time, otherwise the evaluation time. Only UtcTime records can be written. A
Sink writes one point per evaluation; a Source is a Read with
`PollingInterval` returning the latest row (`last()` over a lookback range).
The Record never holds a current value. The segment's StartTime/EndTime/State
bound reads and gate writes (implemented 2026-10-22, SPEC §5.5).

**Logging.** Only what is authored: `Log` series written by ordinary sinks or
ResponseSinks. aas-model generates one typed series per Operation (Time,
Outcome, DurationMs and the Operation's variables). The response
transformation also runs on failure, with the error in `meta.invocation`; Lua
gets a global `meta` table (sources' times, sinks' kinds, the invocation). SPEC §10,
`InfluxAuditLog`, `LineProtocol` and `DMP_AUDIT_INFLUX_*` are removed
(2026-10-22); DMP-internal diagnostics go to the application log and
`/management/status`.

**Camel.** `dmp-influxdb2:` extends `camel-influxdb2` 4.22 (producer only,
asynchronous `makeWriteApi()`, auto-creates buckets and orgs): writes through
`WriteApiBlocking` so failures fail the exchange, builds typed points from the
Lua row, reads the latest row with Flux, never creates buckets or orgs. One
`InfluxDBClient` for the configured InfluxDB (`DMP_INFLUX_*`), its token only
from the environment. One request per row; batching only with per-batch
acknowledgement.

**Web UI.** The TimeSeries plugin reads a TimeSeries submodel or SMC (children
from `value`, the group's idShortPath as prefix), resolves references and
names fields by idShortPath.

**Done (2026-10-22):** `aas-model` (Variables groups, TimeSeries SMC,
`InfluxDb2Target`, segment periods, Record references, the `operation_log`
builder on a reference to its Operation); the
DMP (discovery of the series container, segment Sources and Sinks,
`dmp-influxdb2:`, segment periods, Record references and lists, SPEC §5.5);
Lua's global `meta` and the
failure response (SPEC §7 step 8, §8); the old interaction log removed; the
parent workspace's `influxdb` Compose service (org and bucket `ap2030`); the
web UI's TimeSeries plugin (TimeSeries SMCs, nested groups, and an
nginx query proxy that keeps the token server-side, `INFLUXDB_PROXY_*`).

**Stations (2026-10-22):** every operational station records
`State/Station` (`station_state`) through a history mapping of its own beside
the one keeping the AAS Properties current (each mapping has its own
consumers, SPEC §6), and `Log/Occupy`, `Log/Release`
(`operation_<skill>`, also failed calls) through the skills' response
mappings; dispensing records `Measurements/Weight`, the Planar shuttles
`Measurements/Location`, the same way. Verified on the e2e stack. Halt answers nothing and has no log; station-specific
skills (Dispensing, MoveToPosition, …) are not logged yet. Every stack running
the stations needs the InfluxDB, since the skill mappings write their log
(main and e2e stacks have one).

**Open, in order:**
1. Consumers: BT_Controller (it reads `Variables` today, partly at stale
   paths, and must poll InfluxDB for current values), `stationctl`, the e2e
   tests and kg-bridge; then the flat `Variables.variable` map and the state
   mappings that keep it current are removed.
2. Logs for the station-specific skills (`command_reply(..., logged=...)`).

### 3. Parent workspace

- The main and e2e stacks (`compose/`) run the `dmp` service; DataBridge
  residue and the `labServer` setup are gone (2026-10-18).
- The Python `OperationDelegation` service is removed (2026-10-21); every
  operation runs through the DMP. ARSO is a side project and out of scope.

## Deferred (keep in mind)

- **CAN** (AID 1.2 Annex E.2). Not IP-based: receiving it needs a CAN
  controller on the DMP's host wired to the bus, the bus brought up there, and
  the SocketCAN device handed to the container. That ties the central DMP to
  one machine on one bus; AID 1.2 itself leaves `base` unused for CAN and
  expects gateway or adapter hardware at the bus (§2.24.8). Integrate a CAN
  device through a gateway that bridges it to IP (e.g. MQTT), or later through
  an edge DMP instance on the bus (needs multi-instance, below). Worked out for
  that case (2026-10-18): `canv_offset`/`canv_length` are bits (ranges 0–63 and
  1–64); a field is an unsigned integer in Intel (little-endian) bit order, the
  value its raw integer plus `canv_valueOffset` (the binding shifts values so
  that payloads carry no negative numbers), `canv_enumeratedValues` maps raw
  integers to strings, `canv_payloadMapping` fields form one object; bits the
  AID does not describe are sent as 0. PLC4X 0.13.1 could carry the frames
  (generic CAN driver over SocketCAN, a virtual CAN transport for tests) but
  decodes only whole-byte types, so the bit fields are the DMP's to decode.
- **TimescaleDB** as a second TimeSeries dialect (InfluxDB only for now).
  Advantages: the latest value is an indexed `ORDER BY time DESC LIMIT 1`
  with no lookback window (InfluxDB `last()` needs a `range()`, so a state
  unchanged for longer than the window is not found, and `range(start: 0)`
  slows as history grows); a Record maps one to one onto a hypertable with
  typed columns. Costs: the tables need DDL from the Record, and the web UI
  plugin speaks only Flux (needs a REST layer such as PostgREST).
- **HTTP `websub`.** Rejected: it needs an inbound callback endpoint the
  device posts to. `longpoll`, `sse` and static `htv_headers` are done.
- **BACnet.** Rejected by the compiler: the PLC4X 0.13.1 `bacnet-ip` driver
  has `canRead`/`canWrite` false and only listens for COV notifications.
  Options: ReadProperty/WriteProperty/SubscribeCOV on PLC4X's generated BACnet
  model over UDP, a BACnet stack (BACnet4J: check its GPL licence), or drop
  it. AID addresses a device by instance number (`bacnet://{deviceId}`), so
  Who-Is/I-Am discovery is needed, with an optional address term on
  EndpointMetadata where broadcast does not reach.
- **AAS asynchronous invocation** (`invoke-async` with a handle; BaSyx does
  not implement it yet) for actions longer than a caller's HTTP timeout.
- **Runtime:** recovery from a lost MQTT event session without a restart;
  multiple DMP instances / HA (today `DMP_MANAGED_AIMC_IDS` partitions
  manually); per-mapping Lua memory isolation; Lua blob provenance
  (signing); a credential redaction audit of data-plane log lines and caller
  error details beyond H4. The shared MQTT client per broker moved to H5.
- **Verification:** live staging on the lab (Occupy/Halt and granular
  Operations, cross-AAS references, mixed sync+async fan-in); Kafka
  `SASL_SSL` with `PLAIN`; one final performance and capacity evaluation.
- **C5 flaky test:** `ManagedPackagedRuntimeIntegrationTest` once exceeded the
  managed-shutdown deadline after a Kafka outage; the deadline was raised from
  15 s to 45 s (2026-10-18). Reopen only if it recurs.

## Decided, not to be done

| Item | Decision (2026-10-18) |
| --- | --- |
| Acknowledgements written to AAS Properties (old A2) | No. A further Property is written through fan-out: an extra Sink on an ordinary mapping, a ResponseSink after the response transform on a bridge mapping. |
| Operation variables of other element types (A3) | No. Properties, collections and lists (nested) only. |
| Custom-encoded OPC UA datatypes (A4) | No. Structures the server describes are supported (SPEC §5.2, 2026-10-18); pre-1.04 DataTypeDictionaries only if a station needs them. |
| Per-element array fan-out via `valuePath` (A5) | No; Lua loops. |
| PLC4X/Modbus actions (A6) | No; the higher-level control sequences writes and reads (SPEC §7.1). |
| Kafka/ActiveMQ/SQL bindings, Hono (A8) | Dropped 2026-10-17; a protocol needed later returns as an AID binding with tests. |
| Invocation URL validation at registration (D5) | No. `aas-model` authors the final `invocationDelegation` URLs; registration stores the AAS unchanged and does not rewrite or validate them. |
| Deployed-stack cutover (D1) | Not needed; the Python manager is gone. |
| ARSO delegation defaults (D3) | Out of scope (side project). |
| AID checked against Modbus, MQTT or HTTP devices | No (2026-10-18). These devices do not describe themselves; only OPC UA values are checked against the server's DataTypes (SPEC §5.2). Unsupported schema terms are rejected instead. |
| `linux/arm/v7` image | Dropped (2026-10-18); the image is built for `linux/amd64` and `linux/arm64`. Inherited from the DataBridge build; `eclipse-temurin:21-jre` has no arm/v7 variant, the DMP runs on x86-64 servers, and current Raspberry Pis run arm64. |
| Java 25 (E4) | Stay on Java 21 for now (2026-10-18). The full suite, integration tests included, passes compiled for and run on Temurin 25.0.4; the base image exists for amd64 and arm64. On 25, Netty 4.1 (Milo) and the Kafka clients log `sun.misc.Unsafe` deprecation warnings; later JDKs plan to deny that access. Camel 4.22 supports 25; PLC4X 0.13.1 is tested to 24; Milo and LuaJ make no statement. |
| LuaJ replacement (E1) | Stay on LuaJ and work around its defects in `LuaSandbox` (as `6167ad5` did for the shared string metatable). Cobalt promises no API stability; luajava runs native code that can crash the JVM. Revisit only if a defect cannot be worked around. |

## Accepted limitations

These are deliberate and documented in SPEC; listed so they are not mistaken
for open work.

- No periodic repair; a missed event is only corrected by the next relevant
  event or a restart.
- Event-sourced AAS Property sources have no value until their first change.
- Device actions are never retried or replayed; extra response-sink writes are
  best effort.
- Property write-delegation (synchronous PUT with same-Property writeback) is
  retired.
