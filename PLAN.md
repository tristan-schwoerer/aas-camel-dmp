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

Order: H0, then H1 and H2 (independent), H3, H4, H5, H6. All of it lands before
the paper's measurements, so they evaluate the final version (decided
2026-10-24).

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

### H2. Lua budget — done (2026-10-23)

- The budget is a counting `DebugLib` set as `Globals.debuglib`; its timeout is an
  `Error` that `pcall`/`xpcall` cannot catch. A compute loop runs 2.6× faster
  than with LuaJ's `DebugLib` (475 → 185 ms; 172 ms without any hook).
- `string.find`/`match`/`gmatch`/`gsub` use `LuaPatterns`, a port of Lua 5.2's
  matcher that counts against the budget, also through the shared string
  metatable (`s:find(p)`). Compared with LuaJ's on thousands of cases it
  answers alike, with LuaJ's character classes (read from LuaJ itself); where
  LuaJ throws or never ends (141 of the compared cases: empty patterns, `%f`,
  `gmatch` after an empty match) it answers as Lua 5.2.
- 16 MiB caps on `string.rep`, `table.concat`, `gsub` and `json.decode`; a
  depth and cycle guard (200) in `LuaJsonCodec`; `print` to the log at DEBUG;
  no Lua compiler in the sandbox.
- Identical Lua shares one prototype across mappings (chunk `transformation`).
- Tests: the review's reproducers (`LuaBudgetTest`) and the LuaJ comparison
  (`LuaPatternsTest`). Still open (Deferred): a memory cap, so `..` in a loop
  can still grow a string up to the heap.

### H3. Reconciliation and events — done (2026-10-24)

- **Structure without values.** `Aas.withoutValues` strips values at every
  depth (an MLP keeps its languages). The fingerprint of an AAS element target
  uses it, and covers the Operation of a variable Source (delegation URL,
  variables). `relevant()` compares the referenced elements' structure for an
  SME_UPDATED of any type and for an SM_UPDATED carrying the submodel; only
  TimeSeries containers (dependency kind `series`) keep their values relevant.
- **Retry what failed.** A refresh compiles again every AIMC with a transiently
  failed dependency; a new submodel whose first read failed is kept as a failed
  AIMC and read again. Mapping-level `unauthorized` is retried, too.
- **Events during a pass** are held and judged again against the index the
  pass produced (also during the initial scan).
- **Event types.** Unknown and unreadable events are ignored (logged); an
  unreadable MQTT submodel event still asks for a full rescan.
- **Ingestion.** The Kafka identity is read by streaming; the event is parsed
  once and the tree shared with the value-feed listeners (`AasEventHub.publish(
  JsonObject)`). The control consumer polls batches (500, 100 ms) and commits
  each partition once per batch.
- **`/health`** is ready once the initial scan completed and the transport is
  connected; later failures degrade `/management/status` only.
- **Swap.** REST listeners of a new bundle start first (gated, "unavailable");
  a failed replacement, also a fenced one, restores the previous bundle.
- **Cost per pass.** Reverse dependencies are maintained per bundle and rebuilt
  once per pass. The conflict checks stay as they are: one linear pass each.
- **Tests:** a transient AID failure retried by an empty refresh, a new AIMC
  whose first read failed, collection values vs structure (element and
  submodel events), an event during the initial scan (fails without the
  holding), the Operation and collection fingerprints, batch commits, health,
  and the restored bundle. Not tested directly: the REST 404 window during a
  swap. Not done: reconcile time at N = 1000 (left to the E4 pilot).

### H4. Connector boundaries (done 2026-10-24)

- **AAS client (`dmp.camel-aas`), done.** One client per context
  (`AasHttp`, bound in the registry) for reads, writes and value-feed
  enrichment:
  - one URL builder percent-encodes the idShortPath (`Items%5B0%5D`; BaSyx
    answers a raw bracket with a 400);
  - every request carries the repository authorization discovery uses;
  - reads use `sendAsync` with the caller's timeout, no thread per read;
  - no INFO line per write.

  Checked against BaSyx 2.0 (2026-10-24): `$value` accepts only text leaves
  (a JSON number is a 500, also inside a collection), a collection only whole,
  and a list's `$value` write keeps the old length and reorders the items.
  So collections are written ValueOnly with every leaf as text, a list Sink is
  rejected at compile time (its items are Sinks), and a structured value for a
  Property is its JSON text. A list's length is no longer structure
  (`Aas.withoutValues` keeps one item per distinct shape), so a sink that
  appends to a list does not reinstall the mappings that read it.
- **Credentials, done.** No credential goes into a URI any more; each is bound
  in the Camel registry while its bundle lives and the URI only names it:
  - OPC UA: camel-milo's `username`/`password` options as `BoundCredential`
    references, with a salted `clientId` (`BoundCredential.connectionKey()`),
    so endpoints of one server and user still share a session. The options
    are in name order now (a `parallelStream` over a `HashMap` gave a
    different URI per compile).
  - HTTP Basic: an `Authorization` header in the bound client options (next
    item), for polls, observations, sinks and actions; sinks and actions
    accepted no credentials before.
  - SNMP: the community as a registry reference.

  Tested with a password holding `@`, `&`, `#` and `:` against a Milo server
  (the session is the user's), a device for HTTP and an SNMP agent. Found on
  the way: camel-milo always adds an anonymous identity after the user, so a
  user that cannot authenticate (a token policy that needs an encrypted
  password from a server without a certificate) falls back to an anonymous
  session without a word. The security step (4) must not keep that fallback.
- **HTTP, done.** One option set (`HttpClientOptions`, bound per endpoint)
  for polls, sinks, observation and actions: automatic retries off, connect
  and response timeouts of 30 s (an action's own timeout replaces them), the
  form's static headers and Basic credentials. Observation uses HTTP/1.1, a
  long poll backs off after a connect timeout (it counted as an idle poll),
  and an event stream silent for 5 min is closed and reopened.
- **MQTT, done.**
  - The reply subscription is confirmed (SUBSCRIBE and its SUBACK) once per
    connection and again after a reconnect, not once per call. A call held the
    transport lock for that broker round trip, so calls on one endpoint queued
    behind it; now a call only publishes under the lock.
  - Every declared data schema means a JSON payload (the WoT default content
    type), so a number or boolean reaches Lua typed instead of as bytes; a
    `string` schema takes a JSON string or the bare text a device publishes.
  - Without `mqv_qos` the QoS is 0, the WoT MQTT binding's default (decided
    2026-10-24; camel-paho's is 2), on sources, sinks and actions. An
    invoke-only action (no `mqv_responseTopic`) without `mqv_qos` 1 or 2 is
    blocked: the broker would not acknowledge it. aas-model authors `mqv_qos`
    on every form, so nothing of the line changes.
- **OPC UA, done.**
  - A failed subscription setup deletes the subscription it created and backs
    off (0.5 s doubling to 30 s). Before, a node that did not resolve left a
    subscription on the shared session every 500 ms (7 in 3 s in the test).
  - Browse paths are resolved once per session for Read and Call, as they
    were for subscriptions and writes.
  - The node is read from the endpoint URI without the regex that cut at `)`,
    so NodeIds and browse paths may hold parentheses (`2:Pump(1)`; the
    compiler rejected those browse paths for the regex's sake).
  - The device-schema check takes the first Good sample (a Bad first sample
    switched the check off for the subscription's life).
  - Every session has a 30 s request timeout (camel-milo's `requestTimeout`;
    Milo's default is 60 s), so Read, Write and Call end, and a Read waits at
    most 35 s for its answer.
  - Int64 and UInt64 writes are exact: a double beyond the range saturated, a
    BigInteger wrapped, and UInt64 refused values above `Long.MAX_VALUE`.
- **SNMP, done.** An octet string is text when it is valid UTF-8 without
  control characters (snmp4j's test admits ASCII only, so `café` was hex), and
  text is written as UTF-8 rather than the platform charset.
- **Security (AID `security`/`securityDefinitions`), decided 2026-10-24.**
  The AID says what an interface requires, in IDTA terms only; the DMP holds
  the credentials. Secrets never live in the AAS: an AAS is meant to be
  shared (AASX exports, registries, the web UI, `aas-model` in git), and the
  WoT TD that AID's security model comes from describes the mechanism and
  leaves the credentials to the consumer. AID 1.1 (§2.25–2.27) has no element
  for a secret; AID 1.2 is the same as far as checked.
  1. **What the DMP reads.** `security` on `EndpointMetadata` (all
     interactions) or on a form (replacing it for that form), each a list of
     references into the interface's `securityDefinitions`, all of which must
     be satisfied. Per scheme: `scheme`; `name`, `in` (basic, apikey,
     bearer, digest); `qop`; `format`; `token`, `scopes`, `flow` (oauth2);
     `oneOf`/`allOf` (combo: `oneOf` takes the first scheme the DMP can
     apply); `uav_securityMode`, `uav_securityPolicy`,
     `uav_userIdentityToken`, `uav_issueToken` (OPC UA). An empty `security`
     blocks the mapping: AID requires at least one reference, `nosec_sc` for
     an interface without security.
  2. **Where credentials come from.** The DMP's deployment, never the AAS:
     a mounted secrets file (a Docker secret). Credentials are keyed by what
     they authenticate against, so equipment on shared infrastructure needs
     no new DMP configuration:
     - the DMP's own identity: its OPC UA application certificate and key,
       its TLS client certificate, the CAs it trusts (one per DMP);
     - per token server (the `oauth2_sc` `token` URI): the OAuth2 client id
       and secret;
     - per host or broker (`scheme://host:port`): a user name and password,
       or an API key or bearer token;
     - per scheme reference (AID submodel id, interface, scheme idShort): an
       override for a secret only one device has.

     A required scheme without a matching credential blocks the mapping with
     its path; nothing falls back (no anonymous OPC UA session). Credentials
     in an href's user info are rejected: they are secrets in the AAS (no
     deployed AID has any). Secrets are bound in the Camel registry and never
     reach a log, URI, status or the exported fingerprint.
  3. **Now (H4), done:** the
     reader (1); the credential source (2) with its lookup and the file
     format; `nosec` for every protocol; HTTP `basic`, `bearer`, `apikey`
     (header, query, cookie); MQTT `basic` (CONNECT user name and password).
     Everything else blocks explicitly, by scheme and path. (`AidSecurity`,
     `DmpCredentials`; the file is read once at start, so a changed secret
     needs a restart.)
  4. **Later, when a device needs it:** HTTP `digest` and OAuth2 client
     credentials; TLS (`https`, `mqtts`) with the DMP's CAs and client
     certificate; OPC UA through sessions the DMP opens itself (a
     `MiloClientConnectionManager` of its own: camel-milo always adds an
     anonymous fallback and selects endpoints by policy only) with
     `opcua_channel_sc`, `auto` and `opcua_authentication_sc` (Anonymous,
     UserName, Certificate; IssuedToken via `uav_issueToken`). `psk` stays
     unsupported (the JDK has no TLS-PSK).
  5. **Deployment and tests use `nosec`, as AID demands, done.** `aas-model`
     authors a `nosec_sc` in every interface's `securityDefinitions` and
     references it from `EndpointMetadata/security` (`select_nosec`, run by
     the id injector and the fixture generator); the DMP fixtures are
     regenerated. The deployed AIDs still have an empty `security`: they must
     be republished before a DMP with this change runs, or every mapping
     blocks.
- **Tests, done:** list-index sink and read (`Items%5B0%5D`); the repository
  authorization on reads and writes; a password with `@&#:` against Milo, an
  HTTP device and an SNMP agent; an HTTP write to a hung device (sent once,
  ends at its timeout); scalar MQTT sources; a node that does not resolve
  leaves no subscriptions behind; request/reply with 32 concurrent callers
  (one publish each, one confirmed subscription); the AID security reader
  (nosec, basic, bearer, apikey, combo, empty and unknown selections, OPC UA
  blocked) and the credentials file.

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

### Decided (2026-10-24)

1. **Coalescing (H5): not now.** Triggers that arrive while a mapping
   evaluates keep evaluating one by one; revisit only if it becomes a
   problem. (Operation calls are never merged in any case: each call is an
   evaluation of its own.)
2. **MQTT QoS without `mqv_qos`: 0**, the WoT binding default (H4).
3. **Security: the AID's security definitions as IDTA defines them, no
   extension; secrets never in the AAS.** The DMP holds the credentials, keyed
   by endpoint or scheme reference; deployment and tests use `nosec_sc` (H4).
4. **H6 before the measurements**, so they evaluate the final version.

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
