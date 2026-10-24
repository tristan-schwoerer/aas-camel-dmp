# AAS-Camel-DMP

A data-mapping processor (DMP) for Asset Administration Shells, built on
[Apache Camel](https://camel.apache.org/) 4 and Java 21. It is a fork of the
[Eclipse BaSyx DataBridge](https://github.com/eclipse-basyx/basyx-databridge).

The DMP runs as **one Java process**. It has no configuration files: every
route is built at runtime from the AAS repository and kept current from
submodel events.

## How it works

1. **Discovery at startup, on a need-to-know basis.** The DMP lists the AIMC
   (Asset Interfaces Mapping Configuration 2.0) submodels of the AAS v3
   repository (BaSyx v2) by semanticId (`GET /submodels?semanticId=…`), which
   returns them whole. Every Source and Sink is a ModelReference whose first key
   is a submodel and whose remaining keys are the idShort path to the element.
   The DMP groups these references per submodel and reads each submodel once:
   - an **AID** (Asset Interfaces Description) whole, because a target needs
     the interface's endpoint metadata, forms and schemas, not just the
     referenced property. An AID is recognised by its fixed idShort
     (`…/AssetInterfacesDescription`) or by the semanticId in `$metadata`;
   - **any other submodel** as `$metadata` plus the referenced element subtrees
     (`GET …/submodel-elements/<idShortPath>`), or whole when it is referenced
     at more than eight separate paths.

   Shells are never read. This is the only full discovery.
2. **Compilation.** Each mapping becomes one isolated bundle of Camel routes.
   Lua transformations arrive as Blobs in the AIMC and are compiled in memory;
   Lua is never read from files. A referenced element contributes its
   structure: the Property `valueType` (or a list's `valueTypeListElement`)
   types the values Lua receives and returns; Property values are never part of
   the configuration.
3. **Lifecycle from events.** Repository submodel events arrive over Kafka or
   MQTT (`DMP_MANAGED_EVENT_TRANSPORT`). The DMP keeps an index of what its
   mappings reference and drops every event that cannot change a mapping:
   events on other submodels, elements nothing references, and value-only
   updates of referenced Properties. A relevant event re-reads only the changed
   submodel and recompiles only the AIMCs that depend on it; a new submodel is
   read once to see whether it is an AIMC. Unchanged mappings (same
   fingerprint) keep their routes. There is no periodic repair; a pass that
   fails transiently is retried with backoff.

### What a mapping can do

- **Property and event routing.** Read or observe a source (MQTT, OPC UA,
  HTTP polling and observation, Modbus via PLC4X, SNMP, AAS elements,
  TimeSeries segments), transform it with Lua,
  and write to sinks (AAS elements, MQTT, OPC UA, HTTP, PLC4X). An AAS Property
  source without a `PollingInterval` follows the repository's change events
  instead of being polled: each event carries the new value. These values come
  from a separate value feed, not the configuration consumer: it reads only
  new events (Kafka: own consumer group at the latest offset, nothing
  committed; MQTT: clean session), so values published while the DMP was down
  are never replayed to devices. It runs only while such sources exist.
- **Synchronous actions.** BaSyx delegates an AAS Operation to the URL in its
  `invocationDelegation` qualifier; the DMP serves `POST` on that URL's path
  (by convention `/operations/{aasIdShort}/{skill}`). It invokes the device
  action over HTTP, an OPC UA Method, or MQTT 5 request/reply (or a one-way
  MQTT publish for an action without output), and returns the typed result:
  the outputs the authored response mapping makes of the device's reply,
  never fields matched by name. Operation input, output and InOut variables may be Properties,
  SubmodelElementCollections (objects) or SubmodelElementLists (arrays),
  nested freely; Lua sees them as typed Lua values.

### Lua

The `lua:` Camel component (`dmp.camel-lua`) runs AIMC transformation blobs
with LuaJ. A blob defines `function aimc_main(sources)` and returns a table
keyed by SinkId.

- Sandbox: base functions, `math`, `table`, `string`, `json` and `os.clock`
  only. No `io`, `require`/`load*`, `debug`, `luajava`, `package` or
  `coroutine`.
- A CPU budget per invocation, enforced by an instruction-count hook
  (default 2 s).
- Each blob is compiled once per mapping. Each invocation gets a fresh,
  isolated environment.

### AAS API

AAS sources and sinks speak only the AAS Part 2 HTTP/REST API v3:
`<repository>/submodels/<base64url(id)>`, `GET …/submodel-elements/<path>`
to read and `PATCH …/$value` to write. BaSyx v1 is not supported.

## Running

```bash
docker build -t dmp:java .   # builds the jar from these sources inside the image
```

The image is built locally only (the parent workspace's Compose stacks build
it as `dmp:java`); a local `mvn` build is needed only for tests.

The container listens on port 8080 for operation calls, `/health` and
`/management/status`.

| Setting | Meaning / default |
| --- | --- |
| `DMP_MANAGED_AAS_REPOSITORY_URI` | **Required.** Credential-free HTTP(S) repository root |
| `DMP_MANAGED_AAS_AUTHORIZATION` | Optional `Authorization` header for repository reads (secret) |
| `DMP_CREDENTIALS_FILE` | Optional JSON file (a mounted secret) with the credentials the AIDs' security schemes need, by endpoint or scheme reference (SPEC §5.2); without it only `nosec` interfaces compile |
| `DMP_MANAGED_EVENT_TRANSPORT` | `kafka` (default) or `mqtt`: where repository events come from |
| `DMP_MANAGED_KAFKA_BROKERS` | **Required for Kafka.** Kafka bootstrap servers for control events |
| `DMP_MANAGED_CONTROL_TOPIC` | **Required for Kafka.** 1–8 comma-separated literal topic names, e.g. `submodel-events` |
| `DMP_MANAGED_CONTROL_GROUP_ID` | **Required for Kafka.** Consumer group of the control listener |
| `DMP_MANAGED_KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` (default), `SASL_PLAINTEXT` or `SASL_SSL`; control plane only |
| `DMP_MANAGED_KAFKA_SASL_MECHANISM` | With SASL: `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512` |
| `DMP_MANAGED_KAFKA_SASL_USERNAME`, `…_PASSWORD` | With SASL: nonblank credentials; never logged or exposed |
| `DMP_MANAGED_MQTT_BROKER_URI` | **Required for MQTT.** `tcp://`, `ssl://`, `ws://` or `wss://` broker of the BaSyx MQTT feature |
| `DMP_MANAGED_MQTT_CLIENT_ID` | `dmp-control` (≤ 54 characters); sessions are resumed as `<id>-submodels` and `<id>-elements` |
| `DMP_MANAGED_MQTT_USERNAME`, `…_PASSWORD` | Optional, together; never logged or exposed |
| `DMP_MANAGED_MQTT_REPOSITORY_ID` | `+`: the `sm-repository/<id>/…` topic level to follow |
| `DMP_MANAGED_MQTT_SESSION_EXPIRY_SECONDS` | 86400 |
| `DMP_MANAGED_AIMC_IDS` | Optional JSON array of exact AIMC IDs to adopt; read by ID instead of listing |
| `DMP_MANAGED_MAX_MAPPINGS`, `DMP_MANAGED_MAX_INVALIDATIONS` | 4096 each |
| `DMP_MANAGED_MAX_PARALLEL_READS` | 4 (1–64) concurrent repository reads |
| `DMP_MANAGED_RETRY_INTERVAL_MS` | 5000: first retry after a failed pass, doubling up to 16× |
| `DMP_MANAGED_CONNECT_TIMEOUT_MS`, `DMP_MANAGED_REQUEST_TIMEOUT_MS` | 5000 / 15000 |
| `DMP_MANAGED_PAGE_SIZE`, `DMP_MANAGED_MAX_PAGES` | 100 / 100 |
| `DMP_AFFORDANCE_VALIDATION` | `OFF` (default), `ON` or `REPORT` |
| `DMP_HTTP_PORT` / `-Ddmp.http.port` | HTTP listener port (health, status, Operations), default 8080 |
| `DMP_INFLUX_URL` | Optional InfluxDB 2 base URL for TimeSeries segments (SPEC §5.5); without it, segment mappings are blocked |
| `DMP_INFLUX_ORG`, `DMP_INFLUX_TOKEN` | Required with `DMP_INFLUX_URL`; the token is never logged or exposed |
| `DMP_INFLUX_LOOKBACK` | How far back a segment Source looks for the latest row (ISO-8601, default `P30D`) |

### Health and status

- `/health` returns 200 once the initial repository scan has completed and the
  event transport is connected (Kafka: the control consumer is assigned on every
  topic), otherwise 503.
- `/management/status` reports convergence and, per mapping, its state
  (`installed`, `blocked`, `stale`, `failed`), generation (the bundle fence, from
  a sequence shared by all mappings), revision (this mapping's installations
  since start: 1, then +1 per redeploy), desired/installed fingerprints and a
  failure category. Repository IDs appear only as SHA-256
  tokens. Blocked mappings can coexist with a ready process; `converged`
  distinguishes the two.

### Logging

The DMP logs each mapping's state changes (warnings for blocked and failed
mappings). There is no built-in interaction log: what is recorded is authored
as a `Log` TimeSeries written by the mappings themselves (SPEC §5.5, §10).

### Operational limits

- There is no lease or cluster coordination. Run exactly one DMP per set of
  mappings.
- Replacing a mapping drains in-flight requests; calls during the swap receive
  "unavailable". Device actions are never retried or replayed.
- Repository reads are not a multi-submodel transaction. The event consumer
  starts before the first scan, so an edit made during a read arrives as an
  event and converges.
- Kafka resumes from committed offsets. The DMP uses Kafka 4.3 clients, which
  work with brokers from Kafka 2.1 on (the compose stack runs cp-kafka 7.9 =
  Kafka 3.9). MQTT uses Camel's `paho-mqtt5` with two
  fixed QoS 1 subscriptions on persistent sessions
  (`sm-repository/<repo>/submodels/+` and `…/submodels/+/submodelElements/#`,
  clients `<clientId>-submodels` and `<clientId>-elements`), so events published
  while the DMP was disconnected arrive on reconnect. If the broker drops the
  session (expiry, broker reset), events from that gap are lost until the next
  restart.
- The event transport is only the repository's channel. MQTT sources and sinks
  from AID forms use their own MQTT 5 clients (`paho-mqtt5`), one per endpoint,
  also when Kafka carries the events. Their credentials are bound in the Camel
  registry per bundle; endpoint URIs only reference them by name.
- An AAS Property source that follows events has no value until its first
  change event; startup does not replay current values.

## Modules

| Module | Purpose |
| --- | --- |
| `dmp.core` | Route model, execution policies, fan-in/out, bundle lifecycle (`DmpComponent`) |
| `dmp.management` | Repository client, AIMC/AID projection, mapping compiler, reconciliation, Kafka and MQTT event consumers |
| `dmp.component` | Executable, runtime settings, health/status endpoints, Docker image |
| `dmp.camel-lua` | `lua:` component and sandbox |
| `dmp.camel-aas` | AAS element source/sink, `aas-event:` event source and the operation bridge |
| `dmp.camel-paho`, `-opcua`, `-http`, `-plc4x`, `-snmp` | Connectors reachable from AID bindings |
| `dmp.camel-influxdb2` | TimeSeries LinkedSegments on InfluxDB 2 |

See [SPEC.md](SPEC.md) for the design specification and decision records.

## Provenance

- Fork origin: [eclipse-basyx/basyx-databridge](https://github.com/eclipse-basyx/basyx-databridge) (MIT).
- Derived files keep their original MIT headers; the project remains MIT (see `LICENSE`).
- AIMC/AID semantics: IDTA submodel templates (`admin-shell-io/submodel-templates`, CC BY 4.0).
- This is a derivative work, not an official Eclipse/BaSyx component.
  Its Java packages and Maven groupId are `dk.aau.smartproductionlab` (renamed
  from the DataBridge's `org.eclipse.digitaltwin.basyx`, 2026-10-22).
