# Fixtures

The AASs the DMP is tested and evaluated with, authored with aas-model (pinned by commit in `pyproject.toml`)
and written where their readers expect them. The JSON is generated, never edited by hand: both readers check
it against the SHA-256 in its `manifest.json`.

| Module | Writes | Read by |
| --- | --- | --- |
| `contracts.py` | `../dmp.component/src/test/resources/runtime-model-contracts/` | `RawModelContractFixtureTest`, `WireFixtureCompatibilityTest` |
| `scenarios.py` | `../evaluation/test_aas/` | the evaluation's runner and adapters |

`serialize.py` turns the authored submodels into AAS JSON the way registration publishes them.

```bash
export MQTT_SCHEMAS_DIR=<aas-model checkout>/MQTTSchemas   # at the commit the pinned aas-model names
uv run python -m contracts
uv run python -m scenarios
```

Installed from git, aas-model has no vendored `MQTTSchemas` and fetches the published schemas instead, which
may differ from the ones it pins (the stations' action outputs did); `MQTT_SCHEMAS_DIR` keeps the output
reproducible.

`contracts.py` has synthetic cases, one protocol or mapping form each, and every resource of aas-model's
catalog as registration builds it; review every changed compile outcome in the DMP after regenerating.
`scenarios.py` records the aas-model commit in its manifest. To bump aas-model, change the `rev` in
`pyproject.toml`, run `uv lock` and regenerate both.
