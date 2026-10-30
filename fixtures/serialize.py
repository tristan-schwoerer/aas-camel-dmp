"""Authored aas-model submodels as AAS JSON, the way registration publishes them."""

from __future__ import annotations

import json

from basyx.aas import model
from basyx.aas.adapter.json import json_serialization

from aas_pydantic.convert_pydantic_model import convert_model_to_submodel


def authored(pydantic_submodel, identifier: str) -> model.Submodel:
    """A pydantic submodel under ``identifier`` as a BaSyx submodel. An AID's interfaces without security select
    their ``nosec_sc`` (AID 1.1 §2.25) once the id is known."""
    pydantic_submodel.id = identifier
    if hasattr(pydantic_submodel, "select_nosec"):
        pydantic_submodel.select_nosec()
    return convert_model_to_submodel(model_submodel=pydantic_submodel)


def store_json(*objects) -> dict:
    """The AAS environment JSON of ``objects``."""
    return json.loads(json_serialization.object_store_to_json(model.DictIdentifiableStore(objects)))
