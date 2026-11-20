import json
from typing import Any, Dict, List

import pytest

from datahub.metadata.schema_classes import ChangeTypeClass, EdgeClass
from datahub.specific.api import ApiPatchBuilder
from datahub.utilities.urns.error import InvalidUrnError

API_URN = "urn:li:api:orders.OrderService/GetOrder"
REQUEST_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,orders.GetOrderRequest,PROD)"
RESPONSE_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,orders.Order,PROD)"


def _patch_ops(builder: ApiPatchBuilder) -> List[Dict[str, Any]]:
    mcps = builder.build()
    assert len(mcps) == 1
    mcp = mcps[0]
    assert mcp.entityUrn == API_URN
    assert mcp.aspectName == "apiSignature"
    assert mcp.changeType == ChangeTypeClass.PATCH
    assert mcp.aspect is not None
    payload = json.loads(mcp.aspect.value)
    # The generic patch payload is either the bare op list or wrapped as {"patch": [...]}.
    return payload["patch"] if isinstance(payload, dict) else payload


def test_add_and_remove_dataset_edges_patch_only_the_edges() -> None:
    builder = (
        ApiPatchBuilder(API_URN)
        .add_input_dataset(REQUEST_URN)
        .add_output_dataset(EdgeClass(destinationUrn=RESPONSE_URN))
        .remove_input_dataset(RESPONSE_URN)
    )

    ops = _patch_ops(builder)

    assert [(op["op"], op["path"]) for op in ops] == [
        ("add", f"/inputDatasetEdges/{REQUEST_URN}"),
        ("add", f"/outputDatasetEdges/{RESPONSE_URN}"),
        ("remove", f"/inputDatasetEdges/{RESPONSE_URN}"),
    ]
    assert ops[0]["value"]["destinationUrn"] == REQUEST_URN
    assert ops[1]["value"]["destinationUrn"] == RESPONSE_URN


def test_dataset_reference_must_be_a_dataset_urn() -> None:
    with pytest.raises(InvalidUrnError):
        ApiPatchBuilder(API_URN).add_input_dataset("urn:li:api:not-a-dataset")
