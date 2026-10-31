import warnings
from typing import List, Sequence, Union
from unittest import mock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    StatusClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub_airflow_plugin.hooks.datahub import (
    DatahubCompositeHook,
    DatahubKafkaHook,
    DatahubRestHook,
    SynchronizedFileHook,
)

URN = "urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)"
MCE = MetadataChangeEventClass(
    proposedSnapshot=DatasetSnapshotClass(
        urn=URN,
        aspects=[
            StatusClass(removed=False),
            DatasetPropertiesClass(description="d"),
            UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset="urn:li:dataset:(urn:li:dataPlatform:hive,db.up,PROD)",
                        type="TRANSFORMED",
                    )
                ]
            ),
        ],
    )
)
MCPW = MetadataChangeProposalWrapper(entityUrn=URN, aspect=StatusClass(removed=True))

Hook = Union[
    DatahubRestHook, DatahubKafkaHook, SynchronizedFileHook, DatahubCompositeHook
]

HOOKS: List[Hook] = [
    DatahubRestHook(),
    DatahubKafkaHook(),
    SynchronizedFileHook(),
    DatahubCompositeHook(["datahub_rest_default"]),
]


def _emitted(
    hook: Hook,
    items: Sequence[Union[MetadataChangeEventClass, MetadataChangeProposalWrapper]],
) -> List[MetadataChangeProposalWrapper]:
    with mock.patch.object(type(hook), "make_emitter") as make_emitter:
        hook.emit(items)
    return [c.args[0] for c in make_emitter.return_value.emit.call_args_list]


@pytest.mark.parametrize("hook", HOOKS, ids=lambda h: type(h).__name__)
def test_hook_sends_mce_as_one_mcp_per_aspect(hook: Hook) -> None:
    with pytest.warns(DeprecationWarning, match="MetadataChangeEvent"):
        emitted = _emitted(hook, [MCE, MCPW])

    assert all(isinstance(e, MetadataChangeProposalWrapper) for e in emitted)
    assert [(e.entityUrn, e.aspect) for e in emitted] == [
        *((URN, a) for a in MCE.proposedSnapshot.aspects),
        (URN, MCPW.aspect),
    ]


@pytest.mark.parametrize("hook", HOOKS, ids=lambda h: type(h).__name__)
def test_hook_does_not_warn_on_mcps(hook: Hook) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        assert _emitted(hook, [MCPW]) == [MCPW]


def test_hook_accepts_a_generator() -> None:
    with pytest.warns(DeprecationWarning):
        # emit() is typed Sequence, but a generator must not lose items either.
        emitted = _emitted(
            DatahubRestHook(),
            (item for item in [MCPW, MCE, MCPW]),  # type: ignore[arg-type]
        )

    assert len(emitted) == 2 + len(MCE.proposedSnapshot.aspects)
