"""Every public way to build, emit, write, read or accept a MetadataChangeEvent warns.

Exceptions, listed in breaking-changes: the MCE constants, the type hints and the snapshot classes.
"""

import contextlib
import copy
import json
import logging
import pathlib
import pickle
import threading
import warnings
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    TypeVar,
    Union,
)
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

import datahub.emitter.mce_builder as builder
from datahub.api.entities.datajob import DataFlow
from datahub.cli.check_cli import check
from datahub.configuration.common import ConfigurationWarning
from datahub.emitter.composite_emitter import CompositeEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.emitter.rest_emitter import DataHubRestEmitter, EmitMode
from datahub.emitter.synchronized_file_emitter import SynchronizedFileEmitter
from datahub.errors import DataHubDeprecationWarning
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback, Sink
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.api.workunit import (
    _MCE_WORKUNIT_DEPRECATION_MESSAGE,
    MetadataWorkUnit,
    _warn_mce_workunit_deprecated,
)
from datahub.ingestion.extractor.mce_extractor import WorkUnitRecordExtractorConfig
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.sink.datahub_kafka import DatahubKafkaSink
from datahub.ingestion.sink.datahub_lite import DataHubLiteSink
from datahub.ingestion.sink.datahub_pg_queue import DatahubPgQueueSink
from datahub.ingestion.sink.datahub_rest import DatahubRestSink, RestSinkMode
from datahub.ingestion.sink.file import FileSink, FileSinkConfig, write_metadata_file
from datahub.ingestion.source.file import (
    FileSourceConfig,
    GenericFileSource,
    read_metadata_file,
)
from datahub.ingestion.source.sql.sql_common import SqlWorkUnit
from datahub.ingestion.transformer.auto_helper_transformer import (
    AutoHelperTransformer,
)
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    LegacyMCETransformer,
)
from datahub.ingestion.transformer.generic_aspect_transformer import (
    GenericAspectTransformer,
)
from datahub.ingestion.transformer.mark_dataset_status import MarkDatasetStatus
from datahub.ingestion.transformer.set_attribution import (
    SetAttributionConfig,
    SetAttributionTransformer,
)
from datahub.lite.lite_local import DataHubLiteLocal
from datahub.lite.lite_util import DataHubLiteWrapper, get_datahub_lite
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    GenericAspectClass,
    MetadataChangeEventClass,
    StatusClass,
)
from datahub.pgqueue.config import PgQueueConnectionConfig, PgQueueConsumerConfig
from datahub.pgqueue.consumer import DatahubPgQueueConsumer, poll_mce_messages
from datahub.testing import mce_helpers
from datahub.utilities.deprecation import log_deprecation_once

URN = builder.make_dataset_urn("hive", "db.table")
T = TypeVar("T")
Call = Callable[[pathlib.Path], object]
Record = Union[MetadataChangeEventClass, MetadataChangeProposalWrapper]


def _quiet_mce(aspects: List[Any]) -> MetadataChangeEventClass:
    # Built without its own warning so each test sees only the warning of the API it calls.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DataHubDeprecationWarning)
        return MetadataChangeEventClass(
            proposedSnapshot=DatasetSnapshotClass(urn=URN, aspects=aspects)
        )


def _mce() -> MetadataChangeEventClass:
    return _quiet_mce([StatusClass(removed=False), DatasetPropertiesClass(name="t")])


def _mcp() -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(entityUrn=URN, aspect=StatusClass(False))


def _objs(records: Iterable[Any]) -> List[dict]:
    return json.loads(json.dumps([r.to_obj() for r in records]))


def _rest_sink() -> DatahubRestSink:
    sink = DatahubRestSink.__new__(DatahubRestSink)
    sink.config = MagicMock(mode=RestSinkMode.SYNC)
    sink.report = MagicMock()
    sink._emitter_thread_local = threading.local()
    sink._emitter_thread_local.emitter = MagicMock()
    sink._gms_emit_mode = EmitMode.SYNC_PRIMARY
    return sink


def _emit_all(record: Record) -> None:
    graph = DataHubGraph.__new__(DataHubGraph)

    @contextlib.contextmanager
    def make_rest_sink(run_id: str) -> Iterator[DatahubRestSink]:
        yield _rest_sink()

    with patch.object(DataHubGraph, "make_rest_sink", side_effect=make_rest_sink):
        graph.emit_all([record])


def _kafka_sink() -> DatahubKafkaSink:
    with patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True):
        return DatahubKafkaSink.create(
            {"connection": {"bootstrap": "localhost:9092"}},
            PipelineContext(run_id="test"),
        )


def _pg_queue_sink() -> DatahubPgQueueSink:
    with patch("datahub.ingestion.sink.datahub_pg_queue.DatahubPgQueueEmitter"):
        return DatahubPgQueueSink.create(
            {
                "queue": {
                    "host_port": "localhost:5432",
                    "database": "datahub",
                    "username": "datahub",
                    "password": "datahub",
                }
            },
            MagicMock(),
        )


def _lite(tmp_path: pathlib.Path) -> DataHubLiteLocal:
    return get_datahub_lite(
        {"type": "sqlite", "config": {"file": str(tmp_path / "lite.db")}}
    )


def _lite_sink(tmp_path: pathlib.Path) -> DataHubLiteSink:
    return DataHubLiteSink.create(
        {"type": "sqlite", "config": {"file": str(tmp_path / "lite.db")}},
        PipelineContext(run_id="test"),
    )


def _write_sink(sink: Sink, record: Record) -> None:
    sink.write_record_async(RecordEnvelope(record, metadata={}), NoopWriteCallback())


def _mce_file(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / "mces.json"
    path.write_text(json.dumps([_mce().to_obj(), _mce().to_obj()]))
    return path


def _mcp_file(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / "mcps.json"
    path.write_text(json.dumps([_mcp().to_obj()]))
    return path


def _pg_consumer(payload_kind: str) -> DatahubPgQueueConsumer:
    consumer = DatahubPgQueueConsumer.__new__(DatahubPgQueueConsumer)
    consumer.config = MagicMock(payload_kind_by_route_key={"route": payload_kind})
    consumer._avro_deserializer = MagicMock(
        return_value={
            "proposedSnapshot": (
                "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot",
                {"urn": URN, "aspects": []},
            )
        }
        if payload_kind == "mce"
        else {"entityUrn": URN, "entityType": "dataset", "changeType": "UPSERT"}
    )
    return consumer


class _CustomAspectTransformer(GenericAspectTransformer):
    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "_CustomAspectTransformer":
        return cls()

    def entity_types(self) -> List[str]:
        return ["dataset"]

    def aspect_name(self) -> str:
        return "customAspect"

    def transform_generic_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[GenericAspectClass]
    ) -> Optional[GenericAspectClass]:
        return aspect


def _transformers() -> List[Transformer]:
    ctx = PipelineContext(run_id="test")
    return [
        MarkDatasetStatus.create({"removed": False}, ctx),
        _CustomAspectTransformer(),
        SetAttributionTransformer(
            SetAttributionConfig(
                attribution_source="urn:li:platformResource:ingestion"
            ),
            ctx,
        ),
    ]


def _transform(transformer: Transformer, record: Record) -> None:
    list(transformer.transform([RecordEnvelope(record, metadata={})]))


TRANSFORMERS = ["MarkDatasetStatus", "generic", "set_attribution"]


def _iterate_generic_file(path: pathlib.Path) -> None:
    source = GenericFileSource(
        ctx=PipelineContext(run_id="t"), config=FileSourceConfig(path=str(path))
    )
    for file_info in source.get_filenames():
        list(source.iterate_generic_file(file_info))


class _McpToMceTransformer(Transformer):
    """A user's transformer that builds MCEs itself."""

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "_McpToMceTransformer":
        return cls()

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        for envelope in record_envelopes:
            if isinstance(envelope.record, MetadataChangeProposalWrapper):
                mce = MetadataChangeEventClass(
                    proposedSnapshot=DatasetSnapshotClass(
                        urn=URN, aspects=[StatusClass(False)]
                    )
                )
                envelope = RecordEnvelope(mce, envelope.metadata)
            yield envelope


class _McpSource(Source):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "_McpSource":
        return cls(ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for i in range(3):
            yield MetadataChangeProposalWrapper(
                entityUrn=builder.make_dataset_urn("hive", f"db.t{i}"),
                aspect=StatusClass(removed=False),
            ).as_workunit()

    def get_report(self) -> SourceReport:
        return SourceReport()


class _MceSource(Source):
    """A user's custom source that still yields MCE work units."""

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "_MceSource":
        return cls(ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for i in range(3):
            yield MetadataWorkUnit(f"wu{i}", mce=_mce())

    def get_report(self) -> SourceReport:
        return SourceReport()


def _pg_consumer_polling_mce() -> DatahubPgQueueConsumer:
    consumer = _pg_consumer("mce")
    consumer.config.topic_routes = {"MetadataChangeEvent": "topic"}
    consumer._conn = MagicMock()
    consumer._lock_owner = "test"
    consumer._priority_bands_json = ""
    consumer._repo = MagicMock()
    consumer._repo.fetch_topic_row.return_value = (1, 1, None)
    consumer._repo.receive_batch_for_group.return_value = [
        MagicMock(payload=b"x", payload_compression=0)
    ]
    consumer.config.payload_kind_by_route_key = {"MetadataChangeEvent": "mce"}
    return consumer


def _subclass_legacy_mce_transformer(tmp_path: pathlib.Path) -> None:
    class MyTransformer(BaseTransformer, LegacyMCETransformer):
        pass


def _subclass_legacy_mce_transformer_after_generic(tmp_path: pathlib.Path) -> None:
    class MyTransformer(Generic[T], BaseTransformer, LegacyMCETransformer):
        pass


def _check_unpack_mces(tmp_path: pathlib.Path) -> str:
    return (
        CliRunner()
        .invoke(check, ["metadata-file", "--unpack-mces", str(_mcp_file(tmp_path))])
        .output
    )


def _pg_consumer_config(payload_kind: str) -> PgQueueConsumerConfig:
    queue = PgQueueConnectionConfig(
        host_port="localhost:5432", database="db", username="u", password="p"
    )
    return PgQueueConsumerConfig(
        queue=queue, payload_kind_by_route_key={"route": payload_kind}
    )


# The urn helpers that also count MCE records; each takes (path) and reads the file.
URN_HELPERS: List[Any] = [
    ("get_entity_urns", lambda f: mce_helpers.get_entity_urns(f)),
    (
        "assert_for_each_entity",
        lambda f: mce_helpers.assert_for_each_entity(
            "dataset", "status", {}, f, exception_urns=[URN]
        ),
    ),
    (
        "assert_entity_urn_like",
        lambda f: mce_helpers.assert_entity_urn_like("dataset", ".*", f),
    ),
    (
        "assert_entity_urn_not_like",
        lambda f: mce_helpers.assert_entity_urn_not_like("dataset", "^$", f),
    ),
]


MCE_CALLS: List[Any] = [
    pytest.param(
        lambda p: MetadataChangeEventClass(
            proposedSnapshot=DatasetSnapshotClass(urn=URN, aspects=[])
        ),
        id="MetadataChangeEventClass",
    ),
    pytest.param(lambda p: MetadataWorkUnit("id", mce=_mce()), id="workunit-mce"),
    pytest.param(lambda p: MetadataWorkUnit.from_metadata(_mce()), id="from_metadata"),
    pytest.param(lambda p: list(auto_workunit([_mce()])), id="auto_workunit"),
    pytest.param(lambda p: builder.make_lineage_mce([URN], URN), id="make_lineage_mce"),
    pytest.param(
        lambda p: builder.can_add_aspect_to_snapshot(DatasetSnapshotClass, StatusClass),
        id="can_add_aspect_to_snapshot",
    ),
    pytest.param(
        lambda p: builder.can_add_aspect(_mce(), StatusClass), id="can_add_aspect"
    ),
    pytest.param(
        lambda p: builder.assert_can_add_aspect(_mce(), StatusClass),
        id="assert_can_add_aspect",
    ),
    pytest.param(
        lambda p: builder.get_aspect_if_available(_mce(), StatusClass),
        id="get_aspect_if_available",
    ),
    pytest.param(
        lambda p: builder.remove_aspect_if_available(_mce(), StatusClass),
        id="remove_aspect_if_available",
    ),
    pytest.param(
        lambda p: builder.get_or_add_aspect(_mce(), StatusClass(removed=True)),
        id="get_or_add_aspect",
    ),
    pytest.param(
        lambda p: builder.set_aspect(_mce(), None, StatusClass), id="set_aspect"
    ),
    pytest.param(
        lambda p: DataFlow(
            orchestrator="airflow", id="flow", env="PROD"
        ).generate_mce(),
        id="DataFlow.generate_mce",
    ),
    pytest.param(lambda p: _emit_all(_mce()), id="DataHubGraph.emit_all"),
    pytest.param(lambda p: _rest_sink().emit_async(_mce()), id="rest-sink-emit_async"),
    pytest.param(lambda p: _write_sink(_rest_sink(), _mce()), id="rest-sink-write"),
    pytest.param(lambda p: _write_sink(_kafka_sink(), _mce()), id="kafka-sink-write"),
    pytest.param(
        lambda p: _write_sink(_pg_queue_sink(), _mce()), id="pg-queue-sink-write"
    ),
    pytest.param(
        lambda p: _write_sink(
            FileSink(
                config=FileSinkConfig(filename=str(p / "out.json")), ctx=MagicMock()
            ),
            _mce(),
        ),
        id="file-sink-write",
    ),
    pytest.param(lambda p: _write_sink(_lite_sink(p), _mce()), id="lite-sink-write"),
    pytest.param(
        lambda p: SynchronizedFileEmitter(str(p / "sync.json")).emit(_mce()),
        id="SynchronizedFileEmitter.emit",
    ),
    pytest.param(
        lambda p: CompositeEmitter(
            [SynchronizedFileEmitter(str(p / "composite.json"))]
        ).emit(_mce()),
        id="CompositeEmitter-file",
    ),
    pytest.param(
        lambda p: write_metadata_file(p / "out.json", [_mce(), _mce()]),
        id="write_metadata_file",
    ),
    pytest.param(
        lambda p: write_metadata_file(
            p / "out.json", [_mce().to_obj(), _mce().to_obj()]
        ),
        id="write_metadata_file-dict",
    ),
    pytest.param(
        lambda p: list(read_metadata_file(_mce_file(p))), id="read_metadata_file"
    ),
    pytest.param(
        lambda p: write_metadata_file(p / "out.json", read_metadata_file(_mce_file(p))),
        id="read_metadata_file-iterated-by-datahub",
    ),
    pytest.param(
        lambda p: list(
            GenericFileSource(
                ctx=PipelineContext(run_id="t"), config=FileSourceConfig(path=str(p))
            ).iterate_mce_file(str(_mce_file(p)))
        ),
        id="iterate_mce_file",
    ),
    *[
        pytest.param(
            lambda p, i=i: _transform(_transformers()[i], _mce()), id=f"transform-{n}"
        )
        for i, n in enumerate(TRANSFORMERS)
    ],
    pytest.param(
        lambda p: _iterate_generic_file(_mce_file(p)), id="iterate_generic_file"
    ),
    pytest.param(
        lambda p: mce_helpers.check_goldens_stream(
            outputs=[_mce(), _mce()], golden_path=_mce_file(p)
        ),
        id="check_goldens_stream",
    ),
    pytest.param(
        lambda p: mce_helpers.check_goldens_stream(
            outputs=[_mce().to_obj(), _mce().to_obj()], golden_path=_mce_file(p)
        ),
        id="check_goldens_stream-dict",
    ),
    pytest.param(
        lambda p: AutoHelperTransformer(lambda s: s), id="AutoHelperTransformer"
    ),
    pytest.param(_subclass_legacy_mce_transformer, id="LegacyMCETransformer"),
    pytest.param(
        _subclass_legacy_mce_transformer_after_generic,
        id="LegacyMCETransformer-after-Generic",
    ),
    pytest.param(lambda p: SqlWorkUnit(id="id", mcp=_mcp()), id="SqlWorkUnit"),
    pytest.param(lambda p: _lite(p).write(_mce()), id="DataHubLiteLocal.write"),
    pytest.param(
        lambda p: DataHubLiteWrapper(_lite(p), MagicMock()).write(_mce()),
        id="DataHubLiteWrapper.write",
    ),
    # The wrapper forwards to a sink from datahub code: only the wrapper may warn.
    *[
        pytest.param(
            lambda p, make=make: DataHubLiteWrapper(_lite(p), make(p)).write(_mce()),
            id=f"DataHubLiteWrapper.write-forward-{name}",
        )
        for name, make in [
            ("rest", lambda p: _rest_sink()),
            ("kafka", lambda p: _kafka_sink()),
            ("pg-queue", lambda p: _pg_queue_sink()),
            (
                "file",
                lambda p: FileSink(
                    config=FileSinkConfig(filename=str(p / "f.json")), ctx=MagicMock()
                ),
            ),
        ]
    ],
    pytest.param(
        lambda p: mce_helpers.assert_mce_entity_urn(
            "ALL", "dataset", ".*", str(_mce_file(p))
        ),
        id="assert_mce_entity_urn",
    ),
    pytest.param(
        lambda p: mce_helpers.assert_entity_mce_aspect(
            URN, StatusClass(removed=False), StatusClass, str(_mce_file(p))
        ),
        id="assert_entity_mce_aspect",
    ),
    pytest.param(lambda p: poll_mce_messages(MagicMock()), id="poll_mce_messages"),
    pytest.param(
        lambda p: poll_mce_messages(_pg_consumer_polling_mce()),
        id="poll_mce_messages-reading-mce",
    ),
    pytest.param(
        lambda p: _pg_consumer("mce")._deserialize("route", "topic", b"\x00"),
        id="pgqueue-mce-payload",
    ),
    pytest.param(lambda p: _pg_consumer_config("mce"), id="pgqueue-mce-payload-kind"),
    pytest.param(
        lambda p: MetadataWorkUnit.generate_workunit_id(_mce()),
        id="generate_workunit_id",
    ),
    *[
        pytest.param(lambda p, call=call: call(str(_mce_file(p))), id=name)
        for name, call in URN_HELPERS
    ],
]


@pytest.fixture(autouse=True)
def _fresh_once_caches() -> Iterator[None]:
    caches = [
        log_deprecation_once,
        _warn_mce_workunit_deprecated,
    ]
    for cache in caches:
        cache.cache_clear()
    yield
    for cache in caches:
        cache.cache_clear()


def _deprecations(call: Call, tmp_path: pathlib.Path) -> List[warnings.WarningMessage]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        call(tmp_path)
    return [w for w in caught if issubclass(w.category, DataHubDeprecationWarning)]


@pytest.mark.parametrize("call", MCE_CALLS)
def test_mce_api_warns_once_at_the_caller(
    call: Call, tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING):
        first = _deprecations(call, tmp_path)
        second = _deprecations(call, tmp_path)

    assert len(first) == 1, [str(w.message) for w in first]
    messages = {str(w.message) for w in first + second}
    assert len(messages) == 1, messages
    assert {w.filename for w in first} == {__file__}
    assert [r.getMessage() for r in caplog.records].count(messages.pop()) == 1


@pytest.mark.parametrize(
    "rebuild",
    [
        pytest.param(
            lambda m: MetadataChangeEventClass.from_obj(m.to_obj()), id="from_obj"
        ),
        pytest.param(copy.deepcopy, id="deepcopy"),
        pytest.param(lambda m: pickle.loads(pickle.dumps(m)), id="pickle"),
    ],
)
def test_reading_an_mce_does_not_warn(
    rebuild: Callable[[MetadataChangeEventClass], MetadataChangeEventClass],
) -> None:
    mce = _mce()
    # Any DeprecationWarning, so a runtime class marker (which warns in __new__) also fails.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        rebuild(mce)
    assert [w for w in caught if issubclass(w.category, DeprecationWarning)] == []


MCP_CALLS: List[Any] = [
    pytest.param(lambda p: MetadataWorkUnit("id", mcp=_mcp()), id="workunit-mcp"),
    pytest.param(lambda p: list(auto_workunit([_mcp()])), id="auto_workunit"),
    pytest.param(lambda p: _emit_all(_mcp()), id="DataHubGraph.emit_all"),
    pytest.param(lambda p: _rest_sink().emit_async(_mcp()), id="rest-sink-emit_async"),
    pytest.param(lambda p: _write_sink(_kafka_sink(), _mcp()), id="kafka-sink-write"),
    pytest.param(
        lambda p: _write_sink(_pg_queue_sink(), _mcp()), id="pg-queue-sink-write"
    ),
    pytest.param(
        lambda p: _write_sink(
            FileSink(
                config=FileSinkConfig(filename=str(p / "out.json")), ctx=MagicMock()
            ),
            _mcp(),
        ),
        id="file-sink-write",
    ),
    pytest.param(lambda p: _write_sink(_lite_sink(p), _mcp()), id="lite-sink-write"),
    pytest.param(
        lambda p: SynchronizedFileEmitter(str(p / "sync.json")).emit(_mcp()),
        id="SynchronizedFileEmitter.emit",
    ),
    pytest.param(
        lambda p: write_metadata_file(p / "out.json", [_mcp()]),
        id="write_metadata_file",
    ),
    pytest.param(
        lambda p: list(read_metadata_file(_mcp_file(p))), id="read_metadata_file"
    ),
    pytest.param(
        lambda p: _iterate_generic_file(_mcp_file(p)), id="iterate_generic_file"
    ),
    pytest.param(lambda p: _write_sink(_rest_sink(), _mcp()), id="rest-sink-write"),
    pytest.param(
        lambda p: DataHubLiteWrapper(_lite(p), MagicMock()).write(_mcp()),
        id="DataHubLiteWrapper.write",
    ),
    pytest.param(
        lambda p: mce_helpers.check_goldens_stream(
            outputs=iter([_mcp()]), golden_path=_mcp_file(p)
        ),
        id="check_goldens_stream-iterator",
    ),
    pytest.param(
        lambda p: write_metadata_file(p / "out.json", [_mcp().to_obj()]),
        id="write_metadata_file-dict",
    ),
    *[
        pytest.param(
            lambda p, i=i: _transform(_transformers()[i], _mcp()), id=f"transform-{n}"
        )
        for i, n in enumerate(TRANSFORMERS)
    ],
    pytest.param(lambda p: _lite(p).write(_mcp()), id="DataHubLiteLocal.write"),
    pytest.param(
        lambda p: _pg_consumer("mcp")._deserialize("route", "topic", b"\x00"),
        id="pgqueue-mcp-payload",
    ),
    pytest.param(lambda p: _pg_consumer_config("mcp"), id="pgqueue-mcp-payload-kind"),
    # The default routes include "mce"; datahub's own default must not warn.
    pytest.param(
        lambda p: PgQueueConsumerConfig(queue=_pg_consumer_config("mcp").queue),
        id="pgqueue-default-payload-kinds",
    ),
    pytest.param(
        lambda p: MetadataWorkUnit.generate_workunit_id(_mcp()),
        id="generate_workunit_id",
    ),
    *[
        pytest.param(lambda p, call=call: call(str(_mcp_file(p))), id=name)
        for name, call in URN_HELPERS
    ],
]


@pytest.mark.parametrize("call", MCP_CALLS)
def test_mcp_input_does_not_warn(call: Call, tmp_path: pathlib.Path) -> None:
    assert _deprecations(call, tmp_path) == []


def test_composite_emitter_warning_points_at_the_caller() -> None:
    rest = DataHubRestEmitter.__new__(DataHubRestEmitter)
    rest._default_emit_mode = EmitMode.SYNC_PRIMARY
    rest.emit_mcps = MagicMock()  # type: ignore[method-assign]

    caught = _deprecations(
        lambda p: CompositeEmitter([rest]).emit(_mce()), pathlib.Path()
    )

    assert [w.filename for w in caught] == [__file__]
    rest.emit_mcps.assert_called_once()


class _McpToMceListTransformer(_McpToMceTransformer):
    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        return list(super().transform(record_envelopes))


class _BufferingTransformer(Transformer):
    """Holds every record and passes them all on at the end of the stream."""

    def __init__(self) -> None:
        self.held: List[RecordEnvelope] = []

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "_BufferingTransformer":
        return cls()

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        for envelope in record_envelopes:
            if isinstance(envelope.record, EndOfStream):
                yield from self.held
                yield envelope
            else:
                self.held.append(envelope)


@pytest.mark.parametrize(
    "transformer", ["_McpToMceTransformer", "_McpToMceListTransformer"]
)
def test_user_transformer_that_outputs_mces_warns_once(
    transformer: str, tmp_path: pathlib.Path
) -> None:
    def run() -> None:
        Pipeline.create(
            {
                "run_id": "mce-transformer",
                "source": {"type": f"{__name__}._McpSource"},
                "transformers": [{"type": f"{__name__}.{transformer}"}],
                "sink": {
                    "type": "file",
                    "config": {"filename": str(tmp_path / "o.json")},
                },
            },
            report_to=None,
        ).run()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        run()

    assert [
        w.filename for w in caught if issubclass(w.category, DataHubDeprecationWarning)
    ] == [__file__]


def test_unpack_mces_into_mcps_recipe_key_warns() -> None:
    with pytest.warns(ConfigurationWarning, match="unpack_mces_into_mcps"):
        WorkUnitRecordExtractorConfig.model_validate({"unpack_mces_into_mcps": True})


def test_unpack_mces_cli_flag_warns(tmp_path: pathlib.Path) -> None:
    output = _check_unpack_mces(tmp_path)
    assert "unpack_mces" in output and "deprecated" in output.lower()
    output = (
        CliRunner().invoke(check, ["metadata-file", str(_mcp_file(tmp_path))]).output
    )
    assert "deprecated" not in output


def test_converting_paths_write_the_mcps_of_the_mce(tmp_path: pathlib.Path) -> None:
    expected = _objs(mcps_from_mce(_mce()))

    with pytest.warns(DataHubDeprecationWarning):
        assert _objs(read_metadata_file(_mce_file(tmp_path))) == expected * 2

    sync_file = tmp_path / "sync.json"
    with pytest.warns(DataHubDeprecationWarning):
        SynchronizedFileEmitter(str(sync_file)).emit(_mce())
    written = [
        m.to_obj(simplified_structure=True) for m in [*mcps_from_mce(_mce()), _mcp()]
    ]
    assert json.loads(sync_file.read_text()) == json.loads(json.dumps(written[:-1]))

    # MCEs already in the file are rewritten as MCPs on the next emit, without a warning.
    sync_file.write_text(json.dumps([_mce().to_obj()]))
    assert (
        _deprecations(
            lambda p: SynchronizedFileEmitter(str(sync_file)).emit(_mcp()), tmp_path
        )
        == []
    )
    assert json.loads(sync_file.read_text()) == json.loads(json.dumps(written))


def test_converting_paths_keep_an_mce_without_aspects(tmp_path: pathlib.Path) -> None:
    empty = _quiet_mce([])
    path = tmp_path / "empty.json"
    path.write_text(json.dumps([empty.to_obj()]))
    with pytest.warns(DataHubDeprecationWarning):
        assert _objs(read_metadata_file(path)) == _objs([empty])

    sync_file = tmp_path / "sync.json"
    with pytest.warns(DataHubDeprecationWarning):
        SynchronizedFileEmitter(str(sync_file)).emit(empty)
    assert json.loads(sync_file.read_text()) == _objs([empty])


def test_recipe_mce_warns_once_and_golden_guard_still_fails(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    output = tmp_path / "output.json"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        Pipeline.create(
            {
                "run_id": "mce-guard",
                "source": {"type": f"{__name__}._MceSource"},
                # Built-in and custom transformers that only pass the MCEs on.
                "transformers": [
                    {"type": "simple_add_dataset_tags", "config": {"tag_urns": []}},
                    # Lazy pass-through first: it outputs each MCE right after reading it.
                    {"type": f"{__name__}._CustomAspectTransformer"},
                    {"type": f"{__name__}._ListReturningTransformer"},
                    {"type": f"{__name__}._BufferingTransformer"},
                ],
                "sink": {"type": "file", "config": {"filename": str(output)}},
            },
            report_to=None,
        ).run()
        with pytest.raises(pytest.fail.Exception, match="MCE"):
            mce_helpers.check_golden_file(
                pytestconfig, output, output, fail_on_output_mces=True
            )

    # One message per user problem, however many MCEs: the work unit warns once; the
    # transformer, the sink and the golden comparison stay silent.
    assert [
        str(w.message)
        for w in caught
        if issubclass(w.category, DataHubDeprecationWarning)
    ] == [_MCE_WORKUNIT_DEPRECATION_MESSAGE]


def test_file_source_recipe_on_mce_file_adds_no_deprecation_warning(
    tmp_path: pathlib.Path,
) -> None:
    # The file source reports MCE files in its run report; nothing else may warn.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        Pipeline.create(
            {
                "source": {
                    "type": "file",
                    "config": {"path": str(_mce_file(tmp_path))},
                },
                "transformers": [
                    {"type": "simple_add_dataset_tags", "config": {"tag_urns": []}}
                ],
                "sink": {
                    "type": "file",
                    "config": {"filename": str(tmp_path / "o.json")},
                },
            },
            report_to=None,
        ).run()
        CliRunner().invoke(
            check, ["metadata-file", "--rewrite", str(_mce_file(tmp_path))]
        )

    assert [
        w
        for w in caught
        if issubclass(w.category, (DataHubDeprecationWarning, ConfigurationWarning))
    ] == []


class _ListReturningTransformer(Transformer):
    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "_ListReturningTransformer":
        return cls()

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        return list(record_envelopes)


def test_iterate_generic_file_warning_is_not_swallowed(tmp_path: pathlib.Path) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", DataHubDeprecationWarning)
        with pytest.raises(DataHubDeprecationWarning):
            _iterate_generic_file(_mce_file(tmp_path))
