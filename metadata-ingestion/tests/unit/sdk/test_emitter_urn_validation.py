"""Emit-time rejection of entityUrns that no reader can parse back.

GMS accepts URNs with unencoded reserved characters by default
(`STRICT_URN_VALIDATION_ENABLED` defaults to false), persists them, and every
subsequent typed-URN read of that entity then fails forever. See
https://github.com/datahub-project/datahub/issues/19086.
"""

from unittest.mock import patch

import pytest

from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper, validate_emitted_urn
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import DatasetSnapshotClass, StatusClass
from datahub.utilities.urns.error import InvalidUrnError

# Shape taken from a real incident: parentheses are URN structural characters
# and cannot appear unencoded inside an id.
UNPARSEABLE_URN = "urn:li:dashboard:(looker,my_dashboard (copy))"

# The same id, correctly encoded. This is what DashboardUrn("looker", ...) emits.
ENCODED_URN = "urn:li:dashboard:(looker,my_dashboard %28copy%29)"

UNPARSEABLE_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.my_table (copy),PROD)"


def _mcp(urn: str) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=urn, aspect=StatusClass(removed=False)
    )


def _rest_emitter() -> DatahubRestEmitter:
    return DatahubRestEmitter("http://fake-gms:8080")


@pytest.fixture
def kafka_emitter():
    with patch("datahub.emitter.kafka_emitter.SerializingProducer"):
        yield DatahubKafkaEmitter(
            KafkaEmitterConfig.model_validate(
                {"connection": {"bootstrap": "fake-broker:9092"}}
            )
        )


class TestRestEmitterRejectsUnparseableUrns:
    def test_emit_mcp_rejects(self):
        with pytest.raises(InvalidUrnError, match=r"my_dashboard"):
            _rest_emitter().emit_mcp(_mcp(UNPARSEABLE_URN))

    def test_emit_mcps_rejects(self):
        """The batch path is what every ingestion pipeline uses."""
        with pytest.raises(InvalidUrnError, match=r"my_dashboard"):
            _rest_emitter().emit_mcps([_mcp(ENCODED_URN), _mcp(UNPARSEABLE_URN)])

    def test_emit_mce_rejects(self):
        mce = MetadataChangeEvent(
            proposedSnapshot=DatasetSnapshotClass(
                urn=UNPARSEABLE_DATASET_URN, aspects=[StatusClass(removed=False)]
            )
        )
        with pytest.raises(InvalidUrnError, match=r"my_table"):
            _rest_emitter().emit_mce(mce)


class TestKafkaEmitterRejectsUnparseableUrns:
    """Kafka matters most: GMS deliberately keeps best-effort skip on the
    consumer path (#11187), so a bad URN there is dropped with only a log line
    and the client never sees an error. The client-side guard is the only one.
    """

    def test_emit_mcp_async_rejects(self, kafka_emitter):
        with pytest.raises(InvalidUrnError, match=r"my_dashboard"):
            kafka_emitter.emit_mcp_async(_mcp(UNPARSEABLE_URN), lambda e, m: None)

    def test_emit_mce_async_rejects(self, kafka_emitter):
        mce = MetadataChangeEvent(
            proposedSnapshot=DatasetSnapshotClass(
                urn=UNPARSEABLE_DATASET_URN, aspects=[StatusClass(removed=False)]
            )
        )
        with pytest.raises(InvalidUrnError, match=r"my_table"):
            kafka_emitter.emit_mce_async(mce, lambda e, m: None)


class TestGuardAcceptsWhatReadersAccept:
    """The guard uses Urn.from_string, so it accepts exactly what a reader can
    parse -- no stricter. These pin that invariant so the guard cannot drift
    into rejecting URNs that work today.
    """

    def test_encoded_reserved_characters_are_accepted(self):
        validate_emitted_urn(ENCODED_URN)

    def test_unknown_entity_type_is_accepted(self):
        """Custom entity types fall back to the generic Urn, which by design
        does not enforce reserved characters. A reader is equally permissive, so
        the guard must not reject these either.
        """
        validate_emitted_urn("urn:li:myCustomEntity:some (thing)")

    def test_construction_from_a_bad_urn_still_works(self):
        """Regression guard for the read path.

        `datahub_database_reader._parse_row` builds one MCPW per source row, so
        validating in `__post_init__` would make the SDK unable to even read
        existing bad data -- breaking the migration and cleanup tooling that has
        to handle it. Construction must stay permissive; only emitting is
        rejected.
        """
        assert _mcp(UNPARSEABLE_URN).entityUrn == UNPARSEABLE_URN
