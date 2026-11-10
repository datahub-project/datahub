import logging
from typing import Dict, List
from unittest.mock import patch

import pytest
import yaml
from pydantic import ValidationError

from datahub.ingestion.source.metadata.lineage import LineageConfig, _get_lineage_mcp
from datahub.metadata.schema_classes import (
    AuditStampClass,
    FineGrainedLineageClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def basic_mcp():
    """
    The below mcp should represent a lineage that looks like this
    topic1 -
             ->topic3
    topic2 -
    :return:
    """
    sample_lineage = """
        lineage:
          - entity:
              name: topic3
              type: dataset
              env: DEV
              platform: kafka
            upstream:
              - entity:
                  name: topic1
                  type: dataset
                  env: DEV
                  platform: kafka
              - entity:
                  name: topic2
                  type: dataset
                  env: DEV
                  platform: kafka
            fineGrainedLineages:
            - upstreamType: FIELD_SET
              upstreams:
                - urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:kafka,topic1,PROD),user_id)
              downstreamType: FIELD_SET
              downstreams:
                - urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:kafka,topic3,PROD),user_id)
              confidenceScore: 0.9
              transformOperation: func1
        """
    config = yaml.safe_load(sample_lineage)
    lineage_config: LineageConfig = LineageConfig.model_validate(config)
    return _get_lineage_mcp(lineage_config.lineage[0], False)


def unsupported_entity_type_mcp():
    sample_lineage = """
        lineage:
          - entity:
              name: topic3
              type: NotSupported!
              env: DEV
              platform: kafka
            upstream:
              - entity:
                  name: topic1
                  type: dataset
                  env: DEV
                  platform: kafka
              - entity:
                  name: topic2
                  type: dataset
                  env: DEV
                  platform: kafka
          - entity:
              name: topic6
              type: dataset
              env: DEV
              platform: kafka
            upstream:
              - entity:
                  name: topic4
                  type: dataset
                  env: DEV
                  platform: kafka
              - entity:
                  name: topic5
                  type: dataset
                  env: DEV
                  platform: kafka
        """
    config = yaml.safe_load(sample_lineage)
    return LineageConfig.model_validate(config)


def unsupported_upstream_entity_type_mcp():
    sample_lineage = """
        lineage:
          - entity:
              name: topic3
              type: dataset
              env: DEV
              platform: kafka
            upstream:
              - entity:
                  name: topic1
                  type: NotSupported
                  env: DEV
                  platform: kafka
              - entity:
                  name: topic2
                  type: dataset
                  env: DEV
                  platform: kafka
        """
    config = yaml.safe_load(sample_lineage)
    return LineageConfig.model_validate(config)


def unsupported_entity_env_mcp():
    sample_lineage = """
        lineage:
          - entity:
              name: topic3
              type: dataset
              env: NotSupported!
              platform: kafka
            upstream:
              - entity:
                  name: topic1
                  type: dataset
                  env: DEV
                  platform: kafka
              - entity:
                  name: topic2
                  type: dataset
                  env: DEV
                  platform: kafka
          - entity:
              name: topic6
              type: dataset
              env: DEV
              platform: kafka
            upstream:
              - entity:
                  name: topic4
                  type: dataset
                  env: DEV
                  platform: kafka
              - entity:
                  name: topic5
                  type: dataset
                  env: DEV
                  platform: kafka
        """
    config = yaml.safe_load(sample_lineage)
    return LineageConfig.model_validate(config)


def test_basic_lineage_entity_root_node_urn(basic_mcp):
    """
    Checks to see if the entityUrn extracted is correct for the root entity node
    """

    assert (
        basic_mcp.entityUrn == "urn:li:dataset:(urn:li:dataPlatform:kafka,topic3,DEV)"
    )


def test_basic_lineage_upstream_urns(basic_mcp):
    """
    Checks to see if the upstream urns are correct for a basic_mcp example
    """
    basic_mcp_upstreams: List[UpstreamClass] = basic_mcp.aspect.upstreams
    assert (
        basic_mcp_upstreams[0].dataset
        == "urn:li:dataset:(urn:li:dataPlatform:kafka,topic1,DEV)"
        and basic_mcp_upstreams[1].dataset
        == "urn:li:dataset:(urn:li:dataPlatform:kafka,topic2,DEV)"
    )


def test_basic_lineage_finegrained_upstream_urns(basic_mcp):
    """
    Checks to see if the finegrained urns are correct for a basic_mcp example
    """
    fine_grained_lineage: FineGrainedLineageClass = (
        basic_mcp.aspect.fineGrainedLineages[0]
    )
    assert fine_grained_lineage.upstreamType == "FIELD_SET"
    assert fine_grained_lineage.downstreamType == "FIELD_SET"
    assert fine_grained_lineage.confidenceScore == 0.9
    assert fine_grained_lineage.transformOperation == "func1"


def test_unsupported_entity_type():
    """
    Checks to see how we handle the case of unsupported entity types.
    If validation is working correctly, it should raise a ValidationError
    """
    with pytest.raises(ValidationError):
        unsupported_entity_type_mcp()


def test_unsupported_upstream_entity_type():
    """
    Checks to see how invalid types work in the upstream node.
    If validation is working correctly, it should raise a ValidationError
    """
    with pytest.raises(ValidationError):
        unsupported_upstream_entity_type_mcp()


def test_unsupported_entity_env():
    """
    Checks to see how invalid envs work.
    If validation is working correctly, it should raise a ValidationError
    """
    with pytest.raises(ValidationError):
        unsupported_entity_env_mcp()


TOPIC1 = "urn:li:dataset:(urn:li:dataPlatform:kafka,topic1,DEV)"
TOPIC2 = "urn:li:dataset:(urn:li:dataPlatform:kafka,topic2,DEV)"
OTHER = "urn:li:dataset:(urn:li:dataPlatform:kafka,other,DEV)"
STORED_STAMP = AuditStampClass(time=0, actor="urn:li:corpuser:datahub")

# topic1 is listed twice, as in a file that repeats an upstream.
REPEATED_UPSTREAM_LINEAGE = """
    lineage:
      - entity: {name: topic3, type: dataset, env: DEV, platform: kafka}
        upstream:
          - entity: {name: topic1, type: dataset, env: DEV, platform: kafka}
          - entity: {name: topic2, type: dataset, env: DEV, platform: kafka}
          - entity: {name: topic1, type: dataset, env: DEV, platform: kafka}
    """


def _lineage_mcp(
    preserve_upstream: bool, stored: Dict[str, UpstreamLineageClass]
) -> UpstreamLineageClass:
    lineage_config = LineageConfig.model_validate(
        yaml.safe_load(REPEATED_UPSTREAM_LINEAGE)
    )
    with (
        patch("datahub.ingestion.source.metadata.lineage.get_default_graph"),
        patch(
            "datahub.ingestion.source.metadata.lineage.get_aspects_for_entity",
            return_value=stored,
        ),
    ):
        mcp = _get_lineage_mcp(lineage_config.lineage[0], preserve_upstream)
    assert mcp is not None and isinstance(mcp.aspect, UpstreamLineageClass)
    return mcp.aspect


def test_preserve_upstream_keeps_one_edge_per_upstream() -> None:
    """Copies left by earlier runs collapse, and a stored upstream keeps its
    type and auditStamp."""
    stored = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=TOPIC1, type="COPY", auditStamp=STORED_STAMP),
            UpstreamClass(dataset=OTHER, type="TRANSFORMED", auditStamp=STORED_STAMP),
            UpstreamClass(dataset=TOPIC1, type="COPY", auditStamp=STORED_STAMP),
        ],
    )

    upstreams = _lineage_mcp(True, {"upstreamLineage": stored}).upstreams

    assert [u.dataset for u in upstreams] == [TOPIC1, OTHER, TOPIC2]
    assert upstreams[0].type == "COPY"
    assert upstreams[0].auditStamp == STORED_STAMP


@pytest.mark.parametrize(
    "preserve_upstream, stored",
    [
        pytest.param(True, {}, id="preserve_nothing_stored"),
        pytest.param(False, {}, id="replace"),
    ],
)
def test_file_upstreams_emitted_once(
    preserve_upstream: bool, stored: Dict[str, UpstreamLineageClass]
) -> None:
    upstreams = _lineage_mcp(preserve_upstream, stored).upstreams

    assert [u.dataset for u in upstreams] == [TOPIC1, TOPIC2]
