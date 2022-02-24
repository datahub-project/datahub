import logging
from typing import List

import pytest
import yaml

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.metadata.lineage import LineageConfig, LineageFileSource
from datahub.metadata.schema_classes import UpstreamClass

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
        """
    config = yaml.safe_load(sample_lineage)
    lineage_config: LineageConfig = LineageConfig.parse_obj(config)
    mcp = list(
        LineageFileSource.get_lineage_metadata_change_event_proposal(
            entities=lineage_config.lineage, preserve_upstream=False
        )
    )
    return mcp


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
    lineage_config: LineageConfig = LineageConfig.parse_obj(config)
    mcp = list(
        LineageFileSource.get_lineage_metadata_change_event_proposal(
            entities=lineage_config.lineage, preserve_upstream=False
        )
    )
    return mcp


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
    lineage_config: LineageConfig = LineageConfig.parse_obj(config)
    mcp = list(
        LineageFileSource.get_lineage_metadata_change_event_proposal(
            entities=lineage_config.lineage, preserve_upstream=False
        )
    )
    return mcp


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
    lineage_config: LineageConfig = LineageConfig.parse_obj(config)
    mcp = list(
        LineageFileSource.get_lineage_metadata_change_event_proposal(
            entities=lineage_config.lineage, preserve_upstream=False
        )
    )
    return mcp


def test_basic_lineage_entity_root_node_urn(basic_mcp):
    """
    Checks to see if the entityUrn extracted is correct for the root entity node
    """

    assert (
        basic_mcp[0].entityUrn
        == "urn:li:dataset:(urn:li:dataPlatform:kafka,topic3,DEV)"
    )


def test_basic_lineage_upstream_urns(basic_mcp):
    """
    Checks to see if the upstream urns are correct for a basic_mcp example
    """
    basic_mcp_upstreams: List[UpstreamClass] = basic_mcp[0].aspect.upstreams
    assert (
        basic_mcp_upstreams[0].dataset
        == "urn:li:dataset:(urn:li:dataPlatform:kafka,topic1,DEV)"
        and basic_mcp_upstreams[1].dataset
        == "urn:li:dataset:(urn:li:dataPlatform:kafka,topic2,DEV)"
    )


def test_unsupported_entity_type():
    """
    Checks to see how we handle the case of unsupported entity types.
    If validation is working correctly, it should raise a ConfigurationError
    """
    with pytest.raises(ConfigurationError):
        unsupported_entity_type_mcp()


def test_unsupported_upstream_entity_type():
    """
    Checks to see how invalid types work in the upstream node.
    If validation is working correctly, it should raise a ConfigurationError
    """
    with pytest.raises(ConfigurationError):
        unsupported_upstream_entity_type_mcp()


def test_unsupported_entity_env():
    """
    Checks to see how invalid envs work.
    If validation is working correctly, it should raise a ConfigurationError
    """
    with pytest.raises(ConfigurationError):
        unsupported_entity_env_mcp()
