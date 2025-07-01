import pytest

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import DataHubDeprecationWarning


def test_mcpw_inference() -> None:
    mcpw = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:bigquery,harshal-playground-306419.test_schema.excess_deaths_derived,PROD)",
        aspect=models.DomainsClass(domains=["urn:li:domain:health"]),
    )
    assert mcpw.entityType == "dataset"
    assert mcpw.aspectName == "domains"

    with pytest.raises(ValueError):
        mcpw = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:bigquery,harshal-playground-306419.test_schema.excess_deaths_derived,PROD)",
            entityType="incorrect",
            aspect=models.DomainsClass(domains=["urn:li:domain:health"]),
        )


def test_mcpw_case_coercion() -> None:
    with pytest.warns(DataHubDeprecationWarning):
        mcpw = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:bigquery,harshal-playground-306419.test_schema.excess_deaths_derived,PROD)",
            entityType="DATASET",
            aspect=models.DomainsClass(domains=["urn:li:domain:health"]),
        )
    assert mcpw.entityType == "dataset"


def test_mcpw_from_obj() -> None:
    # Checks that the MCPW from_obj() method returns a MCPW instead
    # of an MCP with a serialized inner aspect object.

    mcpw = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:bigquery,harshal-playground-306419.test_schema.excess_deaths_derived,PROD)",
        aspect=models.DomainsClass(domains=["urn:li:domain:health"]),
    )

    mcpw2 = MetadataChangeProposalWrapper.from_obj(mcpw.to_obj())

    assert isinstance(mcpw2, MetadataChangeProposalWrapper)
    assert mcpw == mcpw2
