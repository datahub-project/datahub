from typing import List

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.utilities.source_helpers import auto_status_aspect

_base_metadata: List[MetadataChangeProposalWrapper] = [
    MetadataChangeProposalWrapper(
        entityUrn="urn:li:container:008e111aa1d250dd52e0fd5d4b307b1a",
        aspect=models.ContainerPropertiesClass(
            name="test",
        ),
    ),
    MetadataChangeProposalWrapper(
        entityUrn="urn:li:container:108e111aa1d250dd52e0fd5d4b307b12",
        aspect=models.StatusClass(removed=True),
    ),
    MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_aha.staffing,PROD)",
        aspect=models.DatasetPropertiesClass(
            customProperties={
                "key": "value",
            },
        ),
    ),
    MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_aha.hospital_beds,PROD)",
        aspect=models.StatusClass(removed=True),
    ),
]


def test_auto_status_aspect():
    initial_wu = [mcp.as_workunit() for mcp in _base_metadata]
    expected = [
        *initial_wu,
        MetadataChangeProposalWrapper(
            entityUrn="urn:li:container:008e111aa1d250dd52e0fd5d4b307b1a",
            aspect=models.StatusClass(removed=False),
        ).as_workunit(),
        MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_aha.staffing,PROD)",
            aspect=models.StatusClass(removed=False),
        ).as_workunit(),
    ]
    assert list(auto_status_aspect(initial_wu)) == expected
