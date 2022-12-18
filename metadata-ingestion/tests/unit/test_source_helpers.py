from typing import List, Union

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.utilities.source_helpers import auto_status_aspect, auto_workunit

_base_metadata: List[
    Union[MetadataChangeProposalWrapper, models.MetadataChangeEventClass]
] = [
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
    models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_aha.staffing,PROD)",
            aspects=[
                models.DatasetPropertiesClass(
                    customProperties={
                        "key": "value",
                    },
                ),
            ],
        ),
    ),
    models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_aha.hospital_beds,PROD)",
            aspects=[
                models.StatusClass(removed=True),
            ],
        ),
    ),
]


def test_auto_workunit():
    wu = list(auto_workunit(_base_metadata))
    assert all(isinstance(w, MetadataWorkUnit) for w in wu)

    ids = [w.id for w in wu]
    assert ids == [
        "urn:li:container:008e111aa1d250dd52e0fd5d4b307b1a-containerProperties",
        "urn:li:container:108e111aa1d250dd52e0fd5d4b307b12-status",
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_aha.staffing,PROD)/mce",
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_aha.hospital_beds,PROD)/mce",
    ]


def test_auto_status_aspect():
    initial_wu = list(auto_workunit(_base_metadata))

    expected = [
        *initial_wu,
        *list(
            auto_workunit(
                [
                    MetadataChangeProposalWrapper(
                        entityUrn="urn:li:container:008e111aa1d250dd52e0fd5d4b307b1a",
                        aspect=models.StatusClass(removed=False),
                    ),
                    MetadataChangeProposalWrapper(
                        entityUrn="urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_aha.staffing,PROD)",
                        aspect=models.StatusClass(removed=False),
                    ),
                ]
            )
        ),
    ]
    assert list(auto_status_aspect(initial_wu)) == expected
