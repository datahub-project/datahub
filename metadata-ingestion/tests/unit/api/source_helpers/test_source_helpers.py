import logging
from datetime import datetime
from typing import List, Union

import pytest
from freezegun import freeze_time

import datahub.metadata.schema_classes as models
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.auto_work_units.auto_dataset_properties_aspect import (
    auto_patch_last_modified,
)
from datahub.ingestion.api.source_helpers import (
    auto_empty_dataset_usage_statistics,
    auto_lowercase_urns,
    auto_status_aspect,
    auto_workunit,
    create_dataset_props_patch_builder,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OperationTypeClass,
    TimeStampClass,
)
from datahub.specific.dataset import DatasetPatchBuilder

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


def test_auto_lowercase_aspects():
    mcws = auto_workunit(
        [
            MetadataChangeProposalWrapper(
                entityUrn=make_dataset_urn(
                    "bigquery", "myProject.mySchema.myTable", "PROD"
                ),
                aspect=models.DatasetKeyClass(
                    "urn:li:dataPlatform:bigquery", "myProject.mySchema.myTable", "PROD"
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn="urn:li:container:008e111aa1d250dd52e0fd5d4b307b1a",
                aspect=models.ContainerPropertiesClass(
                    name="test",
                ),
            ),
            models.MetadataChangeEventClass(
                proposedSnapshot=models.DatasetSnapshotClass(
                    urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-Public-Data.Covid19_Aha.staffing,PROD)",
                    aspects=[
                        models.DatasetPropertiesClass(
                            customProperties={
                                "key": "value",
                            },
                        ),
                    ],
                ),
            ),
        ]
    )

    expected = [
        *list(
            auto_workunit(
                [
                    MetadataChangeProposalWrapper(
                        entityUrn="urn:li:dataset:(urn:li:dataPlatform:bigquery,myproject.myschema.mytable,PROD)",
                        aspect=models.DatasetKeyClass(
                            "urn:li:dataPlatform:bigquery",
                            "myProject.mySchema.myTable",
                            "PROD",
                        ),
                    ),
                    MetadataChangeProposalWrapper(
                        entityUrn="urn:li:container:008e111aa1d250dd52e0fd5d4b307b1a",
                        aspect=models.ContainerPropertiesClass(
                            name="test",
                        ),
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
                ]
            )
        ),
    ]
    assert list(auto_lowercase_urns(mcws)) == expected


@freeze_time("2023-01-02 00:00:00")
def test_auto_empty_dataset_usage_statistics(caplog: pytest.LogCaptureFixture) -> None:
    has_urn = make_dataset_urn("my_platform", "has_aspect")
    empty_urn = make_dataset_urn("my_platform", "no_aspect")
    config = BaseTimeWindowConfig()
    wus = [
        MetadataChangeProposalWrapper(
            entityUrn=has_urn,
            aspect=models.DatasetUsageStatisticsClass(
                timestampMillis=int(config.start_time.timestamp() * 1000),
                eventGranularity=models.TimeWindowSizeClass(
                    models.CalendarIntervalClass.DAY
                ),
                uniqueUserCount=1,
                totalSqlQueries=1,
            ),
        ).as_workunit()
    ]
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        new_wus = list(
            auto_empty_dataset_usage_statistics(
                wus,
                dataset_urns={has_urn, empty_urn},
                config=config,
                all_buckets=False,
            )
        )
        assert not caplog.records

    assert new_wus == [
        *wus,
        MetadataChangeProposalWrapper(
            entityUrn=empty_urn,
            aspect=models.DatasetUsageStatisticsClass(
                timestampMillis=int(datetime(2023, 1, 1).timestamp() * 1000),
                eventGranularity=models.TimeWindowSizeClass(
                    models.CalendarIntervalClass.DAY
                ),
                uniqueUserCount=0,
                totalSqlQueries=0,
                topSqlQueries=[],
                userCounts=[],
                fieldCounts=[],
            ),
        ).as_workunit(),
    ]


@freeze_time("2023-01-02 00:00:00")
def test_auto_empty_dataset_usage_statistics_invalid_timestamp(
    caplog: pytest.LogCaptureFixture,
) -> None:
    urn = make_dataset_urn("my_platform", "my_dataset")
    config = BaseTimeWindowConfig()
    wus = [
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=models.DatasetUsageStatisticsClass(
                timestampMillis=0,
                eventGranularity=models.TimeWindowSizeClass(
                    models.CalendarIntervalClass.DAY
                ),
                uniqueUserCount=1,
                totalSqlQueries=1,
            ),
        ).as_workunit()
    ]
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        new_wus = list(
            auto_empty_dataset_usage_statistics(
                wus,
                dataset_urns={urn},
                config=config,
                all_buckets=True,
            )
        )
        assert len(caplog.records) == 1
        assert "1970-01-01 00:00:00+00:00" in caplog.records[0].msg

    assert new_wus == [
        *wus,
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=models.DatasetUsageStatisticsClass(
                timestampMillis=int(config.start_time.timestamp() * 1000),
                eventGranularity=models.TimeWindowSizeClass(
                    models.CalendarIntervalClass.DAY
                ),
                uniqueUserCount=0,
                totalSqlQueries=0,
                topSqlQueries=[],
                userCounts=[],
                fieldCounts=[],
            ),
            changeType=models.ChangeTypeClass.CREATE,
        ).as_workunit(),
    ]


def get_sample_mcps(mcps_to_append: List = []) -> List[MetadataChangeProposalWrapper]:
    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:dbt,abc.foo.bar,PROD)",
            aspect=models.OperationClass(
                timestampMillis=10,
                lastUpdatedTimestamp=12,
                operationType=OperationTypeClass.CREATE,
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:dbt,abc.foo.bar,PROD)",
            aspect=models.OperationClass(
                timestampMillis=11,
                lastUpdatedTimestamp=20,
                operationType=OperationTypeClass.CREATE,
            ),
        ),
    ]
    mcps.extend(mcps_to_append)
    return mcps


def to_patch_work_units(patch_builder: DatasetPatchBuilder) -> List[MetadataWorkUnit]:
    return [
        MetadataWorkUnit(
            id=MetadataWorkUnit.generate_workunit_id(patch_mcp), mcp_raw=patch_mcp
        )
        for patch_mcp in patch_builder.build()
    ]


def get_auto_generated_wu() -> List[MetadataWorkUnit]:
    dataset_patch_builder = DatasetPatchBuilder(
        urn="urn:li:dataset:(urn:li:dataPlatform:dbt,abc.foo.bar,PROD)"
    ).set_last_modified(TimeStampClass(time=20))

    auto_generated_work_units = to_patch_work_units(dataset_patch_builder)

    return auto_generated_work_units


@freeze_time("2023-01-02 00:00:00")
def test_auto_patch_last_modified_no_change():
    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn="urn:li:container:008e111aa1d250dd52e0fd5d4b307b1a",
            aspect=models.StatusClass(removed=False),
        )
    ]

    initial_wu = list(auto_workunit(mcps))

    expected = initial_wu

    assert (
        list(auto_patch_last_modified(initial_wu)) == expected
    )  # There should be no change


@freeze_time("2023-01-02 00:00:00")
def test_auto_patch_last_modified_max_last_updated_timestamp():
    mcps = get_sample_mcps()

    expected = list(auto_workunit(mcps))

    auto_generated_work_units = get_auto_generated_wu()

    expected.extend(auto_generated_work_units)

    # work unit should contain a path of datasetProperties with lastModified set to max of operation.lastUpdatedTime
    # i.e., 20
    assert list(auto_patch_last_modified(auto_workunit(mcps))) == expected


@freeze_time("2023-01-02 00:00:00")
def test_auto_patch_last_modified_multi_patch():
    mcps = get_sample_mcps()

    dataset_patch_builder = DatasetPatchBuilder(
        urn="urn:li:dataset:(urn:li:dataPlatform:dbt,abc.foo.bar,PROD)"
    )

    dataset_patch_builder.set_display_name("foo")
    dataset_patch_builder.set_description("it is fake")

    patch_work_units = to_patch_work_units(dataset_patch_builder)

    work_units = [*list(auto_workunit(mcps)), *patch_work_units]

    auto_generated_work_units = get_auto_generated_wu()

    expected = [*work_units, *auto_generated_work_units]

    # In this case, the final work units include two patch units: one originating from the source and
    # the other from auto_patch_last_modified.
    assert list(auto_patch_last_modified(work_units)) == expected


@freeze_time("2023-01-02 00:00:00")
def test_auto_patch_last_modified_last_modified_patch_exist():
    mcps = get_sample_mcps()

    patch_builder = create_dataset_props_patch_builder(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:dbt,abc.foo.bar,PROD)",
        dataset_properties=DatasetPropertiesClass(
            name="foo",
            description="dataset for collection of foo",
            lastModified=TimeStampClass(time=20),
        ),
    )

    work_units = [
        *list(auto_workunit(mcps)),
        *to_patch_work_units(patch_builder),
    ]
    # The input and output should align since the source is generating a patch for datasetProperties with the
    # lastModified attribute.
    # Therefore, `auto_patch_last_modified` should not create any additional patch.
    assert list(auto_patch_last_modified(work_units)) == work_units


@freeze_time("2023-01-02 00:00:00")
def test_auto_patch_last_modified_last_modified_patch_not_exist():
    mcps = get_sample_mcps()

    patch_builder = create_dataset_props_patch_builder(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:dbt,abc.foo.bar,PROD)",
        dataset_properties=DatasetPropertiesClass(
            name="foo",
            description="dataset for collection of foo",
        ),
    )

    work_units = [
        *list(auto_workunit(mcps)),
        *to_patch_work_units(patch_builder),
    ]

    expected = [
        *work_units,
        *get_auto_generated_wu(),  # The output should include an additional patch for the `lastModified` attribute.
    ]

    assert list(auto_patch_last_modified(work_units)) == expected
