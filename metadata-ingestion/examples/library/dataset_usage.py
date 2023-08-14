# Imports for urn construction utility methods
from datetime import datetime

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    CalendarIntervalClass,
    TimeWindowSizeClass,
    DatasetProfileClass,
)


# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

usage_1: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=make_dataset_urn("snowflake", "test_db.test_schema.table_1"),
    aspect=DatasetProfileClass(
        timestampMillis=round(
            datetime.strptime("2023-08-10", "%Y-%m-%d").timestamp() * 1000
        ),
        eventGranularity=TimeWindowSizeClass(unit=CalendarIntervalClass.DAY),
        rowCount=1000000,
        columnCount=5,
        sizeInBytes=100000,
    ),
)

usage_2: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=make_dataset_urn("snowflake", "test_db.test_schema.table_1"),
    aspect=DatasetProfileClass(
        timestampMillis=round(
            datetime.strptime("2023-08-11", "%Y-%m-%d").timestamp() * 1000
        ),
        eventGranularity=TimeWindowSizeClass(unit=CalendarIntervalClass.DAY),
        rowCount=1120000,
        columnCount=5,
        sizeInBytes=130000,
    ),
)

usage_3: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=make_dataset_urn("snowflake", "test_db.test_schema.table_1"),
    aspect=DatasetProfileClass(
        timestampMillis=round(
            datetime.strptime("2023-08-13", "%Y-%m-%d").timestamp() * 1000
        ),
        eventGranularity=TimeWindowSizeClass(unit=CalendarIntervalClass.DAY),
        rowCount=1520000,
        columnCount=5,
        sizeInBytes=230000,
    ),
)

rest_emitter.emit(usage_1)
rest_emitter.emit(usage_2)
rest_emitter.emit(usage_3)
