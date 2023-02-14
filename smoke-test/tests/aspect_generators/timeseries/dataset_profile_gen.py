from typing import Iterable

from datahub.metadata.schema_classes import (DatasetFieldProfileClass,
                                             DatasetProfileClass,
                                             TimeWindowSizeClass)

from tests.utils import get_timestampmillis_at_start_of_day


def gen_dataset_profiles(
    num_days: int = 30,
) -> Iterable[DatasetProfileClass]:
    """
    Generates `num_days` number of test dataset profiles for the entity
    represented by the test_dataset_urn, starting from the start time of
    now - num_days + 1 day to the start of today.
    """
    num_rows: int = 100
    num_columns: int = 1
    # [-num_days + 1, -num_days + 2, ..., 0]
    for relative_day_num in range(-num_days + 1, 1):
        timestampMillis: int = get_timestampmillis_at_start_of_day(relative_day_num)
        profile = DatasetProfileClass(
            timestampMillis=timestampMillis,
            eventGranularity=TimeWindowSizeClass(unit="DAY", multiple=1),
        )
        profile.rowCount = num_rows
        num_rows += 100
        profile.columnCount = num_columns
        profile.fieldProfiles = []
        field_profile = DatasetFieldProfileClass(fieldPath="test_column")
        field_profile.uniqueCount = int(num_rows / 2)
        field_profile.uniqueProportion = float(0.5)
        field_profile.nullCount = int(num_rows / 10)
        field_profile.nullProportion = float(0.1)
        field_profile.min = "10"
        field_profile.max = "20"
        field_profile.mean = "15"
        field_profile.median = "12"
        field_profile.stdev = "3"
        profile.fieldProfiles.append(field_profile)
        yield profile
