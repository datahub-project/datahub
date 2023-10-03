import re

import pydantic

from datahub.configuration.common import ConfigModel, ConfigurationError

# Regexp for sharded tables.
# A sharded table is a table that has a suffix of the form _yyyymmdd or yyyymmdd, where yyyymmdd is a date.
# The regexp checks for valid dates in the suffix (e.g. 20200101, 20200229, 20201231) and if the date is not valid
# then it is not a sharded table.
_BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX: str = (
    "((.+\\D)[_$]?)?(\\d\\d\\d\\d(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01]))$"
)


class BigQueryBaseConfig(ConfigModel):
    rate_limit: bool = pydantic.Field(
        default=False, description="Should we rate limit requests made to API."
    )
    requests_per_min: int = pydantic.Field(
        default=60,
        description="Used to control number of API calls made per min. Only used when `rate_limit` is set to `True`.",
    )

    temp_table_dataset_prefix: str = pydantic.Field(
        default="_",
        description="If you are creating temp tables in a dataset with a particular prefix you can use this config to set the prefix for the dataset. This is to support workflows from before bigquery's introduction of temp tables. By default we use `_` because of datasets that begin with an underscore are hidden by default https://cloud.google.com/bigquery/docs/datasets#dataset-naming.",
    )

    sharded_table_pattern: str = pydantic.Field(
        deprecated=True,
        default=_BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX,
        description="The regex pattern to match sharded tables and group as one table. This is a very low level config parameter, only change if you know what you are doing, ",
    )

    @pydantic.validator("sharded_table_pattern")
    def sharded_table_pattern_is_a_valid_regexp(cls, v):
        try:
            re.compile(v)
        except Exception as e:
            raise ConfigurationError(
                f"sharded_table_pattern configuration pattern is invalid. The exception was: {e}"
            )
        return v
