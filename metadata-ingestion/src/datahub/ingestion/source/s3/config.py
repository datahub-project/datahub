import logging
import os
import re
from typing import Any, Dict, List, Optional, Union

import parse
import pydantic
from pydantic.fields import Field
from wcmatch import pathlib

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvBasedSourceConfigBase,
    PlatformSourceConfigBase,
)
from datahub.ingestion.source.aws.aws_common import AwsSourceConfig
from datahub.ingestion.source.aws.s3_util import get_bucket_name, is_s3_uri
from datahub.ingestion.source.s3.profiling import DataLakeProfilerConfig

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)

SUPPORTED_FILE_TYPES: List[str] = ["csv", "tsv", "json", "parquet", "avro"]
SUPPORTED_COMPRESSIONS: List[str] = ["gz", "bz2"]


class PathSpec(ConfigModel):
    class Config:
        arbitrary_types_allowed = True

    include: str = Field(
        description="Path to table (s3 or local file system). Name variable {table} is used to mark the folder with dataset. In absence of {table}, file level dataset will be created. Check below examples for more details."
    )
    exclude: Optional[List[str]] = Field(
        default=None,
        description="list of paths in glob pattern which will be excluded while scanning for the datasets",
    )
    file_types: List[str] = Field(
        default=SUPPORTED_FILE_TYPES,
        description="Files with extenstions specified here (subset of default value) only will be scanned to create dataset. Other files will be omitted.",
    )

    default_extension: Optional[str] = Field(
        description="For files without extension it will assume the specified file type. If it is not set the files without extensions will be skipped.",
    )

    table_name: Optional[str] = Field(
        default=None,
        description="Display name of the dataset.Combination of named variableds from include path and strings",
    )

    enable_compression: bool = Field(
        default=True,
        description="Enable or disable processing compressed files. Currenly .gz and .bz files are supported.",
    )

    sample_files: bool = Field(
        default=True,
        description="Not listing all the files but only taking a handful amount of sample file to infer the schema. File count and file size calculation will be disabled. This can affect performance significantly if enabled",
    )

    # to be set internally
    _parsable_include: str
    _compiled_include: parse.Parser
    _glob_include: str
    _is_s3: bool

    def allowed(self, path: str) -> bool:
        logger.debug(f"Checking file to inclusion: {path}")
        if not pathlib.PurePath(path).globmatch(
            self._glob_include, flags=pathlib.GLOBSTAR
        ):
            return False
        logger.debug(f"{path} matched include ")
        if self.exclude:
            for exclude_path in self.exclude:
                if pathlib.PurePath(path).globmatch(
                    exclude_path, flags=pathlib.GLOBSTAR
                ):
                    return False
        logger.debug(f"{path} is not excluded")
        ext = os.path.splitext(path)[1].strip(".")

        if (ext == "" and self.default_extension is None) and (
            ext != "*" and ext not in self.file_types
        ):
            return False

        logger.debug(f"{path} had selected extension {ext}")
        logger.debug(f"{path} allowed for dataset creation")
        return True

    def is_s3(self):
        return self._is_s3

    @classmethod
    def get_parsable_include(cls, include: str) -> str:
        parsable_include = include
        for i in range(parsable_include.count("*")):
            parsable_include = parsable_include.replace("*", f"{{folder[{i}]}}", 1)
        return parsable_include

    def get_named_vars(self, path: str) -> Union[None, parse.Result, parse.Match]:
        return self._compiled_include.parse(path)

    @pydantic.root_validator()
    def validate_path_spec(cls, values: Dict) -> Dict[str, Any]:

        if "**" in values["include"]:
            raise ValueError("path_spec.include cannot contain '**'")

        if values.get("file_types") is None:
            values["file_types"] = SUPPORTED_FILE_TYPES
        else:
            for file_type in values["file_types"]:
                if file_type not in SUPPORTED_FILE_TYPES:
                    raise ValueError(
                        f"file type {file_type} not in supported file types. Please specify one from {SUPPORTED_FILE_TYPES}"
                    )

        if values.get("default_extension") is not None:
            if values.get("default_extension") not in SUPPORTED_FILE_TYPES:
                raise ValueError(
                    f"default extension {values.get('default_extension')} not in supported default file extension. Please specify one from {SUPPORTED_FILE_TYPES}"
                )

        include_ext = os.path.splitext(values["include"])[1].strip(".")
        if (
            include_ext not in values["file_types"]
            and include_ext != "*"
            and not values["default_extension"]
            and include_ext not in SUPPORTED_COMPRESSIONS
        ):
            raise ValueError(
                f"file type specified ({include_ext}) in path_spec.include is not in specified file "
                f'types. Please select one from {values.get("file_types")} or specify ".*" to allow all types'
            )

        values["_parsable_include"] = PathSpec.get_parsable_include(values["include"])
        logger.debug(f'Setting _parsable_include: {values.get("_parsable_include")}')
        compiled_include_tmp = parse.compile(values["_parsable_include"])
        values["_compiled_include"] = compiled_include_tmp
        logger.debug(f'Setting _compiled_include: {values["_compiled_include"]}')
        values["_glob_include"] = re.sub(
            "\{[^}]+\}", "*", values["include"]  # noqa: W605
        )
        logger.debug(f'Setting _glob_include: {values.get("_glob_include")}')

        if values.get("table_name") is None:
            if "{table}" in values["include"]:
                values["table_name"] = "{table}"
        else:
            logger.debug(f"include fields: {compiled_include_tmp.named_fields}")
            logger.debug(
                f"table_name fields: {parse.compile(values['table_name']).named_fields}"
            )
            if not all(
                x in values["_compiled_include"].named_fields
                for x in parse.compile(values["table_name"]).named_fields
            ):
                raise ValueError(
                    "Not all named variables used in path_spec.table_name are specified in "
                    "path_spec.include"
                )

        if values.get("exclude") is not None:
            for exclude_path in values["exclude"]:
                if len(parse.compile(exclude_path).named_fields) != 0:
                    raise ValueError(
                        "path_spec.exclude should not contain any named variables"
                    )

        values["_is_s3"] = is_s3_uri(values["include"])
        if not values["_is_s3"]:
            # Sampling only makes sense on s3 currently
            values["sample_files"] = False
        logger.debug(f'Setting _is_s3: {values.get("_is_s3")}')
        return values


class DataLakeSourceConfig(PlatformSourceConfigBase, EnvBasedSourceConfigBase):
    path_specs: Optional[List[PathSpec]] = Field(
        description="List of PathSpec. See below the details about PathSpec"
    )
    path_spec: Optional[PathSpec] = Field(
        description="Path spec will be deprecated in favour of path_specs option."
    )
    platform: str = Field(
        default="", description="The platform that this source connects to"
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="The instance of the platform that all assets produced by this recipe belong to",
    )
    aws_config: Optional[AwsSourceConfig] = Field(
        default=None, description="AWS configuration"
    )

    # Whether or not to create in datahub from the s3 bucket
    use_s3_bucket_tags: Optional[bool] = Field(
        None, description="Whether or not to create tags in datahub from the s3 bucket"
    )
    # Whether or not to create in datahub from the s3 object
    use_s3_object_tags: Optional[bool] = Field(
        None,
        description="# Whether or not to create tags in datahub from the s3 object",
    )

    profile_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for tables to profile ",
    )
    profiling: DataLakeProfilerConfig = Field(
        default=DataLakeProfilerConfig(), description="Data profiling configuration"
    )

    spark_driver_memory: str = Field(
        default="4g", description="Max amount of memory to grant Spark."
    )

    max_rows: int = Field(
        default=100,
        description="Maximum number of rows to use when inferring schemas for TSV and CSV files.",
    )

    @pydantic.root_validator(pre=False)
    def validate_platform(cls, values: Dict) -> Dict:
        value = values.get("platform")
        if value is not None and value != "":
            return values

        if not values.get("path_specs") and not values.get("path_spec"):
            raise ValueError("Either path_specs or path_spec needs to be specified")

        if values.get("path_specs") and values.get("path_spec"):
            raise ValueError(
                "Either path_specs or path_spec needs to be specified but not both"
            )

        if values.get("path_spec"):
            logger.warning(
                "path_spec config property is deprecated, please use path_specs instead of it."
            )
            values["path_specs"] = [values.get("path_spec")]

        bucket_name: str = ""
        for path_spec in values.get("path_specs", []):
            if path_spec.is_s3():
                platform = "s3"
            else:
                if values.get("use_s3_object_tags") or values.get("use_s3_bucket_tags"):
                    raise ValueError(
                        "cannot grab s3 tags for platform != s3. Remove the flag or use s3."
                    )

                platform = "file"

            if values.get("platform", "") != "":
                if values["platform"] != platform:
                    raise ValueError("all path_spec should belong to the same platform")
            else:
                values["platform"] = platform
                logger.debug(f'Setting config "platform": {values.get("platform")}')

            if platform == "s3":
                if bucket_name == "":
                    bucket_name = get_bucket_name(path_spec.include)
                else:
                    if bucket_name != get_bucket_name(path_spec.include):
                        raise ValueError(
                            "all path_spec should reference the same s3 bucket"
                        )

        return values

    @pydantic.root_validator()
    def ensure_profiling_pattern_is_passed_to_profiling(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        profiling = values.get("profiling")
        if profiling is not None and profiling.enabled:
            profiling.allow_deny_patterns = values["profile_patterns"]
        return values
