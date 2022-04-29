import logging
import os
import re
from typing import Any, Dict, List, Optional, Union

import parse
import pydantic
from wcmatch import pathlib

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.source.aws.aws_common import AwsSourceConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.s3.profiling import DataLakeProfilerConfig

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)

SUPPORTED_FILE_TYPES = ["csv", "tsv", "json", "parquet", "avro"]


class PathSpec(ConfigModel):
    class Config:
        arbitrary_types_allowed = True

    include: str
    exclude: Optional[List[str]]
    file_types: List[str] = SUPPORTED_FILE_TYPES
    table_name: Optional[str]

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

        if ext != "*" and ext not in self.file_types:
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

        include_ext = os.path.splitext(values["include"])[1].strip(".")
        if include_ext not in values["file_types"] and include_ext != "*":
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
        logger.debug(f'Setting _is_s3: {values.get("_is_s3")}')
        return values


class DataLakeSourceConfig(ConfigModel):
    path_spec: PathSpec
    env: str = DEFAULT_ENV
    platform_instance: Optional[str] = None
    platform: str = ""  # overwritten by validator below

    aws_config: Optional[AwsSourceConfig] = None

    # Whether or not to create in datahub from the s3 bucket
    use_s3_bucket_tags: Optional[bool] = None
    # Whether or not to create in datahub from the s3 object
    use_s3_object_tags: Optional[bool] = None

    profile_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()
    profiling: DataLakeProfilerConfig = DataLakeProfilerConfig()

    spark_driver_memory: str = "4g"

    max_rows: int = 100

    @pydantic.root_validator(pre=False)
    def validate_platform(cls, values: Dict) -> Dict:
        value = values.get("platform")
        if value is not None and value != "":
            return values
        if values.get("path_spec") is not None and isinstance(
            values.get("path_spec"), PathSpec
        ):
            if values["path_spec"].is_s3():
                values["platform"] = "s3"
            else:
                if values.get("use_s3_object_tags") or values.get("use_s3_bucket_tags"):
                    raise ValueError(
                        "cannot grab s3 tags for platform != s3. Remove the flag or use s3."
                    )
                values["platform"] = "file"
            logger.debug(f'Setting config "platform": {values.get("platform")}')
            return values
        else:
            raise ValueError("path_spec is not valid. Cannot autodetect platform")

    @pydantic.root_validator()
    def ensure_profiling_pattern_is_passed_to_profiling(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        profiling = values.get("profiling")
        if profiling is not None and profiling.enabled:
            profiling.allow_deny_patterns = values["profile_patterns"]
        return values
