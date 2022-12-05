import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple, Union

import parse
import pydantic
from cached_property import cached_property
from pydantic.fields import Field
from wcmatch import pathlib

from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.aws.s3_util import is_s3_uri

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
        description="Display name of the dataset.Combination of named variables from include path and strings",
    )

    enable_compression: bool = Field(
        default=True,
        description="Enable or disable processing compressed files. Currently .gz and .bz files are supported.",
    )

    sample_files: bool = Field(
        default=True,
        description="Not listing all the files but only taking a handful amount of sample file to infer the schema. File count and file size calculation will be disabled. This can affect performance significantly if enabled",
    )

    def allowed(self, path: str) -> bool:
        logger.debug(f"Checking file to inclusion: {path}")
        if not pathlib.PurePath(path).globmatch(
            self.glob_include, flags=pathlib.GLOBSTAR
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

    @classmethod
    def get_parsable_include(cls, include: str) -> str:
        parsable_include = include
        for i in range(parsable_include.count("*")):
            parsable_include = parsable_include.replace("*", f"{{folder[{i}]}}", 1)
        return parsable_include

    def get_named_vars(self, path: str) -> Union[None, parse.Result, parse.Match]:
        return self.compiled_include.parse(path)

    @pydantic.validator("include")
    def validate_no_double_stars(cls, v: str) -> str:
        if "**" in v:
            raise ValueError("path_spec.include cannot contain '**'")
        return v

    @pydantic.validator("file_types", always=True)
    def validate_file_types(cls, v: Optional[List[str]]) -> List[str]:
        if v is None:
            return SUPPORTED_FILE_TYPES
        else:
            for file_type in v:
                if file_type not in SUPPORTED_FILE_TYPES:
                    raise ValueError(
                        f"file type {file_type} not in supported file types. Please specify one from {SUPPORTED_FILE_TYPES}"
                    )
            return v

    @pydantic.validator("default_extension")
    def validate_default_extension(cls, v):
        if v not in SUPPORTED_FILE_TYPES:
            raise ValueError(
                f"default extension {v} not in supported default file extension. Please specify one from {SUPPORTED_FILE_TYPES}"
            )
        return v

    @pydantic.validator("sample_files", always=True)
    def turn_off_sampling_for_non_s3(cls, v, values):
        is_s3 = is_s3_uri(values.get("include") or "")
        if not is_s3:
            # Sampling only makes sense on s3 currently
            v = False
        return v

    @pydantic.validator("exclude", each_item=True)
    def no_named_fields_in_exclude(cls, v: str) -> str:
        if len(parse.compile(v).named_fields) != 0:
            raise ValueError(
                f"path_spec.exclude {v} should not contain any named variables"
            )
        return v

    @pydantic.validator("table_name", always=True)
    def table_name_in_include(cls, v, values):
        if "include" not in values:
            return v

        parsable_include = PathSpec.get_parsable_include(values["include"])
        compiled_include = parse.compile(parsable_include)

        if v is None:
            if "{table}" in values["include"]:
                v = "{table}"
        else:
            logger.debug(f"include fields: {compiled_include.named_fields}")
            logger.debug(f"table_name fields: {parse.compile(v).named_fields}")
            if not all(
                x in compiled_include.named_fields
                for x in parse.compile(v).named_fields
            ):
                raise ValueError(
                    f"Not all named variables used in path_spec.table_name {v} are specified in path_spec.include {values['include']}"
                )
        return v

    @cached_property
    def is_s3(self):
        return is_s3_uri(self.include)

    @cached_property
    def compiled_include(self):
        parsable_include = PathSpec.get_parsable_include(self.include)
        logger.debug(f"parsable_include: {parsable_include}")
        compiled_include = parse.compile(parsable_include)
        logger.debug(f"Setting compiled_include: {compiled_include}")
        return compiled_include

    @cached_property
    def glob_include(self):
        glob_include = re.sub(r"\{[^}]+\}", "*", self.include)
        logger.debug(f"Setting _glob_include: {glob_include}")
        return glob_include

    @pydantic.root_validator()
    def validate_path_spec(cls, values: Dict) -> Dict[str, Any]:

        # validate that main fields are populated
        required_fields = ["include", "file_types", "default_extension"]
        for f in required_fields:
            if f not in values:
                logger.debug(
                    f"Failed to validate because {f} wasn't populated correctly"
                )
                return values

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

        return values

    def _extract_table_name(self, named_vars: dict) -> str:
        if self.table_name is None:
            raise ValueError("path_spec.table_name is not set")
        return self.table_name.format_map(named_vars)

    def extract_table_name_and_path(self, path: str) -> Tuple[str, str]:
        parsed_vars = self.get_named_vars(path)
        if parsed_vars is None or "table" not in parsed_vars.named:
            return os.path.basename(path), path
        else:
            include = self.include
            depth = include.count("/", 0, include.find("{table}"))
            table_path = (
                "/".join(path.split("/")[:depth]) + "/" + parsed_vars.named["table"]
            )
            return self._extract_table_name(parsed_vars.named), table_path
