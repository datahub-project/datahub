import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple, Union

import parse
import pydantic
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
        values["_glob_include"] = re.sub(r"\{[^}]+\}", "*", values["include"])
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
