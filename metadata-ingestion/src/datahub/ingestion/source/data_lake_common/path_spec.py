import datetime
import logging
import os
import re
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

import parse
import pydantic
from cached_property import cached_property
from pydantic.fields import Field
from wcmatch import pathlib

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.azure.abs_utils import is_abs_uri
from datahub.ingestion.source.gcs.gcs_utils import is_gcs_uri

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)

SUPPORTED_FILE_TYPES: List[str] = ["csv", "tsv", "json", "parquet", "avro"]

# These come from the smart_open library.
SUPPORTED_COMPRESSIONS: List[str] = [
    "gz",
    "bz2",
    # We have a monkeypatch on smart_open that aliases .gzip to .gz.
    "gzip",
]

java_to_python_mapping = {
    "yyyy": "Y",
    "MM": "m",
    "dd": "d",
    "HH": "H",
    "mm": "M",
    "ss": "S",
}


class SortKeyType(Enum):
    STRING = "STRING"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    DATETIME = "DATETIME"
    DATE = "DATE"

    def __str__(self):
        return self.value


class SortKey(ConfigModel):
    key: str = Field(
        description="The key to sort on. This can be a compound key based on the path_spec variables."
    )
    type: SortKeyType = Field(
        default=SortKeyType.STRING,
        description="The date format to use when sorting. This is used to parse the date from the key. The format should follow the java [SimpleDateFormat](https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html) format.",
    )

    date_format: Optional[str] = Field(
        default=None,
        type=str,
        description="The date format to use when sorting. This is used to parse the date from the key. The format should follow the java [SimpleDateFormat](https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html) format.",
    )

    @pydantic.validator("date_format", always=True)
    def convert_date_format_to_python_format(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        else:
            for java_format, python_format in java_to_python_mapping.items():
                v = v.replace(java_format, f"%{python_format}")
        return v


class FolderTraversalMethod(Enum):
    ALL = "ALL"
    MIN_MAX = "MIN_MAX"
    MAX = "MAX"


class PathSpec(ConfigModel):
    class Config:
        arbitrary_types_allowed = True

    include: str = Field(
        description="Path to table. Name variable `{table}` is used to mark the folder with dataset. In absence of `{table}`, file level dataset will be created. Check below examples for more details."
    )
    exclude: Optional[List[str]] = Field(
        default=[],
        description="list of paths in glob pattern which will be excluded while scanning for the datasets",
    )
    file_types: List[str] = Field(
        default=SUPPORTED_FILE_TYPES,
        description="Files with extenstions specified here (subset of default value) only will be scanned to create dataset. Other files will be omitted.",
    )

    default_extension: Optional[str] = Field(
        default=None,
        description="For files without extension it will assume the specified file type. If it is not set the files without extensions will be skipped.",
    )

    table_name: Optional[str] = Field(
        default=None,
        description="Display name of the dataset.Combination of named variables from include path and strings",
    )

    # This is not used yet, but will be used in the future to sort the partitions
    sort_key: Optional[SortKey] = Field(
        hidden_from_docs=True,
        default=None,
        description="Sort key to use when sorting the partitions. This is useful when the partitions are not sorted in the order of the data. The key can be a compound key based on the path_spec variables.",
    )

    enable_compression: bool = Field(
        default=True,
        description="Enable or disable processing compressed files. Currently .gz and .bz files are supported.",
    )

    sample_files: bool = Field(
        default=True,
        description="Not listing all the files but only taking a handful amount of sample file to infer the schema. File count and file size calculation will be disabled. This can affect performance significantly if enabled",
    )

    allow_double_stars: bool = Field(
        default=False,
        description="Allow double stars in the include path. This can affect performance significantly if enabled",
    )

    autodetect_partitions: bool = Field(
        default=True,
        description="Autodetect partition(s) from the path. If set to true, it will autodetect partition key/value if the folder format is {partition_key}={partition_value} for example `year=2024`",
    )

    traversal_method: FolderTraversalMethod = Field(
        default=FolderTraversalMethod.MAX,
        description="Method to traverse the folder. ALL: Traverse all the folders, MIN_MAX: Traverse the folders by finding min and max value, MAX: Traverse the folder with max value",
    )

    include_hidden_folders: bool = Field(
        default=False,
        description="Include hidden folders in the traversal (folders starting with . or _",
    )

    tables_filter_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="The tables_filter_pattern configuration field uses regular expressions to filter the tables part of the Pathspec for ingestion, allowing fine-grained control over which tables are included or excluded based on specified patterns. The default setting allows all tables.",
    )

    def is_path_hidden(self, path: str) -> bool:
        # Split the path into directories and filename
        dirs, filename = os.path.split(path)

        # Check the filename
        if filename.startswith(".") or filename.startswith("_"):
            return True

        # Check each directory in the path
        for dir in dirs.split(os.sep):
            if dir.startswith(".") or dir.startswith("_"):
                return True

        return False

    def allowed(self, path: str, ignore_ext: bool = False) -> bool:
        logger.debug(f"Checking file to inclusion: {path}")
        if self.is_path_hidden(path) and not self.include_hidden_folders:
            return False

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

        table_name, _ = self.extract_table_name_and_path(path)
        if not self.tables_filter_pattern.allowed(table_name):
            return False
        logger.debug(f"{path} is passed table name check")

        ext = os.path.splitext(path)[1].strip(".")

        if not ignore_ext:
            if (ext == "" and self.default_extension is None) and (
                ext != "*" and ext not in self.file_types
            ):
                return False

            logger.debug(f"{path} had selected extension {ext}")
            logger.debug(f"{path} allowed for dataset creation")
        return True

    def dir_allowed(self, path: str) -> bool:
        if self.glob_include.endswith("**"):
            return self.allowed(path, ignore_ext=True)

        path_slash = path.count("/")
        glob_slash = self.glob_include.count("/")
        if path_slash > glob_slash:
            return False

        # We need to remove the extra slashes from the glob include other wise it would keep the part after the last slash
        # which wouldn't match to the dir path
        slash_to_remove_from_glob = (glob_slash - path_slash) + 1

        # glob_include = self.glob_include.rsplit("/", 1)[0]
        glob_include = self.glob_include

        for _ in range(slash_to_remove_from_glob):
            glob_include = glob_include.rsplit("/", 1)[0]

        logger.debug(f"Checking dir to inclusion: {path}")
        if not pathlib.PurePath(path).globmatch(glob_include, flags=pathlib.GLOBSTAR):
            return False
        logger.debug(f"{path} matched include ")
        if self.exclude:
            for exclude_path in self.exclude:
                if pathlib.PurePath(path.rstrip("/")).globmatch(
                    exclude_path.rstrip("/"), flags=pathlib.GLOBSTAR
                ):
                    return False

        file_name_pattern = self.include.rsplit("/", 1)[1]
        table_name, _ = self.extract_table_name_and_path(
            os.path.join(path, file_name_pattern)
        )
        if not self.tables_filter_pattern.allowed(table_name):
            return False
        logger.debug(f"{path} is passed table name check")

        return True

    @classmethod
    def get_parsable_include(cls, include: str) -> str:
        parsable_include = include
        if parsable_include.endswith("/{table}/**"):
            # Remove the last two characters to make it parsable if it ends with {table}/** which marks autodetect partition
            parsable_include = parsable_include[:-2]
        else:
            # Replace all * with {folder[i]} to make it parsable
            for i in range(parsable_include.count("*")):
                parsable_include = parsable_include.replace("*", f"{{folder[{i}]}}", 1)
        return parsable_include

    def get_named_vars(self, path: str) -> Union[None, parse.Result, parse.Match]:
        if self.include.endswith("{table}/**"):
            # If we have a partial path with ** at the end, we need to truncate the path to parse correctly
            # parse needs to have exact number of folders to parse correctly and in case of ** we don't know the number of folders
            # so we need to truncate the path to the last folder before ** to parse and get named vars correctly
            splits = len(self.include[: self.include.find("{table}/")].split("/"))
            path = "/".join(path.split("/", splits)[:-1]) + "/"

        return self.compiled_include.parse(path)

    def get_folder_named_vars(
        self, path: str
    ) -> Union[None, parse.Result, parse.Match]:
        return self.compiled_folder_include.parse(path)

    @pydantic.root_validator()
    def validate_no_double_stars(cls, values: Dict) -> Dict:
        if "include" not in values:
            return values

        if (
            values.get("include")
            and "**" in values["include"]
            and not values.get("allow_double_stars")
        ):
            raise ValueError("path_spec.include cannot contain '**'")
        return values

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
        if v is not None and v not in SUPPORTED_FILE_TYPES:
            raise ValueError(
                f"default extension {v} not in supported default file extension. Please specify one from {SUPPORTED_FILE_TYPES}"
            )
        return v

    @pydantic.validator("sample_files", always=True)
    def turn_off_sampling_for_non_s3(cls, v, values):
        is_s3 = is_s3_uri(values.get("include") or "")
        is_gcs = is_gcs_uri(values.get("include") or "")
        is_abs = is_abs_uri(values.get("include") or "")
        if not is_s3 and not is_gcs and not is_abs:
            # Sampling only makes sense on s3 and gcs currently
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
    def is_gcs(self):
        return is_gcs_uri(self.include)

    @cached_property
    def is_abs(self):
        return is_abs_uri(self.include)

    @cached_property
    def compiled_include(self):
        parsable_include = PathSpec.get_parsable_include(self.include)
        logger.debug(f"parsable_include: {parsable_include}")
        compiled_include = parse.compile(parsable_include)
        logger.debug(f"Setting compiled_include: {compiled_include}")
        return compiled_include

    @cached_property
    def compiled_folder_include(self):
        parsable_folder_include = PathSpec.get_parsable_include(self.include).rsplit(
            "/", 1
        )[0]
        logger.debug(f"parsable_folder_include: {parsable_folder_include}")
        compiled_folder_include = parse.compile(parsable_folder_include)
        logger.debug(f"Setting compiled_folder_include: {compiled_folder_include}")
        return compiled_folder_include

    @cached_property
    def extract_variable_names(self):
        # Regular expression to find all substrings enclosed in {}
        pattern = r"\{(.*?)\}"
        # Find all matches
        matches = re.findall(pattern, self.include.split("{table}/")[1])
        return matches

    def get_partition_from_path(self, path: str) -> Optional[List[Tuple[str, str]]]:
        # Automatic partition detection supports four methods to get partiton keys and values from path:
        # Let's say we have the following path => year=2024/month=10/day=11 for this example you can specify the following path spec expressions:
        #   1. User can specify partition_key and partition_value in the path like => {partition_key[0]}={partition_value[0]}/{partition_key[1]}={partition_value[1]}/{partition_key[2]}={partition_value[2]}
        #   2. User can specify only partition key and the partition key will be used as partition name like => year={year}/month={month}/day={day}
        #   3. You omit specifying anything and it will detect partiton key and value based on the equal signs (this only works if partitioned are specified in the key=value way.
        #   4. if the path is in the form of /value1/value2/value3 we infer it from the path and assign partition_0, partition_1, partition_2 etc

        partition_keys: List[Tuple[str, str]] = []
        if self.include.find("{table}/"):
            named_vars = self.get_named_vars(path)
            if named_vars:
                # If user has specified partition_key and partition_value in the path_spec then we use it to get partition keys
                if "partition_key" in named_vars.named and (
                    (
                        "partition_value" in named_vars.named
                        and len(named_vars.named["partition_key"])
                        == len(named_vars.named["partition_value"])
                    )
                    or (
                        "partition" in named_vars.named
                        and len(named_vars.named["partition_key"])
                        == len(named_vars.named["partition"])
                    )
                ):
                    for key in named_vars.named["partition_key"]:
                        # We need to support both partition_value and partition as both were in our docs
                        if (
                            "partition_value" in named_vars
                            and key in named_vars.named["partition_value"]
                        ) or (
                            "partition" in named_vars
                            and key in named_vars.named["partition"]
                        ):
                            partition_keys.append(
                                (
                                    named_vars.named["partition_key"][key],
                                    (
                                        named_vars.named["partition_value"][key]
                                        if "partition_value" in named_vars.named
                                        else named_vars.named["partition"][key]
                                    ),
                                )
                            )
                    return partition_keys
                else:
                    # TODO: Fix this message
                    logger.debug(
                        "Partition key or value not found. Fallbacking another mechanism to get partition keys"
                    )

                partition_vars = self.extract_variable_names
                if partition_vars:
                    for partition_key in partition_vars:
                        pkey: str = partition_key
                        index: Optional[int] = None
                        # We need to recreate the key and index from the partition_key
                        if partition_key.find("[") != -1:
                            pkey, index = partition_key.strip("]").split("[")
                        else:
                            pkey = partition_key
                            index = None

                        if pkey in named_vars.named:
                            if index and index in named_vars.named[pkey]:
                                partition_keys.append(
                                    (f"{pkey}_{index}", named_vars.named[pkey][index])
                                )
                            else:
                                partition_keys.append(
                                    (partition_key, named_vars.named[partition_key])
                                )
                    return partition_keys

            # If user did not specified partition_key and partition_value in the path_spec then we use the default mechanism to get partition keys
            if len(self.include.split("{table}/")) == 2:
                num_slash = len(self.include.split("{table}/")[0].split("/"))
                partition = path.split("/", num_slash)[num_slash]
            else:
                return None
            if partition.endswith("/"):
                partition = partition[:-1]

            # If partition is in the form of key=value we infer it from the path
            if partition.find("=") != -1:
                partition = partition.rsplit("/", 1)[0]
                for partition_key in partition.split("/"):
                    if partition_key.find("=") != -1:
                        partition_keys.append(tuple(partition_key.split("=")))
            else:
                partition_split = partition.rsplit("/", 1)
                if len(partition_split) == 1:
                    return None
                partition = partition_split[0]
                # If partition is in the form of /value1/value2/value3 we infer it from the path and assign partition_0, partition_1, partition_2 etc
                for num, partition_value in enumerate(partition.split("/")):
                    partition_keys.append((f"partition_{num}", partition_value))
            return partition_keys

        return None

    @cached_property
    def glob_include(self):
        glob_include = re.sub(r"\{[^}]+\}", "*", self.include)
        logger.debug(f"Setting _glob_include: {glob_include}")
        return glob_include

    @pydantic.root_validator(skip_on_failure=True)
    def validate_path_spec(cls, values: Dict) -> Dict[str, Any]:
        # validate that main fields are populated
        required_fields = ["include", "file_types", "default_extension"]
        for f in required_fields:
            if f not in values:
                logger.debug(
                    f"Failed to validate because {f} wasn't populated correctly"
                )
                return values

        if values["include"] and values["autodetect_partitions"]:
            include = values["include"]
            if include.endswith("/"):
                include = include[:-1]

            if include.endswith("{table}"):
                values["include"] = include + "/**"

        include_ext = os.path.splitext(values["include"])[1].strip(".")
        if not include_ext:
            include_ext = (
                "*"  # if no extension is provided, we assume all files are allowed
            )

        if (
            include_ext not in values["file_types"]
            and include_ext not in ["*", ""]
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

    # TODO: Add support to sort partition folders by the defined partition key pattern. This is not implemented yet.
    def extract_datetime_partition(
        self, path: str, is_folder: bool = False
    ) -> Optional[datetime.datetime]:
        if self.sort_key is None:
            return None

        if not self.sort_key.date_format and self.sort_key.type not in [
            SortKeyType.DATETIME,
            SortKeyType.DATE,
        ]:
            return None

        if is_folder:
            parsed_vars = self.get_folder_named_vars(path)
        else:
            parsed_vars = self.get_named_vars(path)
        if parsed_vars is None:
            return None

        partition_format = self.sort_key.key
        datetime_format = self.sort_key.date_format
        if datetime_format is None:
            return None

        for var_key in parsed_vars.named:
            var = parsed_vars.named[var_key]
            if isinstance(var, dict):
                for key in var:
                    template_key = var_key + f"[{key}]"
                    partition_format = partition_format.replace(
                        f"{{{template_key}}}", var[key]
                    )
            else:
                partition_format.replace(f"{{{var_key}}}", var)
        return datetime.datetime.strptime(partition_format, datetime_format).replace(
            tzinfo=datetime.timezone.utc
        )

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
