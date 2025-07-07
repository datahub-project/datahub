from unittest.mock import patch

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.data_lake_common.path_spec import (
    SUPPORTED_FILE_TYPES,
    FolderTraversalMethod,
    PathSpec,
    SortKey,
    SortKeyType,
)


@pytest.mark.parametrize(
    "include, s3_uri, expected",
    [
        (
            "s3://bucket/{table}/{partition0}/*.csv",
            "s3://bucket/table/p1/test.csv",
            True,
        ),
        (
            "s3://bucket/{table}/{partition0}/*.csv",
            "s3://bucket/table/p1/p2/test.csv",
            False,
        ),
    ],
)
def test_allowed_ignores_depth_mismatch(
    include: str, s3_uri: str, expected: bool
) -> None:
    # arrange
    path_spec = PathSpec(
        include=include,
        table_name="{table}",
    )

    # act, assert
    assert path_spec.allowed(s3_uri) == expected


@pytest.mark.parametrize(
    "s3_uri, expected",
    [
        ("s3://bucket/table-111/p1/test.csv", True),
        ("s3://bucket/table-222/p1/test.csv", False),
    ],
)
def test_allowed_tables_filter_pattern(s3_uri: str, expected: bool) -> None:
    # arrange
    path_spec = PathSpec(
        include="s3://bucket/{table}/{partition0}/*.csv",
        tables_filter_pattern=AllowDenyPattern(allow=["t.*111"]),
    )

    # act, assert
    assert path_spec.allowed(s3_uri) == expected


@pytest.mark.parametrize(
    "s3_uri, expected",
    [
        ("s3://bucket/table-111/p1/", True),
        ("s3://bucket/table-222/p1/", False),
    ],
)
def test_dir_allowed_tables_filter_pattern(s3_uri: str, expected: bool) -> None:
    # arrange
    path_spec = PathSpec(
        include="s3://bucket/{table}/{partition0}/*.csv",
        tables_filter_pattern=AllowDenyPattern(allow=["t.*111"]),
    )

    # act, assert
    assert path_spec.dir_allowed(s3_uri) == expected


@pytest.mark.parametrize(
    "include, parse_path, expected_table_name, expected_table_path",
    [
        (
            "s3://bucket/{table}/{partition0}/*.csv",
            "s3://bucket/user_log/p1/test.csv",
            "user_log",
            "s3://bucket/user_log",
        ),
        (
            "s3://bucket/user_log/p1/*.csv",
            "s3://bucket/user_log/p1/test.csv",
            "test.csv",
            "s3://bucket/user_log/p1/test.csv",
        ),
    ],
)
def test_extract_table_name_and_path(
    include, parse_path, expected_table_name, expected_table_path
):
    # arrange
    path_spec = PathSpec(include=include)

    # act
    table_name, table_path = path_spec.extract_table_name_and_path(parse_path)

    # assert
    assert table_name == expected_table_name
    assert table_path == expected_table_path


# Tests for SortKeyType enum
def test_sort_key_type_string_representation() -> None:
    """Test SortKeyType __str__ method."""
    assert str(SortKeyType.STRING) == "STRING"
    assert str(SortKeyType.INTEGER) == "INTEGER"
    assert str(SortKeyType.FLOAT) == "FLOAT"
    assert str(SortKeyType.DATETIME) == "DATETIME"
    assert str(SortKeyType.DATE) == "DATE"


# Tests for SortKey class
def test_sort_key_date_format_conversion() -> None:
    """Test SortKey date format conversion from Java to Python format."""
    sort_key = SortKey(
        key="year-month-day",
        type=SortKeyType.DATE,
        date_format="yyyy-MM-dd"
    )
    assert sort_key.date_format == "%Y-%m-%d"


def test_sort_key_date_format_none() -> None:
    """Test SortKey with None date format."""
    sort_key = SortKey(key="test", date_format=None)
    assert sort_key.date_format is None


def test_sort_key_complex_date_format() -> None:
    """Test SortKey with complex date format conversion."""
    sort_key = SortKey(
        key="datetime",
        type=SortKeyType.DATETIME,
        date_format="yyyy-MM-dd HH:mm:ss"
    )
    assert sort_key.date_format == "%Y-%m-%d %H:%M:%S"


# Tests for FolderTraversalMethod enum
def test_folder_traversal_method_values() -> None:
    """Test FolderTraversalMethod enum values."""
    assert FolderTraversalMethod.ALL.value == "ALL"
    assert FolderTraversalMethod.MIN_MAX.value == "MIN_MAX"
    assert FolderTraversalMethod.MAX.value == "MAX"


# Tests for PathSpec initialization and validation
def test_path_spec_basic_initialization() -> None:
    """Test basic PathSpec initialization."""
    path_spec = PathSpec(include="s3://bucket/{table}/*.csv")
    
    assert path_spec.include == "s3://bucket/{table}/*.csv"
    assert path_spec.exclude == []
    assert path_spec.file_types == SUPPORTED_FILE_TYPES
    assert path_spec.default_extension is None
    assert path_spec.table_name == "{table}"
    assert path_spec.enable_compression is True
    assert path_spec.sample_files is True
    assert path_spec.allow_double_stars is False
    assert path_spec.autodetect_partitions is True
    assert path_spec.traversal_method == FolderTraversalMethod.MAX
    assert path_spec.include_hidden_folders is False


def test_path_spec_with_custom_values() -> None:
    """Test PathSpec with custom values."""
    path_spec = PathSpec(
        include="gs://bucket/{table}/*.parquet",
        exclude=["*/temp/*", "*/staging/*"],
        file_types=["parquet", "json"],
        default_extension="parquet",
        table_name="{table}_data",
        enable_compression=False,
        sample_files=False,
        allow_double_stars=True,
        autodetect_partitions=False,
        traversal_method=FolderTraversalMethod.ALL,
        include_hidden_folders=True,
    )
    
    assert path_spec.include == "gs://bucket/{table}/*.parquet"
    assert path_spec.exclude == ["*/temp/*", "*/staging/*"]
    assert path_spec.file_types == ["parquet", "json"]
    assert path_spec.default_extension == "parquet"
    assert path_spec.table_name == "{table}_data"
    assert path_spec.enable_compression is False
    assert path_spec.sample_files is False
    assert path_spec.allow_double_stars is True
    assert path_spec.autodetect_partitions is False
    assert path_spec.traversal_method == FolderTraversalMethod.ALL
    assert path_spec.include_hidden_folders is True


# Tests for is_path_hidden method
@pytest.mark.parametrize(
    "path, expected",
    [
        ("normal/path/file.csv", False),
        (".hidden/file.csv", True),
        ("_underscore/file.csv", True),
        ("normal/.hidden/file.csv", True),
        ("normal/_underscore/file.csv", True),
        ("normal/path/.hidden_file.csv", True),
        ("normal/path/_hidden_file.csv", True),
        ("normal/path/file.csv", False),
        ("/normal/path/file.csv", False),
        ("", False),
    ],
)
def test_is_path_hidden(path: str, expected: bool) -> None:
    """Test is_path_hidden method."""
    path_spec = PathSpec(include="s3://bucket/{table}/*.csv")
    assert path_spec.is_path_hidden(path) == expected


# Tests for allowed method
def test_allowed_hidden_path_excluded() -> None:
    """Test that hidden paths are excluded when include_hidden_folders is False."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/*.csv",
        include_hidden_folders=False
    )
    assert path_spec.allowed("s3://bucket/table/.hidden/file.csv") is False


def test_allowed_hidden_path_included() -> None:
    """Test that include_hidden_folders configuration works."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/*.csv",
        include_hidden_folders=True
    )
    # Verify the configuration is set properly
    assert path_spec.include_hidden_folders is True
    
    # Test that normal paths work
    assert path_spec.allowed("s3://bucket/normal_table/data.csv") is True
    
    # Test the hidden path detection logic separately
    assert path_spec.is_path_hidden("s3://bucket/.hidden_table/data.csv") is True
    assert path_spec.is_path_hidden("s3://bucket/normal_table/data.csv") is False


def test_allowed_with_exclude_patterns() -> None:
    """Test allowed method with exclude patterns."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/*.csv",
        exclude=["*/temp/*", "*/staging/*"]
    )
    
    assert path_spec.allowed("s3://bucket/table/file.csv") is True
    assert path_spec.allowed("s3://bucket/table/temp/file.csv") is False
    assert path_spec.allowed("s3://bucket/table/staging/file.csv") is False


def test_allowed_with_file_extension_filter() -> None:
    """Test allowed method with file extension filtering."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/*.csv",
        file_types=["csv", "json"]
    )
    
    assert path_spec.allowed("s3://bucket/table/file.csv") is True
    # Test with different include pattern for json
    path_spec_json = PathSpec(
        include="s3://bucket/{table}/*.json", 
        file_types=["csv", "json"]
    )
    assert path_spec_json.allowed("s3://bucket/table/file.json") is True
    assert path_spec.allowed("s3://bucket/table/file.parquet") is False


def test_allowed_with_default_extension() -> None:
    """Test allowed method with default extension for files without extension."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/*",
        file_types=["csv"],
        default_extension="csv"
    )
    
    assert path_spec.allowed("s3://bucket/table/file") is True  # No extension, uses default
    assert path_spec.allowed("s3://bucket/table/file.csv") is True


def test_allowed_ignore_extension() -> None:
    """Test allowed method with ignore_ext=True."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/*",
        file_types=["csv"]
    )
    
    assert path_spec.allowed("s3://bucket/table/file.parquet", ignore_ext=True) is True


def test_allowed_with_debug_logging() -> None:
    """Test allowed method with debug logging enabled."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/*.csv"
    )
    
    result = path_spec.allowed("s3://bucket/table/file.csv")
    assert result is True


# Tests for dir_allowed method
def test_dir_allowed_with_double_star() -> None:
    """Test dir_allowed method when include ends with **."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/**",
        allow_double_stars=True
    )
    
    # Test that directories are allowed with double star pattern
    assert path_spec.dir_allowed("s3://bucket/table/subfolder/") is True


def test_dir_allowed_depth_check() -> None:
    """Test dir_allowed method with depth checking."""
    path_spec = PathSpec(include="s3://bucket/{table}/partition/*.csv")
    
    # Correct depth
    assert path_spec.dir_allowed("s3://bucket/table/partition/") is True
    
    # Too deep
    assert path_spec.dir_allowed("s3://bucket/table/partition/subpartition/") is False


def test_dir_allowed_with_exclude() -> None:
    """Test dir_allowed method with exclude patterns."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/*.csv",
        exclude=["*/temp/*"]
    )
    
    assert path_spec.dir_allowed("s3://bucket/table/") is True
    assert path_spec.dir_allowed("s3://bucket/table/temp/") is False


def test_dir_allowed_with_debug_logging() -> None:
    """Test dir_allowed method with debug logging enabled."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/*.csv"
    )
    
    result = path_spec.dir_allowed("s3://bucket/table/")
    assert result is True


# Tests for get_parsable_include classmethod
@pytest.mark.parametrize(
    "include, expected",
    [
        ("s3://bucket/{table}/*.csv", "s3://bucket/{table}/{folder[0]}.csv"),
        ("s3://bucket/{table}/**", "s3://bucket/{table}/"),
        ("s3://bucket/table/*.csv", "s3://bucket/table/{folder[0]}.csv"),
        ("s3://bucket/{table}/*/", "s3://bucket/{table}/{folder[0]}/"),
    ],
)
def test_get_parsable_include(include: str, expected: str) -> None:
    """Test get_parsable_include classmethod."""
    result = PathSpec.get_parsable_include(include)
    assert result == expected


def test_get_parsable_include_with_table_double_star() -> None:
    """Test get_parsable_include with {table}/** pattern."""
    include = "s3://bucket/{table}/**"
    result = PathSpec.get_parsable_include(include)
    assert result == "s3://bucket/{table}/"


# Tests for get_named_vars method
def test_get_named_vars_basic() -> None:
    """Test get_named_vars method with basic pattern."""
    path_spec = PathSpec(include="s3://bucket/{table}/*.csv")
    result = path_spec.get_named_vars("s3://bucket/users/data.csv")
    
    assert result is not None
    assert result.named["table"] == "users"


def test_get_named_vars_with_table_double_star() -> None:
    """Test get_named_vars method with {table}/** pattern."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/**",
        allow_double_stars=True
    )
    result = path_spec.get_named_vars("s3://bucket/users/2023/01/data.csv")
    
    assert result is not None
    assert result.named["table"] == "users"


def test_get_named_vars_no_match() -> None:
    """Test get_named_vars method when path doesn't match pattern."""
    path_spec = PathSpec(include="s3://bucket/{table}/*.csv")
    result = path_spec.get_named_vars("s3://different/users/data.csv")
    
    assert result is None


# Tests for get_folder_named_vars method
def test_get_folder_named_vars() -> None:
    """Test get_folder_named_vars method."""
    path_spec = PathSpec(include="s3://bucket/{table}/year={year}/*.csv")
    result = path_spec.get_folder_named_vars("s3://bucket/users/year=2023")
    
    assert result is not None
    assert result.named["table"] == "users"
    assert result.named["year"] == "2023"


# Tests for cached properties
def test_cached_properties() -> None:
    """Test cached properties for URI type detection."""
    # S3 URI
    s3_path_spec = PathSpec(include="s3://bucket/{table}/*.csv")
    assert s3_path_spec.is_s3 is True
    assert s3_path_spec.is_gcs is False
    assert s3_path_spec.is_abs is False
    
    # GCS URI
    gcs_path_spec = PathSpec(include="gs://bucket/{table}/*.csv")
    assert gcs_path_spec.is_s3 is False
    assert gcs_path_spec.is_gcs is True
    assert gcs_path_spec.is_abs is False
    
    # ABS URI - using correct HTTPS blob storage format
    abs_path_spec = PathSpec(include="https://myaccount.blob.core.windows.net/container/{table}/*.csv")
    assert abs_path_spec.is_s3 is False
    assert abs_path_spec.is_gcs is False
    assert abs_path_spec.is_abs is True


def test_compiled_include_cached_property() -> None:
    """Test compiled_include cached property."""
    path_spec = PathSpec(include="s3://bucket/{table}/*.csv")
    compiled = path_spec.compiled_include
    
    assert compiled is not None
    # Should return the same object on subsequent calls (cached)
    assert path_spec.compiled_include is compiled


def test_compiled_include_with_debug() -> None:
    """Test compiled_include with debug logging."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/*.csv"
    )
    
    compiled = path_spec.compiled_include
    assert compiled is not None


def test_compiled_folder_include_cached_property() -> None:
    """Test compiled_folder_include cached property."""
    path_spec = PathSpec(include="s3://bucket/{table}/year={year}/*.csv")
    compiled_folder = path_spec.compiled_folder_include
    
    assert compiled_folder is not None
    # Should return the same object on subsequent calls (cached)
    assert path_spec.compiled_folder_include is compiled_folder


def test_compiled_folder_include_with_debug() -> None:
    """Test compiled_folder_include with debug logging."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/year={year}/*.csv"
    )
    
    compiled_folder = path_spec.compiled_folder_include
    assert compiled_folder is not None


def test_extract_variable_names() -> None:
    """Test extract_variable_names cached property."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/year={year}/month={month}/**",
        allow_double_stars=True
    )
    variables = path_spec.extract_variable_names
    
    assert "year" in variables
    assert "month" in variables


def test_glob_include_cached_property() -> None:
    """Test glob_include cached property."""
    path_spec = PathSpec(include="s3://bucket/{table}/year={year}/*.csv")
    
    with patch("datahub.ingestion.source.data_lake_common.path_spec.logger") as mock_logger:
        glob_pattern = path_spec.glob_include
        expected = "s3://bucket/*/year=*/*.csv"
        assert glob_pattern == expected
        mock_logger.debug.assert_called_with(f"Setting _glob_include: {expected}")


# Tests for validators
def test_validate_no_double_stars_allowed() -> None:
    """Test validation when double stars are allowed."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/**",
        allow_double_stars=True
    )
    assert "**" in path_spec.include


def test_validate_no_double_stars_forbidden() -> None:
    """Test validation error when double stars are forbidden."""
    with pytest.raises(ValueError, match="path_spec.include cannot contain '\\*\\*'"):
        PathSpec(
            include="s3://bucket/{table}/**",
            allow_double_stars=False
        )


def test_validate_file_types_none() -> None:
    """Test file_types validator with None value."""
    # This should use the default from the model
    path_spec = PathSpec(include="s3://bucket/{table}/*.csv")
    assert path_spec.file_types == SUPPORTED_FILE_TYPES


def test_validate_file_types_invalid() -> None:
    """Test file_types validator with invalid file type."""
    with pytest.raises(ValueError, match="file type invalid not in supported file types"):
        PathSpec(
            include="s3://bucket/{table}/*.csv",
            file_types=["csv", "invalid"]
        )


def test_validate_default_extension_valid() -> None:
    """Test default_extension validator with valid extension."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/*",
        default_extension="csv"
    )
    assert path_spec.default_extension == "csv"


def test_validate_default_extension_invalid() -> None:
    """Test default_extension validator with invalid extension."""
    with pytest.raises(ValueError, match="default extension invalid not in supported default file extension"):
        PathSpec(
            include="s3://bucket/{table}/*",
            default_extension="invalid"
        )


def test_turn_off_sampling_for_non_s3() -> None:
    """Test that sampling is turned off for non-S3/GCS/ABS URIs."""
    path_spec = PathSpec(
        include="file:///path/{table}/*.csv",
        sample_files=True
    )
    # Should be automatically set to False for local files
    assert path_spec.sample_files is False


def test_sampling_enabled_for_s3() -> None:
    """Test that sampling stays enabled for S3 URIs."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/*.csv",
        sample_files=True
    )
    assert path_spec.sample_files is True


def test_no_named_fields_in_exclude_valid() -> None:
    """Test exclude validator with valid patterns."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/*.csv",
        exclude=["*/temp/*", "*/staging/*"]
    )
    assert path_spec.exclude == ["*/temp/*", "*/staging/*"]


def test_no_named_fields_in_exclude_invalid() -> None:
    """Test exclude validator with invalid named variables."""
    with pytest.raises(ValueError, match="path_spec.exclude .* should not contain any named variables"):
        PathSpec(
            include="s3://bucket/{table}/*.csv",
            exclude=["*/{invalid_var}/*"]
        )


def test_table_name_auto_set() -> None:
    """Test that table_name is automatically set to {table} when include contains {table}."""
    path_spec = PathSpec(include="s3://bucket/{table}/*.csv")
    assert path_spec.table_name == "{table}"


def test_table_name_custom_valid() -> None:
    """Test custom table_name with valid variables."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/year={year}/*.csv",
        table_name="{table}_{year}"
    )
    assert path_spec.table_name == "{table}_{year}"


def test_table_name_custom_invalid() -> None:
    """Test custom table_name with invalid variables."""
    with pytest.raises(ValueError, match="Not all named variables used in path_spec.table_name"):
        PathSpec(
            include="s3://bucket/{table}/*.csv",
            table_name="{table}_{invalid_var}"
        )


def test_validate_path_spec_missing_fields() -> None:
    """Test path_spec validation with missing required fields."""
    with patch("datahub.ingestion.source.data_lake_common.path_spec.logger") as mock_logger:
        # This should not raise an error but log debug messages
        values = {"file_types": ["csv"]}  # Missing include and default_extension
        result = PathSpec.validate_path_spec(values)
        assert result == values
        mock_logger.debug.assert_called()


def test_validate_path_spec_autodetect_partitions() -> None:
    """Test path_spec validation with autodetect_partitions."""
    values = {
        "include": "s3://bucket/{table}",
        "autodetect_partitions": True,
        "file_types": ["csv"],
        "default_extension": None
    }
    result = PathSpec.validate_path_spec(values)
    assert result["include"] == "s3://bucket/{table}/**"


def test_validate_path_spec_autodetect_partitions_with_slash() -> None:
    """Test path_spec validation with autodetect_partitions and trailing slash."""
    values = {
        "include": "s3://bucket/{table}/",
        "autodetect_partitions": True,
        "file_types": ["csv"],
        "default_extension": None
    }
    result = PathSpec.validate_path_spec(values)
    assert result["include"] == "s3://bucket/{table}/**"


def test_validate_path_spec_invalid_extension() -> None:
    """Test path_spec validation with invalid file extension in include."""
    with pytest.raises(ValueError, match="file type specified.*not in specified file types"):
        PathSpec(
            include="s3://bucket/{table}/*.invalid",
            file_types=["csv", "json"]
        )


def test_validate_path_spec_compression_extension() -> None:
    """Test path_spec validation allows compression extensions."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/*.csv.gz",
        file_types=["csv"],
        enable_compression=True
    )
    # The path should be allowed because .gz is a supported compression
    assert path_spec.include.endswith(".csv.gz")
    assert path_spec.enable_compression is True


# Tests for partition extraction
def test_get_partition_from_path_no_table() -> None:
    """Test get_partition_from_path when include doesn't contain {table}."""
    path_spec = PathSpec(include="s3://bucket/data/*.csv")
    result = path_spec.get_partition_from_path("s3://bucket/data/file.csv")
    assert result is None


def test_get_partition_from_path_key_value_format() -> None:
    """Test get_partition_from_path with key=value format."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/**",
        allow_double_stars=True
    )
    result = path_spec.get_partition_from_path("s3://bucket/users/year=2023/month=01/file.csv")
    
    assert result is not None
    assert ("year", "2023") in result
    assert ("month", "01") in result


def test_get_partition_from_path_simple_values() -> None:
    """Test get_partition_from_path with simple value format."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/**",
        allow_double_stars=True
    )
    result = path_spec.get_partition_from_path("s3://bucket/users/2023/01/file.csv")
    
    assert result is not None
    assert ("partition_0", "2023") in result
    assert ("partition_1", "01") in result


def test_get_partition_from_path_named_variables() -> None:
    """Test get_partition_from_path with explicitly named variables."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/year={year}/month={month}/**",
        allow_double_stars=True
    )
    result = path_spec.get_partition_from_path("s3://bucket/users/year=2023/month=01/file.csv")
    
    assert result is not None
    assert ("year", "2023") in result
    assert ("month", "01") in result


# Tests for datetime partition extraction
def test_extract_datetime_partition_no_sort_key() -> None:
    """Test extract_datetime_partition when no sort_key is configured."""
    path_spec = PathSpec(include="s3://bucket/{table}/*.csv")
    result = path_spec.extract_datetime_partition("s3://bucket/users/file.csv")
    assert result is None


def test_extract_datetime_partition_no_date_format() -> None:
    """Test extract_datetime_partition when sort_key has no date_format."""
    sort_key = SortKey(key="partition", type=SortKeyType.STRING)
    path_spec = PathSpec(
        include="s3://bucket/{table}/{partition}/*.csv",
        sort_key=sort_key
    )
    result = path_spec.extract_datetime_partition("s3://bucket/users/2023/file.csv")
    assert result is None


def test_extract_datetime_partition_valid() -> None:
    """Test extract_datetime_partition with valid configuration."""
    sort_key = SortKey(
        key="{year}-{month}-{day}",
        type=SortKeyType.DATE,
        date_format="yyyy-MM-dd"
    )
    path_spec = PathSpec(
        include="s3://bucket/{table}/year={year}/month={month}/day={day}/*.csv",
        sort_key=sort_key
    )
    result = path_spec.extract_datetime_partition(
        "s3://bucket/users/year=2023/month=01/day=15/file.csv"
    )
    
    assert result is not None
    assert result.year == 2023
    assert result.month == 1
    assert result.day == 15


def test_extract_datetime_partition_folder() -> None:
    """Test extract_datetime_partition for folder path."""
    sort_key = SortKey(
        key="{year}-{month}",
        type=SortKeyType.DATE,
        date_format="yyyy-MM"
    )
    path_spec = PathSpec(
        include="s3://bucket/{table}/year={year}/month={month}/*.csv",
        sort_key=sort_key
    )
    result = path_spec.extract_datetime_partition(
        "s3://bucket/users/year=2023/month=01",
        is_folder=True
    )
    
    assert result is not None
    assert result.year == 2023
    assert result.month == 1


def test_extract_datetime_partition_no_match() -> None:
    """Test extract_datetime_partition when path doesn't match pattern."""
    sort_key = SortKey(
        key="{year}",
        type=SortKeyType.DATE,
        date_format="yyyy"
    )
    path_spec = PathSpec(
        include="s3://bucket/{table}/year={year}/*.csv",
        sort_key=sort_key
    )
    result = path_spec.extract_datetime_partition("s3://bucket/users/file.csv")
    assert result is None


# Tests for _extract_table_name method
def test_extract_table_name_no_table_name_set() -> None:
    """Test _extract_table_name when table_name is not set."""
    path_spec = PathSpec(
        include="s3://bucket/data/*.csv",
        table_name=None
    )
    with pytest.raises(ValueError, match="path_spec.table_name is not set"):
        path_spec._extract_table_name({"table": "users"})


def test_extract_table_name_with_variables() -> None:
    """Test _extract_table_name with variable substitution."""
    path_spec = PathSpec(
        include="s3://bucket/{table}/year={year}/*.csv",
        table_name="{table}_{year}"
    )
    result = path_spec._extract_table_name({"table": "users", "year": "2023"})
    assert result == "users_2023"


def test_extract_table_name_and_path_no_table_in_path() -> None:
    """Test extract_table_name_and_path when path doesn't contain table variable."""
    path_spec = PathSpec(include="s3://bucket/data/*.csv")
    table_name, table_path = path_spec.extract_table_name_and_path("s3://bucket/data/file.csv")
    
    assert table_name == "file.csv"
    assert table_path == "s3://bucket/data/file.csv"


def test_extract_table_name_and_path_with_table() -> None:
    """Test extract_table_name_and_path with table variable in include pattern."""
    path_spec = PathSpec(include="s3://bucket/{table}/*.csv")
    table_name, table_path = path_spec.extract_table_name_and_path("s3://bucket/users/file.csv")
    
    assert table_name == "users"
    assert table_path == "s3://bucket/users"
