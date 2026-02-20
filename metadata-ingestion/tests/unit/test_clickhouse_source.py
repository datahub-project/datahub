import pytest

from datahub.ingestion.source.sql.clickhouse import (
    ClickHouseConfig,
    _extract_sharding_key,
    _split_distributed_args,
    _strip_wrapping_quotes,
)


class TestShardingKeyExtraction:
    """Tests for ClickHouse sharding key extraction from Distributed engine."""

    def test_extract_sharding_key_with_explicit_key(self):
        """Explicit sharding key (4th argument) should be extracted."""
        engine_full = "Distributed('cluster', 'db', 'table', 'user_id')"
        assert _extract_sharding_key(engine_full) == "user_id"

    def test_extract_sharding_key_with_function(self):
        """Sharding key with function call should be extracted."""
        engine_full = "Distributed('cluster', 'db', 'table', murmurHash3_64(user_id))"
        assert _extract_sharding_key(engine_full) == "murmurHash3_64(user_id)"

    def test_extract_sharding_key_implicit_rand(self):
        """3-arg Distributed (implicit sharding) should return rand()."""
        engine_full = "Distributed('cluster', 'db', 'table')"
        assert _extract_sharding_key(engine_full) == "rand()"

    def test_extract_sharding_key_non_distributed(self):
        """Non-Distributed engines should return None."""
        assert _extract_sharding_key("MergeTree()") is None
        assert _extract_sharding_key("ReplicatedMergeTree('/path', 'replica')") is None

    def test_extract_sharding_key_empty_input(self):
        """Empty/None input should return None."""
        assert _extract_sharding_key(None) is None
        assert _extract_sharding_key("") is None

    def test_extract_sharding_key_with_nested_parens(self):
        """Sharding key with nested function calls should work."""
        engine_full = "Distributed('cluster', 'db', 'table', cityHash64(concat(a, b)))"
        assert _extract_sharding_key(engine_full) == "cityHash64(concat(a, b))"

    def test_extract_sharding_key_with_escaped_quotes(self):
        """Escaped quotes in engine_full should be handled."""
        engine_full = "Distributed(\\'cluster\\', \\'db\\', \\'table\\', \\'key\\')"
        assert _extract_sharding_key(engine_full) == "key"

    def test_extract_sharding_key_custom_default(self):
        """Custom default sharding key should be returned for 3-arg form."""
        engine_full = "Distributed('cluster', 'db', 'table')"
        assert (
            _extract_sharding_key(engine_full, default_sharding_key="sipHash64()")
            == "sipHash64()"
        )


class TestHelperFunctions:
    """Tests for helper functions used in sharding key extraction."""

    def test_strip_wrapping_quotes_single(self):
        assert _strip_wrapping_quotes("'value'") == "value"

    def test_strip_wrapping_quotes_double(self):
        assert _strip_wrapping_quotes('"value"') == "value"

    def test_strip_wrapping_quotes_none(self):
        assert _strip_wrapping_quotes("value") == "value"

    def test_strip_wrapping_quotes_empty(self):
        assert _strip_wrapping_quotes("") == ""

    def test_split_distributed_args_simple(self):
        args = _split_distributed_args("'cluster', 'db', 'table'")
        assert args == ["'cluster'", "'db'", "'table'"]

    def test_split_distributed_args_with_function(self):
        args = _split_distributed_args("'cluster', 'db', 'table', murmurHash(id)")
        assert args == ["'cluster'", "'db'", "'table'", "murmurHash(id)"]

    def test_split_distributed_args_nested_parens(self):
        args = _split_distributed_args("'cluster', 'db', 'table', f(g(x, y), z)")
        assert args == ["'cluster'", "'db'", "'table'", "f(g(x, y), z)"]


def test_clickhouse_uri_https():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "database": "db",
            "uri_opts": {"protocol": "https"},
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "clickhouse://user:password@host:1111/db?protocol=https"
    )


def test_clickhouse_uri_native():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "scheme": "clickhouse+native",
        }
    )
    assert config.get_sql_alchemy_url() == "clickhouse+native://user:password@host:1111"


def test_clickhouse_uri_native_secure():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "database": "db",
            "scheme": "clickhouse+native",
            "uri_opts": {"secure": True},
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "clickhouse+native://user:password@host:1111/db?secure=True"
    )


def test_clickhouse_uri_default_password():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "host_port": "host:1111",
            "database": "db",
            "scheme": "clickhouse+native",
        }
    )
    assert config.get_sql_alchemy_url() == "clickhouse+native://user@host:1111/db"


def test_clickhouse_uri_native_secure_backward_compatibility():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "database": "db",
            "scheme": "clickhouse+native",
            "secure": True,
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "clickhouse+native://user:password@host:1111/db?secure=True"
    )


def test_clickhouse_uri_https_backward_compatibility():
    config = ClickHouseConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:1111",
            "database": "db",
            "protocol": "https",
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "clickhouse://user:password@host:1111/db?protocol=https"
    )


# Query log extraction tests


def test_query_log_deny_usernames_validation_valid():
    """Test that valid usernames are accepted."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "query_log_deny_usernames": [
                "system",
                "default",
                "admin-user",
                "test_user123",
            ],
        }
    )
    assert set(config.query_log_deny_usernames) == {
        "system",
        "default",
        "admin-user",
        "test_user123",
    }


def test_query_log_deny_usernames_validation_invalid():
    """Test that invalid usernames are rejected (SQL injection prevention)."""

    # SQL injection attempt
    with pytest.raises(ValueError, match="Invalid username"):
        ClickHouseConfig.model_validate(
            {
                "host_port": "localhost:8123",
                "query_log_deny_usernames": ["system'; DROP TABLE users;--"],
            }
        )

    # Username with quotes
    with pytest.raises(ValueError, match="Invalid username"):
        ClickHouseConfig.model_validate(
            {"host_port": "localhost:8123", "query_log_deny_usernames": ["user'name"]}
        )


def test_is_temp_table():
    """Test that is_temp_table correctly identifies temporary tables."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
        }
    )

    # Tables that should match temporary patterns
    assert config.is_temp_table("_temp_table")
    assert config.is_temp_table("db.tmp_staging")
    assert config.is_temp_table("db.temp_data")
    assert config.is_temp_table("db._inner_mv")

    # Tables that should NOT match
    assert not config.is_temp_table("normal_table")
    assert not config.is_temp_table("db.regular_table")
    assert not config.is_temp_table("my_db.production_table")


def test_is_temp_table_custom_patterns():
    """Test is_temp_table with custom patterns."""
    config = ClickHouseConfig.model_validate(
        {
            "host_port": "localhost:8123",
            "temporary_tables_pattern": [
                r".*\.staging_.*",  # Any table with staging_ prefix
                r"^test_.*",  # Tables starting with test_
            ],
        }
    )

    assert config.is_temp_table("db.staging_data")
    assert config.is_temp_table("test_table")
    # Default patterns no longer match with custom patterns
    assert not config.is_temp_table("_temp_table")
