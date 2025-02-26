from datahub.ingestion.source.superset import SupersetConfig


def test_default_values():
    config = SupersetConfig.parse_obj({})

    assert config.connect_uri == "http://localhost:8088"
    assert config.display_uri == "http://localhost:8088"
    assert config.provider == "db"
    assert config.env == "PROD"
    assert config.username is None
    assert config.password is None


def test_set_display_uri():
    display_uri = "some_host:1234"

    config = SupersetConfig.parse_obj({"display_uri": display_uri})

    assert config.connect_uri == "http://localhost:8088"
    assert config.display_uri == display_uri


def test_database_pattern():
    db_pattern = "test_database1"

    config = SupersetConfig.parse_obj(
        {
            "database_pattern": {
                "allow": [".*"],
                "deny": [db_pattern],
                "ignoreCase": False,
            }
        }
    )

    assert config.database_pattern.allow == [".*"]
    assert config.database_pattern.deny == [db_pattern]
    assert config.database_pattern.ignoreCase is False

    assert config.database_pattern.allowed("test_db2") is True
    assert config.database_pattern.allowed(db_pattern) is False
