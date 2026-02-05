"""Tests for JDBC parser factories (source and sink)."""

import pytest

from datahub.ingestion.source.kafka_connect.common import ConnectorManifest
from datahub.ingestion.source.kafka_connect.sink_connectors import (
    JdbcSinkParserFactory,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    JdbcParserFactory,
)


class TestJdbcParserFactorySource:
    """Test JdbcParserFactory for source connectors."""

    def test_create_parser_with_connection_url(self) -> None:
        """Test creating parser from connection.url configuration."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "connection.url": "jdbc:postgresql://localhost:5432/testdb",
                "topic.prefix": "my-prefix-",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()
        parser = factory.create_parser(manifest)

        assert parser.source_platform == "postgres"
        assert parser.database_name == "testdb"
        assert parser.topic_prefix == "my-prefix-"
        assert "postgresql://localhost:5432/testdb" in parser.db_connection_url

    def test_create_parser_with_fields(self) -> None:
        """Test creating parser from individual field configuration."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connector.class": "PostgresCdcSource",
                "database.hostname": "db.example.com",
                "database.port": "5432",
                "database.dbname": "mydb",
                "database.server.name": "my-server",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()
        parser = factory.create_parser(manifest)

        assert parser.source_platform == "postgres"
        assert parser.database_name == "mydb"
        assert parser.topic_prefix == "my-server"
        assert parser.db_connection_url == "postgresql://db.example.com:5432/mydb"

    def test_create_url_parser_postgres(self) -> None:
        """Test URL parser for PostgreSQL."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connection.url": "jdbc:postgresql://localhost:5432/testdb",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()
        parser = factory._create_url_parser(manifest)

        assert parser.source_platform == "postgres"
        assert parser.database_name == "testdb"

    def test_create_url_parser_mysql(self) -> None:
        """Test URL parser for MySQL."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connection.url": "jdbc:mysql://localhost:3306/mydb",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()
        parser = factory._create_url_parser(manifest)

        assert parser.source_platform == "mysql"
        assert parser.database_name == "mydb"

    def test_create_url_parser_missing_database(self) -> None:
        """Test URL parser raises error when database name is missing."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connection.url": "jdbc:postgresql://localhost:5432/",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()

        with pytest.raises(ValueError, match="Missing database name"):
            factory._create_url_parser(manifest)

    def test_create_url_parser_no_database(self) -> None:
        """Test URL parser raises error when database name is completely missing."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connection.url": "jdbc:postgresql://localhost:5432",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()

        with pytest.raises(ValueError, match="Missing database name"):
            factory._create_url_parser(manifest)

    def test_create_url_parser_with_topic_prefix(self) -> None:
        """Test URL parser extracts topic.prefix."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connection.url": "jdbc:postgresql://localhost:5432/testdb",
                "topic.prefix": "my-prefix-",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()
        parser = factory._create_url_parser(manifest)

        assert parser.topic_prefix == "my-prefix-"

    def test_create_fields_parser_postgres_cdc(self) -> None:
        """Test fields parser for PostgresCdcSource."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connector.class": "PostgresCdcSource",
                "database.hostname": "localhost",
                "database.port": "5432",
                "database.dbname": "testdb",
                "database.server.name": "my-server",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()
        parser = factory._create_fields_parser(manifest)

        assert parser.source_platform == "postgres"
        assert parser.database_name == "testdb"
        assert parser.topic_prefix == "my-server"
        assert parser.db_connection_url == "postgresql://localhost:5432/testdb"

    def test_create_fields_parser_mysql_cdc(self) -> None:
        """Test fields parser for MySqlCdcSource."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connector.class": "MySqlCdcSource",
                "database.hostname": "mysql.example.com",
                "database.port": "3306",
                "database.dbname": "mydb",
                "database.server.name": "mysql-server",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()
        parser = factory._create_fields_parser(manifest)

        assert parser.source_platform == "mysql"
        assert parser.database_name == "mydb"
        assert parser.topic_prefix == "mysql-server"
        assert parser.db_connection_url == "mysql://mysql.example.com:3306/mydb"

    def test_create_fields_parser_postgres_cdc_v2_uses_topic_prefix(self) -> None:
        """Test PostgresCdcSourceV2 uses topic.prefix instead of database.server.name."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connector.class": "PostgresCdcSourceV2",
                "database.hostname": "localhost",
                "database.port": "5432",
                "database.dbname": "testdb",
                "topic.prefix": "my-postgres-prefix",
                "database.server.name": "should-be-ignored",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()
        parser = factory._create_fields_parser(manifest)

        assert parser.source_platform == "postgres"
        assert parser.database_name == "testdb"
        assert parser.topic_prefix == "my-postgres-prefix"
        assert parser.db_connection_url == "postgresql://localhost:5432/testdb"

    def test_create_fields_parser_mysql_cdc_v2_uses_topic_prefix(self) -> None:
        """Test MySqlCdcSourceV2 uses topic.prefix instead of database.server.name."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connector.class": "MySqlCdcSourceV2",
                "database.hostname": "mysql.example.com",
                "database.port": "3306",
                "database.dbname": "mydb",
                "topic.prefix": "my-mysql-prefix",
                "database.server.name": "should-be-ignored",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()
        parser = factory._create_fields_parser(manifest)

        assert parser.source_platform == "mysql"
        assert parser.database_name == "mydb"
        assert parser.topic_prefix == "my-mysql-prefix"
        assert parser.db_connection_url == "mysql://mysql.example.com:3306/mydb"

    def test_create_fields_parser_postgres_cdc_v2_only_topic_prefix(self) -> None:
        """Test PostgresCdcSourceV2 works with only topic.prefix (no database.server.name)."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connector.class": "PostgresCdcSourceV2",
                "database.hostname": "localhost",
                "database.port": "5432",
                "database.dbname": "testdb",
                "topic.prefix": "postgres",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()
        parser = factory._create_fields_parser(manifest)

        assert parser.source_platform == "postgres"
        assert parser.topic_prefix == "postgres"

    def test_create_fields_parser_mysql_cdc_v2_only_topic_prefix(self) -> None:
        """Test MySqlCdcSourceV2 works with only topic.prefix (no database.server.name)."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connector.class": "MySqlCdcSourceV2",
                "database.hostname": "mysql.example.com",
                "database.port": "3306",
                "database.dbname": "mydb",
                "topic.prefix": "mysql",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()
        parser = factory._create_fields_parser(manifest)

        assert parser.source_platform == "mysql"
        assert parser.topic_prefix == "mysql"

    def test_create_fields_parser_postgres_lowercase(self) -> None:
        """Test fields parser recognizes lowercase postgres."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": "localhost",
                "database.port": "5432",
                "database.dbname": "testdb",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()
        parser = factory._create_fields_parser(manifest)

        assert parser.source_platform == "postgres"

    def test_create_fields_parser_missing_hostname(self) -> None:
        """Test fields parser raises error when hostname is missing."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connector.class": "PostgresCdcSource",
                "database.port": "5432",
                "database.dbname": "testdb",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()

        with pytest.raises(ValueError, match="Missing required Cloud connector config"):
            factory._create_fields_parser(manifest)

    def test_create_fields_parser_missing_port(self) -> None:
        """Test fields parser raises error when port is missing."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connector.class": "PostgresCdcSource",
                "database.hostname": "localhost",
                "database.dbname": "testdb",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()

        with pytest.raises(ValueError, match="Missing required Cloud connector config"):
            factory._create_fields_parser(manifest)

    def test_create_fields_parser_missing_database_name(self) -> None:
        """Test fields parser raises error when database name is missing."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connector.class": "PostgresCdcSource",
                "database.hostname": "localhost",
                "database.port": "5432",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()

        with pytest.raises(ValueError, match="Missing required Cloud connector config"):
            factory._create_fields_parser(manifest)

    def test_create_fields_parser_unknown_connector(self) -> None:
        """Test fields parser handles unknown connector class."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connector.class": "com.example.UnknownConnector",
                "database.hostname": "localhost",
                "database.port": "5432",
                "database.dbname": "testdb",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()
        parser = factory._create_fields_parser(manifest)

        assert parser.source_platform == "unknown"

    def test_build_parser_with_query(self) -> None:
        """Test parser includes query configuration."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connection.url": "jdbc:postgresql://localhost:5432/testdb",
                "query": "SELECT * FROM users WHERE active = true",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()
        parser = factory.create_parser(manifest)

        assert parser.query == "SELECT * FROM users WHERE active = true"

    def test_build_parser_with_transforms(self) -> None:
        """Test parser includes transform configuration."""
        manifest = ConnectorManifest(
            name="test-connector",
            type="source",
            config={
                "connection.url": "jdbc:postgresql://localhost:5432/testdb",
                "transforms": "Router,Other",
                "transforms.Router.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.Router.regex": ".*",
                "transforms.Router.replacement": "new-topic",
                "transforms.Other.type": "org.apache.kafka.connect.transforms.InsertField",
                "transforms.Other.field": "timestamp",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcParserFactory()
        parser = factory.create_parser(manifest)

        assert len(parser.transforms) == 2
        assert parser.transforms[0]["name"] == "Router"
        assert (
            parser.transforms[0]["type"]
            == "org.apache.kafka.connect.transforms.RegexRouter"
        )
        assert parser.transforms[1]["name"] == "Other"


class TestJdbcSinkParserFactory:
    """Test JdbcSinkParserFactory for sink connectors."""

    def test_create_parser_with_connection_url(self) -> None:
        """Test creating sink parser from connection.url configuration."""
        manifest = ConnectorManifest(
            name="test-sink",
            type="sink",
            config={
                "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                "connection.url": "jdbc:postgresql://localhost:5432/targetdb",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcSinkParserFactory()
        parser = factory.create_parser(manifest, "postgres")

        assert parser.target_platform == "postgres"
        assert parser.database_name == "targetdb"
        assert "postgresql://localhost:5432/targetdb" in parser.db_connection_url

    def test_create_parser_with_fields(self) -> None:
        """Test creating sink parser from individual field configuration."""
        manifest = ConnectorManifest(
            name="test-sink",
            type="sink",
            config={
                "connector.class": "PostgresSink",
                "connection.host": "db.example.com",
                "connection.port": "5432",
                "db.name": "targetdb",
                "db.schema": "public",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcSinkParserFactory()
        parser = factory.create_parser(manifest, "postgres")

        assert parser.target_platform == "postgres"
        assert parser.database_name == "targetdb"
        assert parser.schema_name == "public"
        assert parser.db_connection_url == "postgres://db.example.com:5432/targetdb"

    def test_create_parser_from_url_postgres(self) -> None:
        """Test URL parser for PostgreSQL sink."""
        manifest = ConnectorManifest(
            name="test-sink",
            type="sink",
            config={
                "connection.url": "jdbc:postgresql://localhost:5432/targetdb",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcSinkParserFactory()
        parser = factory._create_parser_from_url(
            manifest, "postgres", "jdbc:postgresql://localhost:5432/targetdb"
        )

        assert parser.target_platform == "postgres"
        assert parser.database_name == "targetdb"

    def test_create_parser_from_url_mysql(self) -> None:
        """Test URL parser for MySQL sink."""
        manifest = ConnectorManifest(
            name="test-sink",
            type="sink",
            config={
                "connection.url": "jdbc:mysql://localhost:3306/targetdb",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcSinkParserFactory()
        parser = factory._create_parser_from_url(
            manifest, "mysql", "jdbc:mysql://localhost:3306/targetdb"
        )

        assert parser.target_platform == "mysql"
        assert parser.database_name == "targetdb"

    def test_create_parser_from_url_missing_database(self) -> None:
        """Test URL parser raises error when database name is missing."""
        manifest = ConnectorManifest(
            name="test-sink",
            type="sink",
            config={
                "connection.url": "jdbc:postgresql://localhost:5432/",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcSinkParserFactory()

        with pytest.raises(ValueError, match="Missing database name"):
            factory._create_parser_from_url(
                manifest, "postgres", "jdbc:postgresql://localhost:5432/"
            )

    def test_create_parser_from_url_with_schema_in_query(self) -> None:
        """Test URL parser extracts schema from query parameters."""
        manifest = ConnectorManifest(
            name="test-sink",
            type="sink",
            config={
                "connection.url": "jdbc:postgresql://localhost:5432/targetdb?currentSchema=myschema",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcSinkParserFactory()
        parser = factory._create_parser_from_url(
            manifest,
            "postgres",
            "jdbc:postgresql://localhost:5432/targetdb?currentSchema=myschema",
        )

        assert parser.schema_name == "myschema"

    def test_create_parser_from_url_with_table_name_format(self) -> None:
        """Test URL parser extracts table name format."""
        manifest = ConnectorManifest(
            name="test-sink",
            type="sink",
            config={
                "connection.url": "jdbc:postgresql://localhost:5432/targetdb",
                "table.name.format": "kafka_${topic}",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcSinkParserFactory()
        parser = factory._create_parser_from_url(
            manifest, "postgres", "jdbc:postgresql://localhost:5432/targetdb"
        )

        assert parser.table_name_format == "kafka_${topic}"

    def test_create_parser_from_fields_postgres(self) -> None:
        """Test fields parser for PostgreSQL sink."""
        manifest = ConnectorManifest(
            name="test-sink",
            type="sink",
            config={
                "connection.host": "localhost",
                "connection.port": "5432",
                "db.name": "targetdb",
                "db.schema": "myschema",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcSinkParserFactory()
        parser = factory._create_parser_from_fields(manifest, "postgres")

        assert parser.target_platform == "postgres"
        assert parser.database_name == "targetdb"
        assert parser.schema_name == "myschema"
        assert parser.db_connection_url == "postgres://localhost:5432/targetdb"

    def test_create_parser_from_fields_mysql(self) -> None:
        """Test fields parser for MySQL sink."""
        manifest = ConnectorManifest(
            name="test-sink",
            type="sink",
            config={
                "connection.host": "mysql.example.com",
                "connection.port": "3306",
                "db.name": "targetdb",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcSinkParserFactory()
        parser = factory._create_parser_from_fields(manifest, "mysql")

        assert parser.target_platform == "mysql"
        assert parser.database_name == "targetdb"

    def test_create_parser_from_fields_postgres_default_schema(self) -> None:
        """Test fields parser uses default 'public' schema for Postgres."""
        manifest = ConnectorManifest(
            name="test-sink",
            type="sink",
            config={
                "connection.host": "localhost",
                "connection.port": "5432",
                "db.name": "targetdb",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcSinkParserFactory()
        parser = factory._create_parser_from_fields(manifest, "postgres")

        assert parser.schema_name == "public"

    def test_create_parser_from_fields_schema_name_field(self) -> None:
        """Test fields parser extracts schema from schema.name field."""
        manifest = ConnectorManifest(
            name="test-sink",
            type="sink",
            config={
                "connection.host": "localhost",
                "connection.port": "5432",
                "db.name": "targetdb",
                "schema.name": "myschema",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcSinkParserFactory()
        parser = factory._create_parser_from_fields(manifest, "postgres")

        assert parser.schema_name == "myschema"

    def test_create_parser_from_fields_missing_host(self) -> None:
        """Test fields parser raises error when host is missing."""
        manifest = ConnectorManifest(
            name="test-sink",
            type="sink",
            config={
                "connection.port": "5432",
                "db.name": "targetdb",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcSinkParserFactory()

        with pytest.raises(ValueError, match="Missing 'connection.host'"):
            factory._create_parser_from_fields(manifest, "postgres")

    def test_create_parser_from_fields_missing_db_name(self) -> None:
        """Test fields parser raises error when db.name is missing."""
        manifest = ConnectorManifest(
            name="test-sink",
            type="sink",
            config={
                "connection.host": "localhost",
                "connection.port": "5432",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcSinkParserFactory()

        with pytest.raises(ValueError, match="Missing 'db.name'"):
            factory._create_parser_from_fields(manifest, "postgres")

    def test_create_parser_from_fields_default_port(self) -> None:
        """Test fields parser uses default port when not specified."""
        manifest = ConnectorManifest(
            name="test-sink",
            type="sink",
            config={
                "connection.host": "localhost",
                "db.name": "targetdb",
            },
            tasks=[],
            topic_names=[],
        )

        factory = JdbcSinkParserFactory()
        parser = factory._create_parser_from_fields(manifest, "postgres")

        # Default port is 5432
        assert "localhost:5432" in parser.db_connection_url

    def test_extract_schema_from_url_current_schema(self) -> None:
        """Test extracting schema from URL currentSchema parameter."""
        from sqlalchemy.engine.url import make_url

        factory = JdbcSinkParserFactory()
        url_instance = make_url(
            "postgresql://localhost:5432/testdb?currentSchema=myschema"
        )

        schema = factory._extract_schema_from_url(url_instance, "postgres", {})

        assert schema == "myschema"

    def test_extract_schema_from_url_schema_parameter(self) -> None:
        """Test extracting schema from URL schema parameter."""
        from sqlalchemy.engine.url import make_url

        factory = JdbcSinkParserFactory()
        url_instance = make_url("postgresql://localhost:5432/testdb?schema=myschema")

        schema = factory._extract_schema_from_url(url_instance, "postgres", {})

        assert schema == "myschema"

    def test_extract_schema_from_url_config_fallback(self) -> None:
        """Test extracting schema from config when not in URL."""
        from sqlalchemy.engine.url import make_url

        factory = JdbcSinkParserFactory()
        url_instance = make_url("postgresql://localhost:5432/testdb")
        config = {"schema.name": "myschema"}

        schema = factory._extract_schema_from_url(url_instance, "postgres", config)

        assert schema == "myschema"

    def test_extract_schema_from_url_postgres_default(self) -> None:
        """Test extracting schema uses default for Postgres."""
        from sqlalchemy.engine.url import make_url

        factory = JdbcSinkParserFactory()
        url_instance = make_url("postgresql://localhost:5432/testdb")

        schema = factory._extract_schema_from_url(url_instance, "postgres", {})

        assert schema == "public"

    def test_extract_schema_from_url_mysql_no_default(self) -> None:
        """Test extracting schema returns None for MySQL (no schema concept)."""
        from sqlalchemy.engine.url import make_url

        factory = JdbcSinkParserFactory()
        url_instance = make_url("mysql://localhost:3306/testdb")

        schema = factory._extract_schema_from_url(url_instance, "mysql", {})

        # MySQL doesn't have schemas (database == schema)
        assert schema is None
