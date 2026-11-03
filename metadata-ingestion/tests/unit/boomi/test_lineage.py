from pathlib import Path
from typing import Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.boomi.client import BoomiApiError
from datahub.ingestion.source.boomi.component_parser import (
    operation_direction,
    parse_connection_component,
    parse_database_profile_component,
    parse_operation_component,
    parse_process_component,
)
from datahub.ingestion.source.boomi.config import BoomiSourceConfig
from datahub.ingestion.source.boomi.constants import OperationDirection
from datahub.ingestion.source.boomi.lineage import (
    BoomiLineageResolver,
    connection_location,
    qualify_table_name,
)
from datahub.ingestion.source.boomi.models import (
    ConnectionComponent,
    ConnectorActionShape,
    JdbcLocation,
    ProcessComponentInfo,
)
from datahub.ingestion.source.boomi.report import BoomiSourceReport

DB_V2 = "officialboomi-X3979C-dbv2da-prod"
SNOWFLAKE = "officialboomi-X3979C-snowfl-prod"


def _component(type_: str, sub_type: str, body: str) -> str:
    # The envelope of a live GET Component/{id} response.
    sub_type_attribute = f'subType="{sub_type}" ' if sub_type else ""
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xmlns:bns="http://api.platform.boomi.com/" '
        'folderFullPath="Test Account/Shared" componentId="c1" version="1" '
        f'name="Component" type="{type_}" {sub_type_attribute}'
        'createdDate="2026-11-02T19:14:48Z" createdBy="svc@example.com" '
        'modifiedDate="2026-11-02T19:14:48Z" modifiedBy="svc@example.com" '
        'deleted="false" currentVersion="true" folderName="Shared" '
        'folderId="Rjo4OTQzNDEy" branchName="main" branchId="Qjo2MDk0Mzg">'
        f"<bns:encryptedValues/><bns:object>{body}</bns:object></bns:Component>"
    )


def _legacy_connection(driver: str = "sqlserver", extra: str = "") -> str:
    return _component(
        "connector-settings",
        "database",
        '<DatabaseConnectionSettings xmlns="" additional="" className="" '
        f'dbname="my_db" driverId="{driver}" host="db.example.com" '
        f'isPoolEnabled="false" password="0000" port="1433" username="svc" {extra}>'
        '<WriteOptions sqlFilePath="" writeSQLToFile="false"/>'
        "</DatabaseConnectionSettings>",
    )


def _v2_connection(url: str, schema: str = "") -> str:
    return _component(
        "connector-settings",
        DB_V2,
        '<GenericConnectionConfig xmlns="">'
        f'<field id="url" type="string" value="{url}"/>'
        '<field id="className" type="string" value=""/>'
        '<field id="username" type="string" value="svc"/>'
        '<field id="password" type="password" value=""/>'
        '<field id="CustomProperties" type="customproperties"><customProperties/></field>'
        f'<field id="schemaName" type="string" value="{schema}"/>'
        '<field id="enablePooling" type="boolean" value="false"/>'
        "</GenericConnectionConfig>",
    )


def _generic_operation(
    sub_type: str,
    operation_type: str,
    object_type_id: str = "",
    query: str = "",
    custom_operation_type: str = "",
) -> str:
    # Database V2 always writes a query field, empty unless the operation
    # runs custom SQL.
    return _component(
        "connector-action",
        sub_type,
        '<Operation xmlns="" returnApplicationErrors="true" trackResponse="false">'
        '<Archiving directory="" enabled="false"/><Configuration>'
        f'<GenericOperationConfig customOperationType="{custom_operation_type}" '
        f'objectTypeId="{object_type_id}" operationType="{operation_type}" '
        'requestProfileType="json" responseProfileType="json">'
        '<field id="GetType" type="string" value="Standard Get"/>'
        '<field id="schemaName" type="string" value=""/>'
        f'<field id="query" type="string" value="{query}"/>'
        '<field id="maxRows" type="integer" value="25"/>'
        "</GenericOperationConfig></Configuration>"
        "<Tracking><TrackedFields/></Tracking><Caching/></Operation>",
    )


SNOWFLAKE_JDBC_URL = (
    "jdbc:snowflake://acct.snowflakecomputing.com/"
    "?db=ANALYTICS&amp;schema=PUBLIC&amp;warehouse=WH"
)


def _generic_connection(sub_type: str, fields: Dict[str, str]) -> str:
    # Field ids are arbitrary on purpose: only the values are inspected.
    body = "".join(
        f'<field id="{field_id}" type="string" value="{value}"/>'
        for field_id, value in fields.items()
    )
    return _component(
        "connector-settings",
        sub_type,
        f'<GenericConnectionConfig xmlns="">{body}'
        '<field id="password" type="password" value=""/>'
        "</GenericConnectionConfig>",
    )


def _generic_operation_fields(
    sub_type: str, object_type_id: str, fields: Dict[str, str]
) -> str:
    body = "".join(
        f'<field id="{field_id}" type="string" value="{value}"/>'
        for field_id, value in fields.items()
    )
    return _component(
        "connector-action",
        sub_type,
        '<Operation xmlns="" returnApplicationErrors="true" trackResponse="false">'
        "<Configuration>"
        f'<GenericOperationConfig customOperationType="" objectTypeId="{object_type_id}" '
        'operationType="QUERY" requestProfileType="json" responseProfileType="json">'
        f"{body}</GenericOperationConfig></Configuration></Operation>",
    )


def _db_operation(action: str, profile_id: str) -> str:
    # Legacy operations name the profile on a ReadProfile/WriteProfile child.
    profile_element = "ReadProfile" if action.endswith("GetAction") else "WriteProfile"
    return _component(
        "connector-action",
        "database",
        '<Operation xmlns=""><Archiving directory="" enabled="false"/><Configuration>'
        f'<{action} batchCount="0" maxRows="0"><{profile_element} profileId="{profile_id}"/>'
        f"</{action}></Configuration><Tracking><TrackedFields/></Tracking><Caching/>"
        "</Operation>",
    )


def _db_profile(table_name: str, sql: Optional[str] = None) -> str:
    # profile.db components carry no subType.
    sql_element = f"<sql>{sql}</sql>" if sql is not None else ""
    return _component(
        "profile.db",
        "",
        '<DatabaseProfile xmlns="" strict="true" version="2"><ProfileProperties>'
        '<DatabaseGeneralInfo executionType="dbread"/></ProfileProperties>'
        '<DataElements><DBStatement isNode="true" key="2" name="Statement" '
        f'statementType="select" storedProcedure="" tableName="{table_name}">'
        '<DBFields isNode="true" key="3" name="Fields" type="result_set"/>'
        f'<DBParameters isNode="true" key="4" name="Parameters"/>{sql_element}'
        "</DBStatement></DataElements></DatabaseProfile>",
    )


def _salesforce_operation(action: str, object_name: str) -> str:
    return _component(
        "connector-action",
        "salesforce",
        '<Operation xmlns=""><Archiving directory="" enabled="false"/><Configuration>'
        f'<{action} objectAction="query" objectName="{object_name}"><Options>'
        f'<QueryOptions><Fields><SalesforceObject name="{object_name}"><FieldList>'
        '<SalesforceField dataType="character" name="Id"/></FieldList>'
        "</SalesforceObject></Fields><Inputs/></QueryOptions></Options>"
        f"</{action}></Configuration><Tracking><TrackedFields/></Tracking><Caching/>"
        "</Operation>",
    )


SALESFORCE_CONNECTION = _component(
    "connector-settings",
    "salesforce",
    '<SalesforceSettings xmlns="" maxActiveConnections="10" '
    'url="https://login.salesforce.com/services/Soap/u/39.0" version="vx">'
    '<AuthSettings password="0000" user="svc@example.com"/></SalesforceSettings>',
)


def _resolver(
    components: Dict[str, str], **config: object
) -> Tuple[BoomiLineageResolver, MagicMock, BoomiSourceReport]:
    client = MagicMock()

    def get_component_xml(component_id: str) -> str:
        if component_id not in components:
            raise BoomiApiError(404, "not found")
        return components[component_id]

    client.get_component_xml.side_effect = get_component_xml
    report = BoomiSourceReport()
    source_config = BoomiSourceConfig.model_validate(
        {
            "account_id": "acme-AB1CD2",
            "username": "svc@example.com",
            "api_token": "secret-token",
            **config,
        }
    )
    return BoomiLineageResolver(source_config, client, report), client, report


def _shape(
    connection_id: str, operation_id: Optional[str], action_type: str = "Get"
) -> ConnectorActionShape:
    return ConnectorActionShape(
        action_type=action_type,
        connection_id=connection_id,
        operation_id=operation_id,
    )


def _info(*shapes: ConnectorActionShape) -> ProcessComponentInfo:
    return ProcessComponentInfo(connector_actions=list(shapes))


def _connector_shape(
    name: str,
    action_type: str,
    connection_id: str,
    connector_type: str,
    operation_id: str,
) -> str:
    return (
        f'<shape image="connectoraction_icon" name="{name}" shapetype="connectoraction" userlabel="" x="273.0" y="48.0">'
        f'<configuration><connectoraction actionType="{action_type}" allowDynamicCredentials="NONE" '
        f'connectionId="{connection_id}" connectorType="{connector_type}" hideSettings="false" '
        f'operationId="{operation_id}"><parameters/><dynamicProperties/></connectoraction>'
        "</configuration><dragpoints/></shape>"
    )


# Shaped like a live process export; actionType values are mixed case there.
PROCESS_XML = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
    'xmlns:bns="http://api.platform.boomi.com/" folderFullPath="Test Account/Orders" '
    'componentId="p1" version="1" name="Orders" type="process">'
    "<bns:encryptedValues/><bns:description>Orders</bns:description><bns:object>"
    '<process xmlns="" allowSimultaneous="false" workload="general"><shapes>'
    '<shape image="start" name="shape1" shapetype="start" userlabel="">'
    "<configuration><noaction/></configuration><dragpoints/></shape>"
    + _connector_shape("shape2", "Listen", "conn-http", "http", "")
    + _connector_shape("shape3", "CREATE", "conn-1", "database", "op-1")
    + '<shape image="processcall_icon" name="shape4" shapetype="processcall" userlabel="">'
    '<configuration><processcall abort="false" processId="p-child" wait="true">'
    "<parameters/><returnpaths/></processcall></configuration><dragpoints/></shape>"
    '<shape image="map_icon" name="shape5" shapetype="map" userlabel="">'
    "<configuration/><dragpoints/></shape>"
    "</shapes></process></bns:object><bns:processOverrides/></bns:Component>"
)


def test_parse_process_shapes() -> None:
    info = parse_process_component(PROCESS_XML)

    assert info.description == "Orders"
    assert info.connector_actions == [
        ConnectorActionShape(
            connector_type="http", action_type="Listen", connection_id="conn-http"
        ),
        ConnectorActionShape(
            connector_type="database",
            action_type="CREATE",
            connection_id="conn-1",
            operation_id="op-1",
        ),
    ]
    assert info.process_call_ids == ["p-child"]


def test_gui_start_shape_connectoraction_is_not_a_connector_step() -> None:
    # The Boomi UI writes an empty connectoraction into the start shape.
    xml = (
        Path(__file__).parent / "fixtures" / "gui_start_only_process.xml"
    ).read_text()
    info = parse_process_component(xml)

    assert info.connector_actions == []
    assert info.process_call_ids == []


def test_parse_legacy_and_v2_connections() -> None:
    legacy = parse_connection_component(
        _legacy_connection(
            "custom", 'urlFormat="jdbc:mysql://db.example.com:3306/shop"'
        )
    )
    assert legacy == ConnectionComponent(
        sub_type="database",
        driver_id="custom",
        host="db.example.com",
        port="1433",
        dbname="my_db",
        url_format="jdbc:mysql://db.example.com:3306/shop",
    )

    v2 = parse_connection_component(
        _v2_connection("jdbc:postgresql://pg.example.com/app", schema="sales")
    )
    assert v2.sub_type == DB_V2
    assert v2.url == "jdbc:postgresql://pg.example.com/app"
    assert v2.schema_name == "sales"


def test_parse_operations() -> None:
    db_get = parse_operation_component(_db_operation("DatabaseGetAction", "prof-1"))
    assert db_get.direction == OperationDirection.READ
    assert db_get.profile_ids == ["prof-1"]

    db_send = parse_operation_component(_db_operation("DatabaseSendAction", "prof-2"))
    assert db_send.direction == OperationDirection.WRITE
    assert db_send.profile_ids == ["prof-2"]

    attribute_form = parse_operation_component(
        _component(
            "connector-action",
            "database",
            '<Operation xmlns=""><Configuration><DatabaseGetAction '
            'responseProfile="prof-3"/></Configuration></Operation>',
        )
    )
    assert attribute_form.profile_ids == ["prof-3"]

    generic = parse_operation_component(
        _generic_operation(
            DB_V2, "QUERY", object_type_id="dbo.orders", query="SELECT 1"
        )
    )
    assert generic.direction == OperationDirection.READ
    assert generic.object_type_id == "dbo.orders"
    assert generic.query == "SELECT 1"

    salesforce = parse_operation_component(
        _salesforce_operation("SalesforceSendAction", "Account")
    )
    assert salesforce.direction == OperationDirection.WRITE
    assert salesforce.object_name == "Account"


def test_parse_database_profile() -> None:
    profile = parse_database_profile_component(
        _db_profile("dbo.orders", "SELECT id FROM dbo.orders WHERE id &gt; ?")
    )
    assert profile.table_name == "dbo.orders"
    assert profile.statement_type == "select"
    assert profile.sql == "SELECT id FROM dbo.orders WHERE id > ?"
    assert parse_database_profile_component(_db_profile("orders")).sql is None


@pytest.mark.parametrize(
    "action, operation_type, custom_operation_type, expected",
    [
        ("DatabaseGetAction", None, None, OperationDirection.READ),
        ("SalesforceSendAction", None, None, OperationDirection.WRITE),
        (None, "query", None, OperationDirection.READ),
        (None, "GET", None, OperationDirection.READ),
        (None, "UPSERT", None, OperationDirection.WRITE),
        (None, "EXECUTE", "QUERY", OperationDirection.READ),
        (None, "DELETE", None, OperationDirection.WRITE),
        (None, "LISTEN", None, OperationDirection.UNKNOWN),
        (None, None, None, OperationDirection.UNKNOWN),
    ],
)
def test_operation_direction(
    action: Optional[str],
    operation_type: Optional[str],
    custom_operation_type: Optional[str],
    expected: OperationDirection,
) -> None:
    assert operation_direction(action, operation_type, custom_operation_type) == (
        expected
    )


@pytest.mark.parametrize(
    "url, expected",
    [
        # Oracle URNs leave the SID or service out by default.
        (
            "jdbc:oracle:thin:@//ora.example.com:1521/ORCL",
            JdbcLocation(platform="oracle"),
        ),
        ("jdbc:unknowndb://host/db", JdbcLocation()),
    ],
)
def test_v2_connection_location(url: str, expected: JdbcLocation) -> None:
    connection = parse_connection_component(_v2_connection(url))
    assert connection_location(connection) == expected


@pytest.mark.parametrize(
    "driver, expected",
    [
        ("sqlserver", JdbcLocation(platform="mssql", database="my_db")),
        ("postgresql", JdbcLocation(platform="postgres", database="my_db")),
        ("SAP HANA", JdbcLocation(platform="hana", database="my_db")),
        # Oracle URNs leave the database out by default.
        ("oracle", JdbcLocation(platform="oracle")),
        ("sybase", JdbcLocation(database="my_db")),
    ],
)
def test_legacy_driver_map(driver: str, expected: JdbcLocation) -> None:
    connection = parse_connection_component(_legacy_connection(driver))
    assert connection_location(connection) == expected


# A live legacy Database connection with a custom driver: urlFormat is the
# driver's template, and host, port and dbname are separate attributes.
LEGACY_CUSTOM_POSTGRES_CONNECTION = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:bns="http://api.platform.boomi.com/" folderFullPath="Test Account/DataHub Test/Shared" componentId="1a5d3fed-78e7-48d6-83e3-886669a42439" version="1" name="DH Test - RNAcentral Postgres" type="connector-settings" subType="database" createdDate="2026-11-02T19:14:48Z" createdBy="svc@example.com" modifiedDate="2026-11-02T19:14:48Z" modifiedBy="svc@example.com" deleted="false" currentVersion="true" folderName="Shared" folderId="Rjo4OTQzNDEy" branchName="main" branchId="Qjo2MDk0Mzg"><bns:encryptedValues><bns:encryptedValue path="//DatabaseConnectionSettings/@password" isSet="true"/></bns:encryptedValues><bns:description>RNAcentral public read-only PostgreSQL database</bns:description><bns:object><DatabaseConnectionSettings xmlns="" additional="" className="org.postgresql.Driver" dbname="pfmegrnargs" driverId="custom" host="hh-pgsql-public.ebi.ac.uk" isPoolEnabled="false" password="0000" port="5432" urlFormat="jdbc:postgresql://{0}:{1}/{2}{3}" username="reader"><WriteOptions sqlFilePath="" writeSQLToFile="false"/><AdapterPoolInfo exhaustedAction="1" maxActive="-1" maxIdle="-1" maxIdleTime="0" maxWait="0" minIdle="0" numberOfTests="0" testIdle="false" testOnBorrow="false" testOnReturn="false" timeBetweenRuns="0" validationQuery=""/>
    </DatabaseConnectionSettings></bns:object></bns:Component>
"""

# A live Database V2 MySQL connection, which repeats the database as schemaName.
DB_V2_MYSQL_CONNECTION = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:bns="http://api.platform.boomi.com/" folderFullPath="Test Account/DataHub Test/Shared" componentId="64f429a7-f94a-420d-a5d0-03385e9c3fc8" version="1" name="DH Test - Ensembl MySQL" type="connector-settings" subType="officialboomi-X3979C-dbv2da-prod" createdDate="2026-11-02T19:15:17Z" createdBy="svc@example.com" modifiedDate="2026-11-02T19:15:17Z" modifiedBy="svc@example.com" deleted="false" currentVersion="true" folderName="Shared" folderId="Rjo4OTQzNDEy" branchName="main" branchId="Qjo2MDk0Mzg"><bns:encryptedValues/><bns:description>Ensembl public anonymous MySQL server</bns:description><bns:object><GenericConnectionConfig xmlns=""><field id="url" type="string" value="jdbc:mysql://ensembldb.ensembl.org:3306/homo_sapiens_core_116_38"/><field id="className" type="string" value="com.mysql.jdbc.Driver"/><field id="username" type="string" value="anonymous"/><field id="password" type="password" value=""/><field id="CustomProperties" type="customproperties"><customProperties/></field><field id="schemaName" type="string" value="homo_sapiens_core_116_38"/><field id="connectTimeOut" type="integer"/><field id="readTimeOut" type="integer"/><field id="enablePooling" type="boolean" value="false"/>
    </GenericConnectionConfig></bns:object></bns:Component>
"""


def test_legacy_custom_driver_fills_url_template() -> None:
    connection = parse_connection_component(LEGACY_CUSTOM_POSTGRES_CONNECTION)

    assert connection.url_format == "jdbc:postgresql://{0}:{1}/{2}{3}"
    assert connection_location(connection) == JdbcLocation(
        platform="postgres", database="pfmegrnargs"
    )


@pytest.mark.parametrize(
    "url_format, additional, expected",
    [
        (
            "jdbc:sqlserver://{0}:{1};databaseName={2}{3}",
            ";encrypt=true",
            JdbcLocation(platform="mssql", database="my_db"),
        ),
        (
            "jdbc:postgresql://{0}:{1}/{2}{3}",
            "?ssl=true",
            JdbcLocation(platform="postgres", database="my_db"),
        ),
        # An unknown placeholder never becomes the database.
        (
            "jdbc:postgresql://{0}:{1}/{4}",
            "",
            JdbcLocation(platform="postgres"),
        ),
    ],
)
def test_legacy_custom_driver_url_templates(
    url_format: str, additional: str, expected: JdbcLocation
) -> None:
    connection = ConnectionComponent(
        sub_type="database",
        driver_id="custom",
        host="db.example.com",
        port="5432",
        dbname="my_db",
        additional=additional,
        url_format=url_format,
    )
    assert connection_location(connection) == expected


def test_legacy_custom_driver_sql_uses_dbname() -> None:
    resolver, _, _ = _resolver(
        {
            "conn-1": LEGACY_CUSTOM_POSTGRES_CONNECTION,
            "op-1": _db_operation("DatabaseGetAction", "prof-1"),
            "prof-1": _db_profile(
                "", "SELECT id, descr, display_name FROM rnc_database ORDER BY id"
            ),
        }
    )

    lineage = resolver.resolve_process(_info(_shape("conn-1", "op-1")))

    assert lineage.input_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,pfmegrnargs.rnc_database,PROD)"
    ]


def test_db_v2_mysql_schema_name_is_the_database() -> None:
    connection = parse_connection_component(DB_V2_MYSQL_CONNECTION)
    assert connection_location(connection) == JdbcLocation(
        platform="mysql", database="homo_sapiens_core_116_38"
    )

    resolver, _, _ = _resolver(
        {
            "conn-1": DB_V2_MYSQL_CONNECTION,
            "op-1": _generic_operation(
                DB_V2,
                "EXECUTE",
                object_type_id="coord_system",
                query="select * from coord_system",
                custom_operation_type="GET",
            ),
        }
    )

    lineage = resolver.resolve_process(_info(_shape("conn-1", "op-1")))

    assert lineage.input_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:mysql,homo_sapiens_core_116_38.coord_system,PROD)"
    ]


def test_v2_schema_name_overrides_url_schema() -> None:
    connection = parse_connection_component(
        _v2_connection(
            "jdbc:snowflake://acct.snowflakecomputing.com/?db=MY_DB&amp;schema=RAW",
            schema="CURATED",
        )
    )
    assert connection_location(connection) == JdbcLocation(
        platform="snowflake", database="MY_DB", schema_name="CURATED"
    )


@pytest.mark.parametrize(
    "table, platform, expected",
    [
        ("orders", "mssql", "my_db.dbo.orders"),
        ("sales.orders", "mssql", "my_db.sales.orders"),
        ("[other].[sales].[orders]", "mssql", "other.sales.orders"),
        ("orders", "mysql", "my_db.orders"),
        ("shop.orders", "mysql", "shop.orders"),
    ],
)
def test_qualify_table_name(table: str, platform: str, expected: str) -> None:
    assert qualify_table_name(table, platform, "my_db", "dbo") == expected


def test_legacy_database_sql_path() -> None:
    resolver, _, report = _resolver(
        {
            "conn-1": _legacy_connection(),
            "op-1": _db_operation("DatabaseGetAction", "prof-1"),
            "prof-1": _db_profile(
                "dbo.ignored",
                "SELECT o.id FROM dbo.Orders o JOIN sales.customers c ON c.id = o.cid",
            ),
        }
    )

    lineage = resolver.resolve_process(_info(_shape("conn-1", "op-1")))

    assert lineage.input_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,my_db.dbo.orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:mssql,my_db.sales.customers,PROD)",
    ]
    assert lineage.output_datasets == []
    assert report.lineage_edges_resolved == 2


def test_legacy_database_write_uses_sql_target_only() -> None:
    resolver, _, _ = _resolver(
        {
            "conn-1": _legacy_connection(),
            "op-1": _db_operation("DatabaseSendAction", "prof-1"),
            "prof-1": _db_profile(
                "dbo.orders",
                "INSERT INTO dbo.orders (id) SELECT id FROM dbo.staging_orders",
            ),
        }
    )

    lineage = resolver.resolve_process(_info(_shape("conn-1", "op-1", "Send")))

    assert lineage.input_datasets == []
    assert lineage.output_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,my_db.dbo.orders,PROD)"
    ]


def test_legacy_database_table_name_fallback() -> None:
    resolver, _, _ = _resolver(
        {
            "conn-1": _legacy_connection(),
            "op-1": _db_operation("DatabaseGetAction", "prof-1"),
            "prof-1": _db_profile("dbo.Orders"),
        }
    )

    lineage = resolver.resolve_process(_info(_shape("conn-1", "op-1")))

    assert lineage.input_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,my_db.dbo.orders,PROD)"
    ]


def test_db_v2_object_type_and_query() -> None:
    resolver, _, _ = _resolver(
        {
            "conn-1": _v2_connection(
                "jdbc:postgresql://pg.example.com:5432/app", schema="sales"
            ),
            "op-table": _generic_operation(DB_V2, "QUERY", object_type_id="orders"),
            "op-query": _generic_operation(
                DB_V2,
                "EXECUTE",
                query="UPDATE invoices SET paid = ? WHERE id = ?",
                custom_operation_type="UPDATE",
            ),
        }
    )

    lineage = resolver.resolve_process(
        _info(_shape("conn-1", "op-table"), _shape("conn-1", "op-query", "EXECUTE"))
    )

    assert lineage.input_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,app.sales.orders,PROD)"
    ]
    assert lineage.output_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,app.sales.invoices,PROD)"
    ]


def test_salesforce_keeps_object_case() -> None:
    resolver, _, _ = _resolver(
        {
            "conn-sf": SALESFORCE_CONNECTION,
            "op-sf": _salesforce_operation("SalesforceGetAction", "Opportunity"),
        }
    )

    lineage = resolver.resolve_process(_info(_shape("conn-sf", "op-sf")))

    assert lineage.input_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:salesforce,Opportunity,PROD)"
    ]


def test_override_wins_over_parsed_connection() -> None:
    resolver, _, _ = _resolver(
        {
            "conn-1": _legacy_connection(),
            "op-1": _db_operation("DatabaseGetAction", "prof-1"),
            "prof-1": _db_profile("Orders"),
        },
        connection_to_platform={
            "conn-1": {
                "platform": "mssql",
                "platform_instance": "erp",
                "env": "DEV",
                "database": "ERP_DB",
                "default_schema": "Sales",
                "convert_urns_to_lowercase": False,
            }
        },
        connection_type_platform_map={"database": "postgres"},
    )

    lineage = resolver.resolve_process(_info(_shape("conn-1", "op-1")))

    assert lineage.input_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,erp.ERP_DB.Sales.Orders,DEV)"
    ]


def test_generic_mapped_subtype() -> None:
    resolver, _, _ = _resolver(
        {
            "conn-snow": _component(
                "connector-settings", SNOWFLAKE, '<GenericConnectionConfig xmlns=""/>'
            ),
            "op-snow": _generic_operation(
                SNOWFLAKE, "CREATE", object_type_id="ORDER_FACTS"
            ),
        },
        connection_type_platform_map={SNOWFLAKE: "snowflake"},
        connection_to_platform={
            "conn-snow": {
                "platform": "snowflake",
                "database": "ANALYTICS",
                "default_schema": "PUBLIC",
            }
        },
    )

    lineage = resolver.resolve_process(_info(_shape("conn-snow", "op-snow", "Send")))

    assert lineage.output_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.order_facts,PROD)"
    ]


def test_type_map_alone_resolves_platform() -> None:
    resolver, _, _ = _resolver(
        {
            "conn-snow": _component(
                "connector-settings", SNOWFLAKE, '<GenericConnectionConfig xmlns=""/>'
            ),
            "op-snow": _generic_operation(
                SNOWFLAKE, "QUERY", object_type_id="MY_DB.PUBLIC.ORDERS"
            ),
        },
        connection_type_platform_map={SNOWFLAKE: "snowflake"},
    )

    lineage = resolver.resolve_process(_info(_shape("conn-snow", "op-snow")))

    assert lineage.input_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.orders,PROD)"
    ]


def test_parse_generic_jdbc_and_sql_fields() -> None:
    connection = parse_connection_component(
        _generic_connection(
            SNOWFLAKE,
            {
                "account": "acct",
                "connectionString": "JDBC:snowflake://acct.snowflakecomputing.com/",
            },
        )
    )
    assert connection.jdbc_urls == ["JDBC:snowflake://acct.snowflakecomputing.com/"]

    operation = parse_operation_component(
        _generic_operation_fields(
            SNOWFLAKE,
            "ORDERS",
            {
                "queryTimeout": "30",
                "customSql": "select id from orders",
                "query": "",
                "batchSize": "SELECT 1",
            },
        )
    )
    assert operation.sql_statements == ["select id from orders"]


def test_generic_jdbc_url_resolves_platform_and_database() -> None:
    resolver, _, report = _resolver(
        {
            "conn-snow": _generic_connection(
                SNOWFLAKE, {"account": "acct", "connectionString": SNOWFLAKE_JDBC_URL}
            ),
            "op-snow": _generic_operation(SNOWFLAKE, "QUERY", object_type_id="ORDERS"),
            "op-qualified": _generic_operation(
                SNOWFLAKE, "UPSERT", object_type_id="RAW.LANDING.ORDERS"
            ),
        }
    )

    lineage = resolver.resolve_process(
        _info(_shape("conn-snow", "op-snow"), _shape("conn-snow", "op-qualified"))
    )

    assert lineage.input_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.orders,PROD)"
    ]
    assert lineage.output_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.landing.orders,PROD)"
    ]
    assert report.lineage_edges_resolved == 2


def test_generic_operation_sql_field_is_parsed() -> None:
    resolver, _, report = _resolver(
        {
            "conn-snow": _generic_connection(
                SNOWFLAKE, {"connectionString": SNOWFLAKE_JDBC_URL}
            ),
            "op-sql": _generic_operation(
                SNOWFLAKE,
                "QUERY",
                object_type_id="IGNORED",
                query="SELECT o.id FROM orders o JOIN sales.customers c ON c.id = o.customer_id",
            ),
            "op-two-sql": _generic_operation_fields(
                SNOWFLAKE,
                "ORDERS",
                {
                    "query": "SELECT id FROM orders",
                    "customSql": "SELECT id FROM refunds",
                },
            ),
        }
    )

    lineage = resolver.resolve_process(
        _info(_shape("conn-snow", "op-sql"), _shape("conn-snow", "op-two-sql"))
    )

    assert lineage.input_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales.customers,PROD)",
    ]
    assert dict(report.lineage_edges_unresolved) == {"ambiguous_sql": 1}
    assert [w.title for w in report.warnings] == ["Ambiguous SQL in Boomi operation"]


def test_conflicting_jdbc_fields_are_skipped() -> None:
    resolver, client, report = _resolver(
        {
            "conn-two": _generic_connection(
                SNOWFLAKE,
                {
                    "primaryUrl": SNOWFLAKE_JDBC_URL,
                    "replicaUrl": "jdbc:postgresql://pg.example.com/app",
                },
            ),
        }
    )

    lineage = resolver.resolve_process(_info(_shape("conn-two", "op-two")))

    assert lineage.input_datasets == [] and lineage.output_datasets == []
    assert dict(report.lineage_edges_unresolved) == {"ambiguous_jdbc_url": 1}
    assert [w.title for w in report.warnings] == [
        "Ambiguous JDBC URL in Boomi connection"
    ]
    fetched = [c.args[0] for c in client.get_component_xml.call_args_list]
    assert "op-two" not in fetched


def test_non_jdbc_value_stays_unresolved() -> None:
    resolver, _, report = _resolver(
        {
            "conn-ns": _generic_connection(
                "netsuitesdk", {"url": "https://acct.suitetalk.api.example.com"}
            ),
        }
    )

    lineage = resolver.resolve_process(_info(_shape("conn-ns", "op-ns")))

    assert lineage.input_datasets == []
    assert dict(report.lineage_edges_unresolved) == {"unsupported_connector": 1}


def test_type_map_wins_over_disagreeing_jdbc_url() -> None:
    resolver, _, report = _resolver(
        {
            "conn-snow": _generic_connection(
                SNOWFLAKE, {"connectionString": SNOWFLAKE_JDBC_URL}
            ),
            "op-snow": _generic_operation(SNOWFLAKE, "QUERY", object_type_id="ORDERS"),
        },
        connection_type_platform_map={SNOWFLAKE: "databricks"},
    )

    lineage = resolver.resolve_process(_info(_shape("conn-snow", "op-snow")))

    assert lineage.input_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:databricks,orders,PROD)"
    ]
    assert [w.title for w in report.warnings] == [
        "Boomi JDBC URL disagrees with connection_type_platform_map"
    ]


def test_unsupported_and_non_dataset_subtypes_are_counted() -> None:
    netsuite = _component(
        "connector-settings", "netsuitesdk", '<GenericConnectionConfig xmlns=""/>'
    )
    resolver, client, report = _resolver(
        {
            "conn-http": _component("connector-settings", "http", "<HttpSettings/>"),
            "conn-sap": _component("connector-settings", "sapjco", "<SAPJCOSettings/>"),
            "conn-ns": netsuite,
            "conn-ns-2": netsuite,
        }
    )

    lineage = resolver.resolve_process(
        _info(
            _shape("conn-http", "op-http"),
            _shape("conn-sap", "op-sap"),
            _shape("conn-ns", "op-ns"),
            _shape("conn-ns-2", "op-ns-2"),
            _shape("conn-http", None, action_type="Listen"),
        )
    )

    assert lineage.input_datasets == [] and lineage.output_datasets == []
    assert dict(report.lineage_skipped_subtypes) == {"http": 1, "sapjco": 1}
    assert dict(report.lineage_edges_unresolved) == {"unsupported_connector": 2}
    assert report.lineage_listen_shapes_skipped == 1
    assert [w.title for w in report.warnings] == [
        "Unsupported Boomi connector for lineage"
    ]
    # Operations of skipped connectors are never fetched.
    fetched = [c.args[0] for c in client.get_component_xml.call_args_list]
    assert not any(component_id.startswith("op-") for component_id in fetched)


def test_unknown_direction_and_missing_table_are_unresolved() -> None:
    resolver, _, report = _resolver(
        {
            "conn-1": _v2_connection("jdbc:postgresql://pg.example.com/app"),
            "op-unknown": _generic_operation(DB_V2, "LISTEN", object_type_id="orders"),
            "op-empty": _generic_operation(DB_V2, "QUERY"),
        }
    )

    lineage = resolver.resolve_process(
        _info(
            _shape("conn-1", "op-unknown"),
            _shape("conn-1", "op-empty"),
            _shape("conn-1", None),
        )
    )

    assert lineage.input_datasets == []
    assert dict(report.lineage_edges_unresolved) == {
        "unknown_direction": 1,
        "no_table": 1,
        "missing_operation": 1,
    }


def test_sql_parse_failure_is_counted_and_warned_once() -> None:
    resolver, _, report = _resolver(
        {
            "conn-1": _legacy_connection(),
            "op-1": _db_operation("DatabaseGetAction", "prof-1"),
            "op-2": _db_operation("DatabaseGetAction", "prof-2"),
            "prof-1": _db_profile("orders", "SELECT FROM WHERE ((("),
            "prof-2": _db_profile("orders", "SELEC * FRM ((("),
        }
    )

    lineage = resolver.resolve_process(
        _info(_shape("conn-1", "op-1"), _shape("conn-1", "op-2"))
    )

    assert lineage.input_datasets == []
    assert report.lineage_sql_parse_failures == 2
    assert dict(report.lineage_edges_unresolved) == {"sql_parse_failed": 2}
    assert [w.title for w in report.warnings] == ["Failed to parse Boomi SQL"]


def test_components_are_fetched_once_including_failures() -> None:
    resolver, client, report = _resolver(
        {
            "conn-sf": SALESFORCE_CONNECTION,
            "op-sf": _salesforce_operation("SalesforceGetAction", "Account"),
        }
    )
    shapes: List[ConnectorActionShape] = [
        _shape("conn-sf", "op-sf"),
        _shape("conn-sf", "op-sf"),
        _shape("conn-missing", "op-sf"),
        _shape("conn-missing", "op-sf"),
    ]

    resolver.resolve_process(_info(*shapes))
    resolver.resolve_process(_info(*shapes))

    fetched = [c.args[0] for c in client.get_component_xml.call_args_list]
    assert sorted(fetched) == ["conn-missing", "conn-sf", "op-sf"]
    assert list(report.lineage_component_failures) == ["conn-missing"]
    assert dict(report.lineage_edges_unresolved) == {"component_unreadable": 4}


def test_unreadable_profile_marks_lineage_incomplete() -> None:
    resolver, _, report = _resolver(
        {
            "conn-1": _legacy_connection(),
            "op-1": _db_operation("DatabaseGetAction", "prof-missing"),
            "op-2": _db_operation("DatabaseGetAction", "prof-1"),
            "prof-1": _db_profile("dbo.orders"),
        }
    )

    lineage = resolver.resolve_process(
        _info(_shape("conn-1", "op-1"), _shape("conn-1", "op-2"))
    )

    assert lineage.incomplete
    assert dict(report.lineage_edges_unresolved) == {"component_unreadable": 1}


def test_unreadable_connection_marks_lineage_incomplete() -> None:
    resolver, _, _ = _resolver(
        {
            "conn-sf": SALESFORCE_CONNECTION,
            "op-sf": _salesforce_operation("SalesforceGetAction", "Account"),
        }
    )

    complete = resolver.resolve_process(_info(_shape("conn-sf", "op-sf")))
    partial = resolver.resolve_process(
        _info(_shape("conn-sf", "op-sf"), _shape("conn-missing", "op-sf"))
    )

    assert not complete.incomplete
    assert partial.incomplete


def test_connection_mapping_env_is_normalised() -> None:
    resolver, _, _ = _resolver(
        {
            "conn-1": _legacy_connection(),
            "op-1": _db_operation("DatabaseGetAction", "prof-1"),
            "prof-1": _db_profile("dbo.orders"),
        },
        connection_to_platform={"conn-1": {"platform": "mssql", "env": "dev"}},
    )

    lineage = resolver.resolve_process(_info(_shape("conn-1", "op-1")))

    assert lineage.input_datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,my_db.dbo.orders,DEV)"
    ]


def test_connection_mapping_rejects_unknown_env() -> None:
    with pytest.raises(ValidationError):
        _resolver(
            {}, connection_to_platform={"conn-1": {"platform": "mssql", "env": "nope"}}
        )
