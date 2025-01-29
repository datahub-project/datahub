import sgqlc.operation
import sgqlc.types

import datahub.ingestion.source.tableau.codegen_sgqlc.tableau_schema as tableau_schema

_schema = tableau_schema
_schema_root = _schema.tableau_schema

__all__ = ("Operations",)


def query_get_items_database_tables_connection():
    _op = sgqlc.operation.Operation(
        _schema_root.query_type,
        name="GetItems_databaseTablesConnection",
        variables=dict(
            first=sgqlc.types.Arg(_schema.Int), after=sgqlc.types.Arg(_schema.String)
        ),
    )
    _op_database_tables_connection = _op.database_tables_connection(
        first=sgqlc.types.Variable("first"),
        after=sgqlc.types.Variable("after"),
        filter={
            "idWithin": (
                "76e2151b-124f-7ec8-896b-dd107eafca05",
                "a1b165ad-c7c2-282d-94c5-1b8a877936ee",
                "2d3bdb4e-08da-a6da-fecb-a3c10abba357",
                "92b0a3ae-2fc9-1b42-47e0-c17d0f0b615a",
                "63ffbbfe-8c2d-c4f3-7a28-e11f247227c7",
                "06d776e1-9376-bc06-84d5-c7a5c4253bf5",
                "159ed86e-796f-3f13-8f07-3e63c271015e",
                "2f2ccb48-edd5-0e02-4d51-eb3b0819fbf1",
                "2fe499f7-9c5a-81ef-f32d-9553dcc86044",
                "4a34ada9-ed4a-089f-01d0-4a1f230ee2e6",
            )
        },
    )
    _op_database_tables_connection_nodes = _op_database_tables_connection.nodes()
    _op_database_tables_connection_nodes.id()
    _op_database_tables_connection_nodes.is_embedded()
    _op_database_tables_connection_nodes_columns = (
        _op_database_tables_connection_nodes.columns()
    )
    _op_database_tables_connection_nodes_columns.remote_type()
    _op_database_tables_connection_nodes_columns.name()
    _op_database_tables_connection_page_info = (
        _op_database_tables_connection.page_info()
    )
    _op_database_tables_connection_page_info.has_next_page()
    _op_database_tables_connection_page_info.end_cursor()
    return _op


def query_get_items_custom_sqltables_connection():
    _op = sgqlc.operation.Operation(
        _schema_root.query_type,
        name="GetItems_customSQLTablesConnection",
        variables=dict(
            first=sgqlc.types.Arg(_schema.Int), after=sgqlc.types.Arg(_schema.String)
        ),
    )
    _op_custom_sqltables_connection = _op.custom_sqltables_connection(
        first=sgqlc.types.Variable("first"),
        after=sgqlc.types.Variable("after"),
        filter={
            "idWithin": (
                "81335c49-5edc-bbfa-77c9-c4a1cd444501",
                "48c19c5f-4300-07bb-17ee-1fbdf6824ff6",
            )
        },
    )
    _op_custom_sqltables_connection_nodes = _op_custom_sqltables_connection.nodes()
    _op_custom_sqltables_connection_nodes.id()
    _op_custom_sqltables_connection_nodes.name()
    _op_custom_sqltables_connection_nodes.query()
    _op_custom_sqltables_connection_nodes_columns = (
        _op_custom_sqltables_connection_nodes.columns()
    )
    _op_custom_sqltables_connection_nodes_columns.id()
    _op_custom_sqltables_connection_nodes_columns.name()
    _op_custom_sqltables_connection_nodes_columns.remote_type()
    _op_custom_sqltables_connection_nodes_columns.description()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields = (
        _op_custom_sqltables_connection_nodes_columns.referenced_by_fields()
    )
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource = (
        _op_custom_sqltables_connection_nodes_columns_referenced_by_fields.datasource()
    )
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource.__typename__()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource.id()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource.name()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource_upstream_tables = _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource.upstream_tables()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource_upstream_tables.id()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource_upstream_tables.name()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource_upstream_tables_database = _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource_upstream_tables.database()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource_upstream_tables_database.name()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource_upstream_tables_database.id()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource_upstream_tables.schema()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource_upstream_tables.full_name()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource_upstream_tables.connection_type()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource__as__PublishedDatasource = _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource.__as__(
        _schema.PublishedDatasource
    )
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource__as__PublishedDatasource.project_name()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource__as__PublishedDatasource.luid()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource__as__EmbeddedDatasource = _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource.__as__(
        _schema.EmbeddedDatasource
    )
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource__as__EmbeddedDatasource_workbook = _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource__as__EmbeddedDatasource.workbook()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource__as__EmbeddedDatasource_workbook.id()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource__as__EmbeddedDatasource_workbook.name()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource__as__EmbeddedDatasource_workbook.project_name()
    _op_custom_sqltables_connection_nodes_columns_referenced_by_fields_datasource__as__EmbeddedDatasource_workbook.luid()
    _op_custom_sqltables_connection_nodes_tables = (
        _op_custom_sqltables_connection_nodes.tables()
    )
    _op_custom_sqltables_connection_nodes_tables.id()
    _op_custom_sqltables_connection_nodes_tables.name()
    _op_custom_sqltables_connection_nodes_tables_database = (
        _op_custom_sqltables_connection_nodes_tables.database()
    )
    _op_custom_sqltables_connection_nodes_tables_database.name()
    _op_custom_sqltables_connection_nodes_tables_database.id()
    _op_custom_sqltables_connection_nodes_tables.schema()
    _op_custom_sqltables_connection_nodes_tables.full_name()
    _op_custom_sqltables_connection_nodes_tables.connection_type()
    _op_custom_sqltables_connection_nodes_tables.description()
    _op_custom_sqltables_connection_nodes_tables_columns_connection = (
        _op_custom_sqltables_connection_nodes_tables.columns_connection()
    )
    _op_custom_sqltables_connection_nodes_tables_columns_connection.total_count()
    _op_custom_sqltables_connection_nodes.connection_type()
    _op_custom_sqltables_connection_nodes_database = (
        _op_custom_sqltables_connection_nodes.database()
    )
    _op_custom_sqltables_connection_nodes_database.name()
    _op_custom_sqltables_connection_nodes_database.id()
    _op_custom_sqltables_connection_nodes_database.connection_type()
    _op_custom_sqltables_connection_page_info = (
        _op_custom_sqltables_connection.page_info()
    )
    _op_custom_sqltables_connection_page_info.has_next_page()
    _op_custom_sqltables_connection_page_info.end_cursor()
    return _op


def query_get_items_published_datasources_connection():
    _op = sgqlc.operation.Operation(
        _schema_root.query_type,
        name="GetItems_publishedDatasourcesConnection",
        variables=dict(
            first=sgqlc.types.Arg(_schema.Int), after=sgqlc.types.Arg(_schema.String)
        ),
    )
    _op_published_datasources_connection = _op.published_datasources_connection(
        first=sgqlc.types.Variable("first"),
        after=sgqlc.types.Variable("after"),
        filter={
            "idWithin": (
                "35d62018-68e1-6ad4-2cf0-d3ede4ee9a67",
                "87d9d9d8-59a8-adc3-3e06-f75c5b98ec52",
                "ae8b52c9-1481-06fd-fe96-e6d4938f9fcb",
            )
        },
    )
    _op_published_datasources_connection_nodes = (
        _op_published_datasources_connection.nodes()
    )
    _op_published_datasources_connection_nodes.__typename__()
    _op_published_datasources_connection_nodes.id()
    _op_published_datasources_connection_nodes.name()
    _op_published_datasources_connection_nodes.luid()
    _op_published_datasources_connection_nodes.has_extracts()
    _op_published_datasources_connection_nodes.extract_last_refresh_time()
    _op_published_datasources_connection_nodes.extract_last_incremental_update_time()
    _op_published_datasources_connection_nodes.extract_last_update_time()
    _op_published_datasources_connection_nodes_upstream_tables = (
        _op_published_datasources_connection_nodes.upstream_tables()
    )
    _op_published_datasources_connection_nodes_upstream_tables.id()
    _op_published_datasources_connection_nodes_upstream_tables.name()
    _op_published_datasources_connection_nodes_upstream_tables_database = (
        _op_published_datasources_connection_nodes_upstream_tables.database()
    )
    _op_published_datasources_connection_nodes_upstream_tables_database.name()
    _op_published_datasources_connection_nodes_upstream_tables_database.id()
    _op_published_datasources_connection_nodes_upstream_tables.schema()
    _op_published_datasources_connection_nodes_upstream_tables.full_name()
    _op_published_datasources_connection_nodes_upstream_tables.connection_type()
    _op_published_datasources_connection_nodes_upstream_tables.description()
    _op_published_datasources_connection_nodes_upstream_tables_columns_connection = (
        _op_published_datasources_connection_nodes_upstream_tables.columns_connection()
    )
    _op_published_datasources_connection_nodes_upstream_tables_columns_connection.total_count()
    _op_published_datasources_connection_nodes_fields = (
        _op_published_datasources_connection_nodes.fields()
    )
    _op_published_datasources_connection_nodes_fields.__typename__()
    _op_published_datasources_connection_nodes_fields.id()
    _op_published_datasources_connection_nodes_fields.name()
    _op_published_datasources_connection_nodes_fields.description()
    _op_published_datasources_connection_nodes_fields.is_hidden()
    _op_published_datasources_connection_nodes_fields.folder_name()
    _op_published_datasources_connection_nodes_fields__as__ColumnField = (
        _op_published_datasources_connection_nodes_fields.__as__(_schema.ColumnField)
    )
    _op_published_datasources_connection_nodes_fields__as__ColumnField.data_category()
    _op_published_datasources_connection_nodes_fields__as__ColumnField.role()
    _op_published_datasources_connection_nodes_fields__as__ColumnField.data_type()
    _op_published_datasources_connection_nodes_fields__as__ColumnField.default_format()
    _op_published_datasources_connection_nodes_fields__as__ColumnField.aggregation()
    _op_published_datasources_connection_nodes_fields__as__CalculatedField = (
        _op_published_datasources_connection_nodes_fields.__as__(
            _schema.CalculatedField
        )
    )
    _op_published_datasources_connection_nodes_fields__as__CalculatedField.role()
    _op_published_datasources_connection_nodes_fields__as__CalculatedField.data_type()
    _op_published_datasources_connection_nodes_fields__as__CalculatedField.default_format()
    _op_published_datasources_connection_nodes_fields__as__CalculatedField.aggregation()
    _op_published_datasources_connection_nodes_fields__as__CalculatedField.formula()
    _op_published_datasources_connection_nodes_fields__as__GroupField = (
        _op_published_datasources_connection_nodes_fields.__as__(_schema.GroupField)
    )
    _op_published_datasources_connection_nodes_fields__as__GroupField.role()
    _op_published_datasources_connection_nodes_fields__as__GroupField.data_type()
    _op_published_datasources_connection_nodes_owner = (
        _op_published_datasources_connection_nodes.owner()
    )
    _op_published_datasources_connection_nodes_owner.username()
    _op_published_datasources_connection_nodes.description()
    _op_published_datasources_connection_nodes.uri()
    _op_published_datasources_connection_nodes.project_name()
    _op_published_datasources_connection_nodes_tags = (
        _op_published_datasources_connection_nodes.tags()
    )
    _op_published_datasources_connection_nodes_tags.name()
    _op_published_datasources_connection_page_info = (
        _op_published_datasources_connection.page_info()
    )
    _op_published_datasources_connection_page_info.has_next_page()
    _op_published_datasources_connection_page_info.end_cursor()
    return _op


def query_get_items_fields_connection():
    _op = sgqlc.operation.Operation(
        _schema_root.query_type,
        name="GetItems_fieldsConnection",
        variables=dict(
            first=sgqlc.types.Arg(_schema.Int), after=sgqlc.types.Arg(_schema.String)
        ),
    )
    _op_fields_connection = _op.fields_connection(
        first=sgqlc.types.Variable("first"),
        after=sgqlc.types.Variable("after"),
        filter={
            "idWithin": (
                "668470bc-f0c9-ec5d-7a4b-172370d6ef45",
                "66bd48ba-a895-4951-a4e1-d26fc191eae9",
                "672a63ee-c34b-8cb4-506b-163fcb131a52",
                "72684adf-d214-0a9a-946c-dd7b3879b51f",
                "76636a30-dfbf-0107-0190-a0fc6ce201be",
                "77aa21d6-3b79-0be0-d309-3d36ef1efe71",
                "8fbf178a-ceef-2c97-3ac8-26f95a004d67",
                "968608d1-5da4-1a97-e96a-269010d4ef5b",
                "b5ae252a-c3c0-3a2d-c147-7a39a8de59ef",
                "bc1641b9-525d-00c5-f163-02519d46f0fd",
            )
        },
    )
    _op_fields_connection_nodes = _op_fields_connection.nodes()
    _op_fields_connection_nodes.id()
    _op_fields_connection_nodes_upstream_fields = (
        _op_fields_connection_nodes.upstream_fields()
    )
    _op_fields_connection_nodes_upstream_fields.name()
    _op_fields_connection_nodes_upstream_fields_datasource = (
        _op_fields_connection_nodes_upstream_fields.datasource()
    )
    _op_fields_connection_nodes_upstream_fields_datasource.id()
    _op_fields_connection_nodes_upstream_columns = (
        _op_fields_connection_nodes.upstream_columns()
    )
    _op_fields_connection_nodes_upstream_columns.name()
    _op_fields_connection_nodes_upstream_columns_table = (
        _op_fields_connection_nodes_upstream_columns.table()
    )
    _op_fields_connection_nodes_upstream_columns_table.__typename__()
    _op_fields_connection_nodes_upstream_columns_table.id()
    _op_fields_connection_page_info = _op_fields_connection.page_info()
    _op_fields_connection_page_info.has_next_page()
    _op_fields_connection_page_info.end_cursor()
    return _op


def query_get_items_embedded_datasources_connection():
    _op = sgqlc.operation.Operation(
        _schema_root.query_type,
        name="GetItems_embeddedDatasourcesConnection",
        variables=dict(
            first=sgqlc.types.Arg(_schema.Int), after=sgqlc.types.Arg(_schema.String)
        ),
    )
    _op_embedded_datasources_connection = _op.embedded_datasources_connection(
        first=sgqlc.types.Variable("first"),
        after=sgqlc.types.Variable("after"),
        filter={
            "idWithin": (
                "7437c561-4e94-0283-3462-c6205c2288cd",
                "797a69b1-c32a-2fd4-62aa-99989c1858a6",
                "79871b17-d526-17be-df59-e001f7140924",
                "8f5e3006-6756-8eea-4fd3-44781cea1493",
                "1c5653d6-c448-0850-108b-5c78aeaf6b51",
                "6731f648-b756-31ea-fcdd-77309dd3b0c3",
                "6bd53e72-9fe4-ea86-3d23-14b826c13fa5",
                "3f46592d-f789-8f48-466e-69606990d589",
                "415ddd3e-be04-b466-bc7c-5678a4b0a733",
                "e9b47a00-12ff-72cc-9dfb-d34e0c65095d",
            )
        },
    )
    _op_embedded_datasources_connection_nodes = (
        _op_embedded_datasources_connection.nodes()
    )
    _op_embedded_datasources_connection_nodes.__typename__()
    _op_embedded_datasources_connection_nodes.id()
    _op_embedded_datasources_connection_nodes.name()
    _op_embedded_datasources_connection_nodes.has_extracts()
    _op_embedded_datasources_connection_nodes.extract_last_refresh_time()
    _op_embedded_datasources_connection_nodes.extract_last_incremental_update_time()
    _op_embedded_datasources_connection_nodes.extract_last_update_time()
    _op_embedded_datasources_connection_nodes_downstream_sheets = (
        _op_embedded_datasources_connection_nodes.downstream_sheets()
    )
    _op_embedded_datasources_connection_nodes_downstream_sheets.name()
    _op_embedded_datasources_connection_nodes_downstream_sheets.id()
    _op_embedded_datasources_connection_nodes_upstream_tables = (
        _op_embedded_datasources_connection_nodes.upstream_tables()
    )
    _op_embedded_datasources_connection_nodes_upstream_tables.id()
    _op_embedded_datasources_connection_nodes_upstream_tables.name()
    _op_embedded_datasources_connection_nodes_upstream_tables_database = (
        _op_embedded_datasources_connection_nodes_upstream_tables.database()
    )
    _op_embedded_datasources_connection_nodes_upstream_tables_database.name()
    _op_embedded_datasources_connection_nodes_upstream_tables_database.id()
    _op_embedded_datasources_connection_nodes_upstream_tables.schema()
    _op_embedded_datasources_connection_nodes_upstream_tables.full_name()
    _op_embedded_datasources_connection_nodes_upstream_tables.connection_type()
    _op_embedded_datasources_connection_nodes_upstream_tables.description()
    _op_embedded_datasources_connection_nodes_upstream_tables_columns_connection = (
        _op_embedded_datasources_connection_nodes_upstream_tables.columns_connection()
    )
    _op_embedded_datasources_connection_nodes_upstream_tables_columns_connection.total_count()
    _op_embedded_datasources_connection_nodes_fields = (
        _op_embedded_datasources_connection_nodes.fields()
    )
    _op_embedded_datasources_connection_nodes_fields.__typename__()
    _op_embedded_datasources_connection_nodes_fields.id()
    _op_embedded_datasources_connection_nodes_fields.name()
    _op_embedded_datasources_connection_nodes_fields.description()
    _op_embedded_datasources_connection_nodes_fields.is_hidden()
    _op_embedded_datasources_connection_nodes_fields.folder_name()
    _op_embedded_datasources_connection_nodes_fields__as__ColumnField = (
        _op_embedded_datasources_connection_nodes_fields.__as__(_schema.ColumnField)
    )
    _op_embedded_datasources_connection_nodes_fields__as__ColumnField.data_category()
    _op_embedded_datasources_connection_nodes_fields__as__ColumnField.role()
    _op_embedded_datasources_connection_nodes_fields__as__ColumnField.data_type()
    _op_embedded_datasources_connection_nodes_fields__as__ColumnField.default_format()
    _op_embedded_datasources_connection_nodes_fields__as__ColumnField.aggregation()
    _op_embedded_datasources_connection_nodes_fields__as__CalculatedField = (
        _op_embedded_datasources_connection_nodes_fields.__as__(_schema.CalculatedField)
    )
    _op_embedded_datasources_connection_nodes_fields__as__CalculatedField.role()
    _op_embedded_datasources_connection_nodes_fields__as__CalculatedField.data_type()
    _op_embedded_datasources_connection_nodes_fields__as__CalculatedField.default_format()
    _op_embedded_datasources_connection_nodes_fields__as__CalculatedField.aggregation()
    _op_embedded_datasources_connection_nodes_fields__as__CalculatedField.formula()
    _op_embedded_datasources_connection_nodes_fields__as__GroupField = (
        _op_embedded_datasources_connection_nodes_fields.__as__(_schema.GroupField)
    )
    _op_embedded_datasources_connection_nodes_fields__as__GroupField.role()
    _op_embedded_datasources_connection_nodes_fields__as__GroupField.data_type()
    _op_embedded_datasources_connection_nodes_upstream_datasources = (
        _op_embedded_datasources_connection_nodes.upstream_datasources()
    )
    _op_embedded_datasources_connection_nodes_upstream_datasources.id()
    _op_embedded_datasources_connection_nodes_upstream_datasources.name()
    _op_embedded_datasources_connection_nodes_workbook = (
        _op_embedded_datasources_connection_nodes.workbook()
    )
    _op_embedded_datasources_connection_nodes_workbook.id()
    _op_embedded_datasources_connection_nodes_workbook.name()
    _op_embedded_datasources_connection_nodes_workbook.project_name()
    _op_embedded_datasources_connection_nodes_workbook.luid()
    _op_embedded_datasources_connection_nodes_workbook_owner = (
        _op_embedded_datasources_connection_nodes_workbook.owner()
    )
    _op_embedded_datasources_connection_nodes_workbook_owner.username()
    _op_embedded_datasources_connection_page_info = (
        _op_embedded_datasources_connection.page_info()
    )
    _op_embedded_datasources_connection_page_info.has_next_page()
    _op_embedded_datasources_connection_page_info.end_cursor()
    return _op


def query_get_items_sheets_connection():
    _op = sgqlc.operation.Operation(
        _schema_root.query_type,
        name="GetItems_sheetsConnection",
        variables=dict(
            first=sgqlc.types.Arg(_schema.Int), after=sgqlc.types.Arg(_schema.String)
        ),
    )
    _op_sheets_connection = _op.sheets_connection(
        first=sgqlc.types.Variable("first"),
        after=sgqlc.types.Variable("after"),
        filter={
            "idWithin": (
                "fa9e30e8-645e-1105-642d-c292c70a921c",
                "cea027c2-24a2-d009-4ccf-ac172a3fac6e",
                "b4f94b9f-26dc-3fb3-1973-796e4c91cb21",
                "f6682a87-7396-f12e-2fd1-0424157c6ceb",
                "8fb398c1-0b18-528a-c3bd-2a03c35528f5",
                "ffe3435f-3e0b-9618-389c-055cbed13ac9",
                "4e51108f-3ba4-0749-6518-8104fc62c202",
                "67f86c94-c102-447a-8752-b1e497bf4551",
                "cbb0b196-5f2a-ecd4-0b2b-e87db220ff47",
                "1359177d-c634-2cff-4408-1751152c7fc2",
            )
        },
    )
    _op_sheets_connection_nodes = _op_sheets_connection.nodes()
    _op_sheets_connection_nodes.id()
    _op_sheets_connection_nodes.name()
    _op_sheets_connection_nodes.path()
    _op_sheets_connection_nodes.luid()
    _op_sheets_connection_nodes.created_at()
    _op_sheets_connection_nodes.updated_at()
    _op_sheets_connection_nodes_tags = _op_sheets_connection_nodes.tags()
    _op_sheets_connection_nodes_tags.name()
    _op_sheets_connection_nodes_contained_in_dashboards = (
        _op_sheets_connection_nodes.contained_in_dashboards()
    )
    _op_sheets_connection_nodes_contained_in_dashboards.name()
    _op_sheets_connection_nodes_contained_in_dashboards.path()
    _op_sheets_connection_nodes_workbook = _op_sheets_connection_nodes.workbook()
    _op_sheets_connection_nodes_workbook.id()
    _op_sheets_connection_nodes_workbook.name()
    _op_sheets_connection_nodes_workbook.project_name()
    _op_sheets_connection_nodes_workbook.luid()
    _op_sheets_connection_nodes_workbook_owner = (
        _op_sheets_connection_nodes_workbook.owner()
    )
    _op_sheets_connection_nodes_workbook_owner.username()
    _op_sheets_connection_nodes_datasource_fields = (
        _op_sheets_connection_nodes.datasource_fields()
    )
    _op_sheets_connection_nodes_datasource_fields.__typename__()
    _op_sheets_connection_nodes_datasource_fields.id()
    _op_sheets_connection_nodes_datasource_fields.name()
    _op_sheets_connection_nodes_datasource_fields.description()
    _op_sheets_connection_nodes_datasource_fields_datasource = (
        _op_sheets_connection_nodes_datasource_fields.datasource()
    )
    _op_sheets_connection_nodes_datasource_fields_datasource.id()
    _op_sheets_connection_nodes_datasource_fields_datasource.name()
    _op_sheets_connection_nodes_datasource_fields__as__ColumnField = (
        _op_sheets_connection_nodes_datasource_fields.__as__(_schema.ColumnField)
    )
    _op_sheets_connection_nodes_datasource_fields__as__ColumnField.data_category()
    _op_sheets_connection_nodes_datasource_fields__as__ColumnField.role()
    _op_sheets_connection_nodes_datasource_fields__as__ColumnField.data_type()
    _op_sheets_connection_nodes_datasource_fields__as__ColumnField.aggregation()
    _op_sheets_connection_nodes_datasource_fields__as__CalculatedField = (
        _op_sheets_connection_nodes_datasource_fields.__as__(_schema.CalculatedField)
    )
    _op_sheets_connection_nodes_datasource_fields__as__CalculatedField.role()
    _op_sheets_connection_nodes_datasource_fields__as__CalculatedField.data_type()
    _op_sheets_connection_nodes_datasource_fields__as__CalculatedField.aggregation()
    _op_sheets_connection_nodes_datasource_fields__as__CalculatedField.formula()
    _op_sheets_connection_nodes_datasource_fields__as__GroupField = (
        _op_sheets_connection_nodes_datasource_fields.__as__(_schema.GroupField)
    )
    _op_sheets_connection_nodes_datasource_fields__as__GroupField.role()
    _op_sheets_connection_nodes_datasource_fields__as__GroupField.data_type()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField = (
        _op_sheets_connection_nodes_datasource_fields.__as__(_schema.DatasourceField)
    )
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field = _op_sheets_connection_nodes_datasource_fields__as__DatasourceField.remote_field()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field.__typename__()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field.id()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field.name()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field.description()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field.folder_name()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field__as__ColumnField = _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field.__as__(
        _schema.ColumnField
    )
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field__as__ColumnField.data_category()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field__as__ColumnField.role()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field__as__ColumnField.data_type()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field__as__ColumnField.aggregation()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field__as__CalculatedField = _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field.__as__(
        _schema.CalculatedField
    )
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field__as__CalculatedField.role()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field__as__CalculatedField.data_type()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field__as__CalculatedField.aggregation()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field__as__CalculatedField.formula()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field__as__GroupField = _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field.__as__(
        _schema.GroupField
    )
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field__as__GroupField.role()
    _op_sheets_connection_nodes_datasource_fields__as__DatasourceField_remote_field__as__GroupField.data_type()
    _op_sheets_connection_page_info = _op_sheets_connection.page_info()
    _op_sheets_connection_page_info.has_next_page()
    _op_sheets_connection_page_info.end_cursor()
    return _op


def query_get_items_workbooks_connection():
    _op = sgqlc.operation.Operation(
        _schema_root.query_type,
        name="GetItems_workbooksConnection",
        variables=dict(
            first=sgqlc.types.Arg(_schema.Int), after=sgqlc.types.Arg(_schema.String)
        ),
    )
    _op_workbooks_connection = _op.workbooks_connection(
        first=sgqlc.types.Variable("first"),
        after=sgqlc.types.Variable("after"),
        filter={"projectNameWithin": ("project_test2",)},
    )
    _op_workbooks_connection_nodes = _op_workbooks_connection.nodes()
    _op_workbooks_connection_nodes.id()
    _op_workbooks_connection_nodes.name()
    _op_workbooks_connection_nodes.luid()
    _op_workbooks_connection_nodes.uri()
    _op_workbooks_connection_nodes.project_name()
    _op_workbooks_connection_nodes_owner = _op_workbooks_connection_nodes.owner()
    _op_workbooks_connection_nodes_owner.username()
    _op_workbooks_connection_nodes.description()
    _op_workbooks_connection_nodes.uri()
    _op_workbooks_connection_nodes.created_at()
    _op_workbooks_connection_nodes.updated_at()
    _op_workbooks_connection_nodes_tags = _op_workbooks_connection_nodes.tags()
    _op_workbooks_connection_nodes_tags.name()
    _op_workbooks_connection_nodes_sheets = _op_workbooks_connection_nodes.sheets()
    _op_workbooks_connection_nodes_sheets.id()
    _op_workbooks_connection_nodes_dashboards = (
        _op_workbooks_connection_nodes.dashboards()
    )
    _op_workbooks_connection_nodes_dashboards.id()
    _op_workbooks_connection_nodes_embedded_datasources = (
        _op_workbooks_connection_nodes.embedded_datasources()
    )
    _op_workbooks_connection_nodes_embedded_datasources.id()
    _op_workbooks_connection_page_info = _op_workbooks_connection.page_info()
    _op_workbooks_connection_page_info.has_next_page()
    _op_workbooks_connection_page_info.end_cursor()
    return _op


class Query:
    get_items_custom_sqltables_connection = (
        query_get_items_custom_sqltables_connection()
    )
    get_items_database_tables_connection = query_get_items_database_tables_connection()
    get_items_embedded_datasources_connection = (
        query_get_items_embedded_datasources_connection()
    )
    get_items_fields_connection = query_get_items_fields_connection()
    get_items_published_datasources_connection = (
        query_get_items_published_datasources_connection()
    )
    get_items_sheets_connection = query_get_items_sheets_connection()
    get_items_workbooks_connection = query_get_items_workbooks_connection()


class Operations:
    query = Query
