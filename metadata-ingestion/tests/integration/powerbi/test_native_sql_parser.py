from typing import List

from datahub.ingestion.source.powerbi.m_query import native_sql_parser


def test_join():
    query: str = "select A.name from GSL_TEST_DB.PUBLIC.SALES_ANALYST as A inner join GSL_TEST_DB.PUBLIC.SALES_FORECAST as B on A.name = B.name where startswith(A.name, 'mo')"
    tables: List[str] = native_sql_parser.get_tables(query)

    assert len(tables) == 2
    assert tables[0] == "GSL_TEST_DB.PUBLIC.SALES_ANALYST"
    assert tables[1] == "GSL_TEST_DB.PUBLIC.SALES_FORECAST"


def test_simple_from():
    query: str = "SELECT#(lf)concat((UPPER(REPLACE(SELLER,'-',''))), MONTHID) as AGENT_KEY,#(lf)concat((UPPER(REPLACE(CLIENT_DIRECTOR,'-',''))), MONTHID) as CD_AGENT_KEY,#(lf) *#(lf)FROM#(lf)OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_APS_SME_UNITS_V4"

    tables: List[str] = native_sql_parser.get_tables(query)

    assert len(tables) == 1
    assert tables[0] == "OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_APS_SME_UNITS_V4"


def test_advance_query_1():
    query: str = """
        WITH employee_ranking AS (
            SELECT
                employee_id,
                last_name,
                first_name,
                salary,
                RANK() OVER (ORDER BY salary DESC) as ranking
            FROM employee
        )
        SELECT
            employee_id,
            last_name,
            first_name,
            salary
        FROM employee_ranking
        WHERE ranking <= 5
        ORDER BY ranking
    """
    tables: List[str] = native_sql_parser.get_tables(query, advance_sql_construct=True)

    assert len(tables) == 1
    assert tables[0] == "public.employee"


def test_advance_query_2_self_join():
    query: str = """
        SELECT
            s1.first_name,
            s2.first_name
        FROM student s1
        JOIN student s2
        ON s1.id = s2.id
    """

    tables: List[str] = native_sql_parser.get_tables(query, advance_sql_construct=True)

    assert len(tables) == 1
    assert tables[0] == "public.student"


def test_advance_query_3():
    query: str = """
        SELECT Name, OrderNumber
        FROM PROD.Order
        WHERE OrderID IN
            (SELECT OrderID
            FROM Sales.SalesOrder
            WHERE OrderQty > 5)
    """

    tables: List[str] = native_sql_parser.get_tables(query, advance_sql_construct=True)

    assert len(tables) == 2
    assert tables[0] == "prod.order"
    assert tables[1] == "sales.salesorder"
