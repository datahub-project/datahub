"""Shared calc-view XML fixtures used by both the unit tests
(``tests/unit/test_hana_source.py``) and the mock integration test
(``tests/integration/hana/test_hana_calc_views_mock.py``).
"""

PROJECTION_VIEW_XML = """\
<Calculation:scenario xmlns:Calculation="http://www.sap.com/ndb/BiModelCalculation.ecore"
                      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dataSources>
    <DataSource id="CUSTOMERS" type="DATA_BASE_TABLE">
      <columnObject schemaName="REPORTING" columnObjectName="CUSTOMERS"/>
    </DataSource>
  </dataSources>
  <calculationViews>
    <calculationView xsi:type="Calculation:ProjectionView" id="Projection_1">
      <input node="#CUSTOMERS">
        <mapping xsi:type="Calculation:AttributeMapping"
                 source="ID" target="CUST_ID"/>
        <mapping xsi:type="Calculation:AttributeMapping"
                 source="NAME" target="CUST_NAME"/>
      </input>
    </calculationView>
  </calculationViews>
  <logicalModel>
    <attributes>
      <attribute id="CUSTOMER_ID">
        <keyMapping columnObjectName="Projection_1" columnName="CUST_ID"/>
      </attribute>
      <attribute id="CUSTOMER_NAME">
        <keyMapping columnObjectName="Projection_1" columnName="CUST_NAME"/>
      </attribute>
    </attributes>
  </logicalModel>
</Calculation:scenario>
"""

SQL_SCRIPT_VIEW_XML = """\
<Calculation:scenario xmlns:Calculation="http://www.sap.com/ndb/BiModelCalculation.ecore"
                      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                      calculationScenarioType="SCRIPT_BASED">
  <dataSources/>
  <calculationViews>
    <calculationView xsi:type="Calculation:SqlScriptView" id="Script_View">
      <viewAttributes>
        <viewAttribute id="CUST_ID"/>
        <viewAttribute id="REVENUE"/>
      </viewAttributes>
      <definition>BEGIN
  RESULT_SET = SELECT "ID" AS "CUST_ID", "TOTAL" AS "REVENUE"
               FROM "REPORTING"."SALES"
               JOIN "REPORTING"."CUSTOMERS"
                 ON "SALES"."CUST_ID" = "CUSTOMERS"."ID";
END</definition>
    </calculationView>
  </calculationViews>
  <logicalModel id="Script_View">
    <attributes>
      <attribute id="CUST_ID">
        <keyMapping columnObjectName="Script_View" columnName="CUST_ID"/>
      </attribute>
    </attributes>
    <baseMeasures>
      <measure id="REVENUE">
        <measureMapping columnObjectName="Script_View" columnName="REVENUE"/>
      </measure>
    </baseMeasures>
  </logicalModel>
</Calculation:scenario>
"""
