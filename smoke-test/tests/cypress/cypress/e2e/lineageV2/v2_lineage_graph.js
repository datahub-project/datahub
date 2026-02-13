import { getTimestampMillisNumDaysAgo } from "../../support/commands";
import {
  checkCounter,
  checkFilteringNodeExist,
  checkIfEdgeBetweenColumnsExist,
  checkIfEdgeBetweenColumnsNotExist,
  checkIfEdgeExist,
  checkIfNodeExist,
  checkIfNodeNotExist,
  checkMatches,
  clearFilter,
  contract,
  ensureTitleHasText,
  expandAll,
  expandColumns,
  expandOne,
  filter,
  hoverColumn,
  selectColumn,
  showAll,
  showLess,
  showMore,
  unhoverColumn,
} from "./utils";

const DATASET_ENTITY_TYPE = "dataset";
const CHART_ENTITY_TYPE = "chart";
const TASKS_ENTITY_TYPE = "tasks";
const DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)";
const JAN_1_2021_TIMESTAMP = 1609553357755;
const JAN_1_2022_TIMESTAMP = 1641089357755;
const TIMESTAMP_MILLIS_14_DAYS_AGO = getTimestampMillisNumDaysAgo(14);
const TIMESTAMP_MILLIS_7_DAYS_AGO = getTimestampMillisNumDaysAgo(7);
const TIMESTAMP_MILLIS_NOW = getTimestampMillisNumDaysAgo(0);
const GNP_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,economic_data.gnp,PROD)";
const TRANSACTION_ETL_URN =
  "urn:li:dataJob:(urn:li:dataFlow:(airflow,bq_etl,prod),transaction_etl)";
const MONTHLY_TEMPERATURE_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.monthly_temperature,PROD)";
const NODE1_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:postgres,cypress_lineage.node1_dataset,PROD)";
const NODE2_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage.node2_dataset,PROD)";
const NODE3_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage.node3_dataset,PROD)";
const NODE4_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage.node4_dataset,PROD)";
const NODE5_DATASET_MANUAL_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage.node5_dataset_manual,PROD)";
const NODE6_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage.node6_dataset,PROD)";
const NODE7_DATAJOB_URN =
  "urn:li:dataJob:(urn:li:dataFlow:(airflow,cypress_lineage_pipeline,PROD),cypress_lineage.node7_datajob)";
const NODE8_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage.node8_dataset,PROD)";
const NODE9_CHART_URN = "urn:li:chart:(looker,cypress_lineage.node9_chart)";
const NODE10_DASHBOARD_URN =
  "urn:li:dashboard:(looker,cypress_lineage.node10_dashboard)";
const NODE11_DBT_URN =
  "urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_lineage.node11_dbt,PROD)";
const NODE12_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage.node12_dataset,PROD)";
const FILTERING_NODE1_URN =
  "urn:li:dataset:(urn:li:dataPlatform:postgres,cypress_lineage_filtering.node1,PROD)";
const FILTERING_NODE2_URN =
  "urn:li:dataset:(urn:li:dataPlatform:postgres,cypress_lineage_filtering.node2,PROD)";
const FILTERING_NODE3_URN =
  "urn:li:dataset:(urn:li:dataPlatform:postgres,cypress_lineage_filtering.node3,PROD)";
const FILTERING_NODE4_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage_filtering.node4,PROD)";
const FILTERING_NODE5_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage_filtering.node5,PROD)";
const FILTERING_NODE6_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage_filtering.node6,PROD)";
const FILTERING_NODE7_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage_filtering.node7,PROD)";
const FILTERING_NODE8_URN =
  "urn:li:dataset:(urn:li:dataPlatform:postgres,cypress_lineage_filtering.node8,PROD)";
const FILTERING_NODE9_URN =
  "urn:li:dataset:(urn:li:dataPlatform:postgres,cypress_lineage_filtering.node9,PROD)";
const FILTERING_NODE10_URN =
  "urn:li:dataset:(urn:li:dataPlatform:postgres,cypress_lineage_filtering.node10,PROD)";
const FILTERING_NODE11_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage_filtering.node11,PROD)";
const FILTERING_NODE12_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage_filtering.node12,PROD)";
const FILTERING_NODE13_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage_filtering.node13,PROD)";
const FILTERING_NODE14_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage_filtering.node14,PROD)";
const FILTERING_NODE15_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage_filtering.node15,PROD)";
const FILTERING_NODE16_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage_filtering.node16,PROD)";
const FILTERING_NODE17_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage_filtering.node17,PROD)";
const FILTERING_NODE18_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage_filtering.node18,PROD)";
const FILTERING_NODE19_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage_filtering.node19,PROD)";
const FILTERING_NODE20_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,cypress_lineage_filtering.node20,PROD)";

describe("lineage_graph", () => {
  beforeEach(() => {
    cy.setFeatureFlags(true, (res) => {
      res.body.data.appConfig.featureFlags.lineageGraphV3 = false;
    });
  });
  it("can see full history", () => {
    cy.login();
    cy.goToEntityLineageGraphV2(DATASET_ENTITY_TYPE, DATASET_URN);

    cy.contains("SampleCypressKafka").should("be.visible");
    cy.contains("SampleCypressHdfs");
    cy.contains("Baz Chart 1");
    cy.contains("some-cypress");
  });

  it("cannot see any lineage edges for 2021", () => {
    cy.login();
    cy.goToEntityLineageGraphV2(
      DATASET_ENTITY_TYPE,
      DATASET_URN,
      JAN_1_2021_TIMESTAMP,
      JAN_1_2022_TIMESTAMP,
    );

    cy.contains("SampleCypressKafka").should("be.visible");
    cy.contains("SampleCypressHdfs").should("not.exist");
    cy.contains("Baz Chart 1").should("not.exist");
    cy.contains("some-cypress").should("not.exist");
  });

  it("can see when the inputs to a data job change", () => {
    cy.login();
    // Between 14 days ago and 7 days ago, only transactions was an input
    cy.goToEntityLineageGraphV2(
      TASKS_ENTITY_TYPE,
      TRANSACTION_ETL_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
    );
    // cy.contains("transaction_etl");
    cy.wait(3000);
    cy.contains("aggregated").should("be.visible");
    cy.contains("transactions");
    cy.contains("user_profile").should("not.exist");
    // 1 day ago, user_profile was also added as an input
    cy.goToEntityLineageGraphV2(
      TASKS_ENTITY_TYPE,
      TRANSACTION_ETL_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    // cy.contains("transaction_etl");
    cy.wait(3000);
    cy.contains("aggregated").should("be.visible");
    cy.contains("transactions");
    cy.contains("user_profile");
  });

  it("can see when a data job is replaced", () => {
    cy.login();
    // Between 14 days ago and 7 days ago, only temperature_etl_1 was an iput
    cy.goToEntityLineageGraphV2(
      DATASET_ENTITY_TYPE,
      MONTHLY_TEMPERATURE_DATASET_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
    );
    cy.contains("monthly_temperature").should("be.visible");
    // cy.contains("temperature_etl_1");
    cy.contains("temperature_etl_2").should("not.exist");
    // Since 7 days ago, temperature_etl_1 has been replaced by temperature_etl_2
    cy.goToEntityLineageGraphV2(
      DATASET_ENTITY_TYPE,
      MONTHLY_TEMPERATURE_DATASET_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    cy.contains("monthly_temperature");
    cy.contains("temperature_etl_1").should("not.exist");
    // cy.contains("temperature_etl_2");
  });

  it("can see when a dataset join changes", () => {
    cy.login();
    // 8 days ago, both gdp and factor_income were joined to create gnp
    cy.goToEntityLineageGraphV2(
      DATASET_ENTITY_TYPE,
      GNP_DATASET_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    cy.contains("gnp").should("be.visible");
    cy.contains("gdp");
    cy.contains("factor_income");
    // 1 day ago, factor_income was removed from the join
    cy.goToEntityLineageGraphV2(
      DATASET_ENTITY_TYPE,
      GNP_DATASET_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    cy.contains("gnp");
    cy.contains("gdp");
    cy.contains("factor_income").should("not.exist");
  });

  it("can edit upstream lineage", () => {
    // note: this test does not make any mutations. This is to prevent introducing a flaky test.
    // Ideally, we should refactor this test to verify that the mutations are correct as well. However, it needs to be done carefully to avoid introducing flakiness.

    cy.login();
    cy.goToEntityLineageGraphV2(DATASET_ENTITY_TYPE, DATASET_URN);
    cy.get(
      '[data-testid="manage-lineage-menu-urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)"]',
    ).click();
    cy.get('[data-testid="edit-upstream-lineage"]').click();
    cy.contains(
      "Select the Upstreams to add to SampleCypressKafkaDataset",
    ).should("be.visible");

    // find the button that says "Set Upstreams" - not via test id
    // verify that is is not disabled
    cy.contains("Set Upstreams").should("be.disabled");

    // there are multiple search inputs, we want to find the one for the search bar in the modal
    cy.get('[role="dialog"] [data-testid="search-input"]').type(
      "cypress_health_test",
    );

    // find the checkbox for cypress_health_test (data-testid="preview-urn:li:dataset:(urn:li:dataPlatform:hive,cypress_health_test,PROD)") is the
    // test id for a sibling of the checkbox, we want to find that id, find its parent, and click the checkbox

    cy.get(
      '[data-testid="preview-urn:li:dataset:(urn:li:dataPlatform:hive,cypress_health_test,PROD)"]',
    )
      .parent()
      .find("input")
      .click();

    // find the button that says "Set Upstreams" - not via test id
    // verify that is is not disabled
    cy.contains("Set Upstreams").should("not.be.disabled");
  });

  it("displays complete lineage graph with all node types", () => {
    cy.login();
    cy.goToEntityLineageGraphV2(DATASET_ENTITY_TYPE, NODE1_DATASET_URN);

    // Check initial state

    checkIfNodeExist(NODE1_DATASET_URN);
    checkIfNodeExist(NODE2_DATASET_URN);
    checkIfNodeExist(NODE5_DATASET_MANUAL_URN);
    checkIfEdgeExist(NODE1_DATASET_URN, NODE2_DATASET_URN); // dataset -> dataset
    checkIfEdgeExist(NODE1_DATASET_URN, NODE5_DATASET_MANUAL_URN); // dataset -> dataset (manual lineage)

    // Fields lineage

    expandColumns(NODE2_DATASET_URN);
    hoverColumn(NODE2_DATASET_URN, "record_id");
    checkIfEdgeBetweenColumnsExist(
      NODE1_DATASET_URN,
      "record_id",
      NODE2_DATASET_URN,
      "record_id",
    );
    unhoverColumn(NODE2_DATASET_URN, "record_id");
    checkIfEdgeBetweenColumnsNotExist(
      NODE1_DATASET_URN,
      "record_id",
      NODE2_DATASET_URN,
      "record_id",
    );
    hoverColumn(NODE2_DATASET_URN, "next_record_id");
    checkIfEdgeBetweenColumnsExist(
      NODE1_DATASET_URN,
      "next_record_id",
      NODE2_DATASET_URN,
      "next_record_id",
    );
    unhoverColumn(NODE2_DATASET_URN, "next_record_id");
    checkIfEdgeBetweenColumnsNotExist(
      NODE1_DATASET_URN,
      "next_record_id",
      NODE2_DATASET_URN,
      "next_record_id",
    );
    selectColumn(NODE2_DATASET_URN, "record_id");
    checkIfEdgeBetweenColumnsExist(
      NODE1_DATASET_URN,
      "record_id",
      NODE2_DATASET_URN,
      "record_id",
    );
    selectColumn(NODE2_DATASET_URN, "next_record_id");
    checkIfEdgeBetweenColumnsExist(
      NODE1_DATASET_URN,
      "next_record_id",
      NODE2_DATASET_URN,
      "next_record_id",
    );
    checkIfEdgeBetweenColumnsNotExist(
      NODE1_DATASET_URN,
      "record_id",
      NODE2_DATASET_URN,
      "record_id",
    );

    // Expand one node

    expandOne(NODE2_DATASET_URN);
    checkIfNodeExist(NODE3_DATASET_URN);
    checkIfEdgeExist(NODE2_DATASET_URN, NODE3_DATASET_URN);
    expandOne(NODE3_DATASET_URN);
    checkIfNodeExist(NODE4_DATASET_URN);
    checkIfEdgeExist(NODE3_DATASET_URN, NODE4_DATASET_URN);

    // Expand all nodes

    expandAll(NODE5_DATASET_MANUAL_URN);
    checkIfNodeExist(NODE6_DATASET_URN);
    checkIfNodeExist(NODE4_DATASET_URN);
    checkIfEdgeExist(NODE5_DATASET_MANUAL_URN, NODE6_DATASET_URN);
    checkIfEdgeExist(NODE6_DATASET_URN, NODE4_DATASET_URN);
    checkIfEdgeExist(NODE3_DATASET_URN, NODE4_DATASET_URN);
    // Datajob
    checkIfNodeExist(NODE7_DATAJOB_URN);
    checkIfNodeExist(NODE8_DATASET_URN);
    checkIfEdgeExist(NODE4_DATASET_URN, NODE7_DATAJOB_URN);
    checkIfEdgeExist(NODE7_DATAJOB_URN, NODE8_DATASET_URN);
    // Dataset -> Chart -> Dashboard
    checkIfNodeExist(NODE9_CHART_URN);
    checkIfNodeExist(NODE10_DASHBOARD_URN);
    checkIfEdgeExist(NODE8_DATASET_URN, NODE9_CHART_URN);
    checkIfEdgeExist(NODE9_CHART_URN, NODE10_DASHBOARD_URN);
    // Dbt
    checkIfNodeExist(NODE11_DBT_URN);
    checkIfNodeExist(NODE12_DATASET_URN);
    checkIfEdgeExist(NODE8_DATASET_URN, NODE11_DBT_URN);
    checkIfEdgeExist(NODE11_DBT_URN, NODE12_DATASET_URN);

    // Contract nodes

    // Contract single node
    contract(NODE9_CHART_URN);
    checkIfNodeNotExist(NODE10_DASHBOARD_URN);
    // Contract multiple nodes
    contract(NODE4_DATASET_URN);
    checkIfNodeNotExist(NODE7_DATAJOB_URN);
    checkIfNodeNotExist(NODE8_DATASET_URN);
    checkIfNodeNotExist(NODE9_CHART_URN);
    checkIfNodeNotExist(NODE11_DBT_URN);
    checkIfNodeNotExist(NODE12_DATASET_URN);
    // Contract when a node has two input edges
    contract(NODE5_DATASET_MANUAL_URN);
    checkIfNodeNotExist(NODE6_DATASET_URN);
    checkIfNodeExist(NODE4_DATASET_URN); // should exist

    // Downstream expanding

    // Expand one (downstream)
    cy.goToEntityLineageGraphV2(CHART_ENTITY_TYPE, NODE9_CHART_URN);
    checkIfNodeExist(NODE9_CHART_URN);
    checkIfNodeExist(NODE8_DATASET_URN);
    expandOne(NODE8_DATASET_URN);
    checkIfNodeExist(NODE7_DATAJOB_URN);
    checkIfNodeExist(NODE4_DATASET_URN);
    checkIfEdgeExist(NODE7_DATAJOB_URN, NODE8_DATASET_URN);
    checkIfEdgeExist(NODE4_DATASET_URN, NODE7_DATAJOB_URN);
    // Expand all (downstream)
    cy.goToEntityLineageGraphV2(CHART_ENTITY_TYPE, NODE9_CHART_URN);
    checkIfNodeExist(NODE9_CHART_URN);
    checkIfNodeExist(NODE8_DATASET_URN);
    expandAll(NODE8_DATASET_URN);
    checkIfNodeExist(NODE7_DATAJOB_URN);
    checkIfNodeExist(NODE4_DATASET_URN);
    checkIfNodeExist(NODE6_DATASET_URN);
    checkIfNodeExist(NODE5_DATASET_MANUAL_URN);
    checkIfNodeExist(NODE3_DATASET_URN);
    checkIfNodeExist(NODE2_DATASET_URN);
    checkIfNodeExist(NODE1_DATASET_URN);
    checkIfEdgeExist(NODE1_DATASET_URN, NODE2_DATASET_URN);
    checkIfEdgeExist(NODE2_DATASET_URN, NODE3_DATASET_URN);
    checkIfEdgeExist(NODE3_DATASET_URN, NODE4_DATASET_URN);
    checkIfEdgeExist(NODE1_DATASET_URN, NODE5_DATASET_MANUAL_URN);
    checkIfEdgeExist(NODE5_DATASET_MANUAL_URN, NODE6_DATASET_URN);
    checkIfEdgeExist(NODE6_DATASET_URN, NODE4_DATASET_URN);
    checkIfEdgeExist(NODE4_DATASET_URN, NODE7_DATAJOB_URN);
    checkIfEdgeExist(NODE7_DATAJOB_URN, NODE8_DATASET_URN);
  });

  it("should allow to expand and filter children", () => {
    cy.login();
    cy.goToEntityLineageGraphV2(DATASET_ENTITY_TYPE, FILTERING_NODE7_URN);

    checkIfNodeExist(FILTERING_NODE7_URN);

    // Check upstream filtering node

    // Initial state
    checkFilteringNodeExist(FILTERING_NODE7_URN, "up");
    ensureTitleHasText(FILTERING_NODE7_URN, "up", "4 of 6 shown");
    checkCounter(FILTERING_NODE7_URN, "up", "platform", "PostgreSQL", "3");
    checkCounter(FILTERING_NODE7_URN, "up", "platform", "Snowflake", "3");
    checkCounter(FILTERING_NODE7_URN, "up", "subtype", "Datasets", "6");
    checkIfNodeNotExist(FILTERING_NODE1_URN);
    checkIfNodeNotExist(FILTERING_NODE2_URN);
    checkIfNodeExist(FILTERING_NODE3_URN);
    checkIfNodeExist(FILTERING_NODE4_URN);
    checkIfNodeExist(FILTERING_NODE5_URN);
    checkIfNodeExist(FILTERING_NODE6_URN);
    // Show more
    showMore(FILTERING_NODE7_URN, "up");
    checkIfNodeExist(FILTERING_NODE1_URN);
    checkIfNodeExist(FILTERING_NODE2_URN);
    checkIfNodeExist(FILTERING_NODE3_URN);
    checkIfNodeExist(FILTERING_NODE4_URN);
    checkIfNodeExist(FILTERING_NODE5_URN);
    checkIfNodeExist(FILTERING_NODE6_URN);
    // Filtering (less than 3 characters)
    filter(FILTERING_NODE7_URN, "up", "xx");
    checkIfNodeExist(FILTERING_NODE1_URN);
    checkIfNodeExist(FILTERING_NODE2_URN);
    checkIfNodeExist(FILTERING_NODE3_URN);
    checkIfNodeExist(FILTERING_NODE4_URN);
    checkIfNodeExist(FILTERING_NODE5_URN);
    checkIfNodeExist(FILTERING_NODE6_URN);
    // Filtering (single node)
    filter(FILTERING_NODE7_URN, "up", "node6");
    ensureTitleHasText(FILTERING_NODE7_URN, "up", "1 of 6 shown");
    checkMatches(FILTERING_NODE7_URN, "up", "1");
    checkIfNodeNotExist(FILTERING_NODE1_URN);
    checkIfNodeNotExist(FILTERING_NODE2_URN);
    checkIfNodeNotExist(FILTERING_NODE3_URN);
    checkIfNodeNotExist(FILTERING_NODE4_URN);
    checkIfNodeNotExist(FILTERING_NODE5_URN);
    checkIfNodeExist(FILTERING_NODE6_URN);
    // Filtering (multiple nodes)
    filter(FILTERING_NODE7_URN, "up", "node");
    ensureTitleHasText(FILTERING_NODE7_URN, "up", "6 of 6 shown");
    checkMatches(FILTERING_NODE7_URN, "up", "6");
    checkIfNodeExist(FILTERING_NODE1_URN);
    checkIfNodeExist(FILTERING_NODE2_URN);
    checkIfNodeExist(FILTERING_NODE3_URN);
    checkIfNodeExist(FILTERING_NODE4_URN);
    checkIfNodeExist(FILTERING_NODE5_URN);
    checkIfNodeExist(FILTERING_NODE6_URN);
    // Filtering (no any nodes)
    filter(FILTERING_NODE7_URN, "up", "unknown");
    ensureTitleHasText(FILTERING_NODE7_URN, "up", "0 of 6 shown");
    checkMatches(FILTERING_NODE7_URN, "up", "0");
    checkIfNodeNotExist(FILTERING_NODE1_URN);
    checkIfNodeNotExist(FILTERING_NODE2_URN);
    checkIfNodeNotExist(FILTERING_NODE3_URN);
    checkIfNodeNotExist(FILTERING_NODE4_URN);
    checkIfNodeNotExist(FILTERING_NODE5_URN);
    checkIfNodeNotExist(FILTERING_NODE6_URN);
    // Filtering (platform)
    filter(FILTERING_NODE7_URN, "up", "postgres");
    ensureTitleHasText(FILTERING_NODE7_URN, "up", "3 of 6 shown");
    checkMatches(FILTERING_NODE7_URN, "up", "3");
    checkIfNodeExist(FILTERING_NODE1_URN);
    checkIfNodeExist(FILTERING_NODE2_URN);
    checkIfNodeExist(FILTERING_NODE3_URN);
    checkIfNodeNotExist(FILTERING_NODE4_URN);
    checkIfNodeNotExist(FILTERING_NODE5_URN);
    checkIfNodeNotExist(FILTERING_NODE6_URN);
    filter(FILTERING_NODE7_URN, "up", "snowflake");
    ensureTitleHasText(FILTERING_NODE7_URN, "up", "3 of 6 shown");
    checkMatches(FILTERING_NODE7_URN, "up", "3");
    checkIfNodeNotExist(FILTERING_NODE1_URN);
    checkIfNodeNotExist(FILTERING_NODE2_URN);
    checkIfNodeNotExist(FILTERING_NODE3_URN);
    checkIfNodeExist(FILTERING_NODE4_URN);
    checkIfNodeExist(FILTERING_NODE5_URN);
    checkIfNodeExist(FILTERING_NODE6_URN);

    // Check donwsteam filtering node

    // Initial state
    checkFilteringNodeExist(FILTERING_NODE7_URN, "down");
    ensureTitleHasText(FILTERING_NODE7_URN, "down", "4 of 13 shown");
    checkCounter(FILTERING_NODE7_URN, "down", "platform", "PostgreSQL", "3");
    checkCounter(FILTERING_NODE7_URN, "down", "platform", "Snowflake", "10");
    checkCounter(FILTERING_NODE7_URN, "down", "subtype", "Datasets", "13");
    checkIfNodeNotExist(FILTERING_NODE8_URN);
    checkIfNodeNotExist(FILTERING_NODE9_URN);
    checkIfNodeNotExist(FILTERING_NODE10_URN);
    checkIfNodeNotExist(FILTERING_NODE11_URN);
    checkIfNodeNotExist(FILTERING_NODE12_URN);
    checkIfNodeNotExist(FILTERING_NODE13_URN);
    checkIfNodeNotExist(FILTERING_NODE14_URN);
    checkIfNodeNotExist(FILTERING_NODE15_URN);
    checkIfNodeNotExist(FILTERING_NODE16_URN);
    checkIfNodeExist(FILTERING_NODE17_URN);
    checkIfNodeExist(FILTERING_NODE18_URN);
    checkIfNodeExist(FILTERING_NODE19_URN);
    checkIfNodeExist(FILTERING_NODE20_URN);
    // Show more
    showMore(FILTERING_NODE7_URN, "down");
    checkIfNodeNotExist(FILTERING_NODE8_URN);
    checkIfNodeNotExist(FILTERING_NODE9_URN);
    checkIfNodeNotExist(FILTERING_NODE10_URN);
    checkIfNodeNotExist(FILTERING_NODE11_URN);
    checkIfNodeNotExist(FILTERING_NODE12_URN);
    checkIfNodeExist(FILTERING_NODE13_URN);
    checkIfNodeExist(FILTERING_NODE14_URN);
    checkIfNodeExist(FILTERING_NODE15_URN);
    checkIfNodeExist(FILTERING_NODE16_URN);
    checkIfNodeExist(FILTERING_NODE17_URN);
    checkIfNodeExist(FILTERING_NODE18_URN);
    checkIfNodeExist(FILTERING_NODE19_URN);
    checkIfNodeExist(FILTERING_NODE20_URN);
    // Show less
    showLess(FILTERING_NODE7_URN, "down");
    checkIfNodeNotExist(FILTERING_NODE8_URN);
    checkIfNodeNotExist(FILTERING_NODE9_URN);
    checkIfNodeNotExist(FILTERING_NODE10_URN);
    checkIfNodeNotExist(FILTERING_NODE11_URN);
    checkIfNodeNotExist(FILTERING_NODE12_URN);
    checkIfNodeNotExist(FILTERING_NODE13_URN);
    checkIfNodeNotExist(FILTERING_NODE14_URN);
    checkIfNodeNotExist(FILTERING_NODE15_URN);
    checkIfNodeNotExist(FILTERING_NODE16_URN);
    checkIfNodeExist(FILTERING_NODE17_URN);
    checkIfNodeExist(FILTERING_NODE18_URN);
    checkIfNodeExist(FILTERING_NODE19_URN);
    checkIfNodeExist(FILTERING_NODE20_URN);
    // Show all
    showAll(FILTERING_NODE7_URN, "down");
    checkIfNodeExist(FILTERING_NODE8_URN);
    checkIfNodeExist(FILTERING_NODE9_URN);
    checkIfNodeExist(FILTERING_NODE10_URN);
    checkIfNodeExist(FILTERING_NODE11_URN);
    checkIfNodeExist(FILTERING_NODE12_URN);
    checkIfNodeExist(FILTERING_NODE13_URN);
    checkIfNodeExist(FILTERING_NODE14_URN);
    checkIfNodeExist(FILTERING_NODE15_URN);
    checkIfNodeExist(FILTERING_NODE16_URN);
    checkIfNodeExist(FILTERING_NODE17_URN);
    checkIfNodeExist(FILTERING_NODE18_URN);
    checkIfNodeExist(FILTERING_NODE19_URN);
    checkIfNodeExist(FILTERING_NODE20_URN);
    // Filtering (less than 3 characters)
    filter(FILTERING_NODE7_URN, "up", "xx");
    checkIfNodeExist(FILTERING_NODE8_URN);
    checkIfNodeExist(FILTERING_NODE9_URN);
    checkIfNodeExist(FILTERING_NODE10_URN);
    checkIfNodeExist(FILTERING_NODE11_URN);
    checkIfNodeExist(FILTERING_NODE12_URN);
    checkIfNodeExist(FILTERING_NODE13_URN);
    checkIfNodeExist(FILTERING_NODE14_URN);
    checkIfNodeExist(FILTERING_NODE15_URN);
    checkIfNodeExist(FILTERING_NODE16_URN);
    checkIfNodeExist(FILTERING_NODE17_URN);
    checkIfNodeExist(FILTERING_NODE18_URN);
    checkIfNodeExist(FILTERING_NODE19_URN);
    checkIfNodeExist(FILTERING_NODE20_URN);
    // Filtering (single node)
    filter(FILTERING_NODE7_URN, "down", "node13");
    ensureTitleHasText(FILTERING_NODE7_URN, "down", "1 of 13 shown");
    checkMatches(FILTERING_NODE7_URN, "down", "1");
    checkIfNodeNotExist(FILTERING_NODE8_URN);
    checkIfNodeNotExist(FILTERING_NODE9_URN);
    checkIfNodeNotExist(FILTERING_NODE10_URN);
    checkIfNodeNotExist(FILTERING_NODE11_URN);
    checkIfNodeNotExist(FILTERING_NODE12_URN);
    checkIfNodeExist(FILTERING_NODE13_URN);
    checkIfNodeNotExist(FILTERING_NODE14_URN);
    checkIfNodeNotExist(FILTERING_NODE15_URN);
    checkIfNodeNotExist(FILTERING_NODE16_URN);
    checkIfNodeNotExist(FILTERING_NODE17_URN);
    checkIfNodeNotExist(FILTERING_NODE18_URN);
    checkIfNodeNotExist(FILTERING_NODE19_URN);
    checkIfNodeNotExist(FILTERING_NODE20_URN);
    // Filtering (multiple nodes)
    filter(FILTERING_NODE7_URN, "down", "node");
    ensureTitleHasText(FILTERING_NODE7_URN, "down", "13 of 13 shown");
    checkMatches(FILTERING_NODE7_URN, "downup", "13");
    checkIfNodeExist(FILTERING_NODE8_URN);
    checkIfNodeExist(FILTERING_NODE9_URN);
    checkIfNodeExist(FILTERING_NODE10_URN);
    checkIfNodeExist(FILTERING_NODE11_URN);
    checkIfNodeExist(FILTERING_NODE12_URN);
    checkIfNodeExist(FILTERING_NODE13_URN);
    checkIfNodeExist(FILTERING_NODE14_URN);
    checkIfNodeExist(FILTERING_NODE15_URN);
    checkIfNodeExist(FILTERING_NODE16_URN);
    checkIfNodeExist(FILTERING_NODE17_URN);
    checkIfNodeExist(FILTERING_NODE18_URN);
    checkIfNodeExist(FILTERING_NODE19_URN);
    checkIfNodeExist(FILTERING_NODE20_URN);
    // Filtering (no any nodes)
    filter(FILTERING_NODE7_URN, "down", "unknown");
    ensureTitleHasText(FILTERING_NODE7_URN, "down", "0 of 13 shown");
    checkMatches(FILTERING_NODE7_URN, "down", "0");
    checkIfNodeNotExist(FILTERING_NODE8_URN);
    checkIfNodeNotExist(FILTERING_NODE9_URN);
    checkIfNodeNotExist(FILTERING_NODE10_URN);
    checkIfNodeNotExist(FILTERING_NODE11_URN);
    checkIfNodeNotExist(FILTERING_NODE12_URN);
    checkIfNodeNotExist(FILTERING_NODE13_URN);
    checkIfNodeNotExist(FILTERING_NODE14_URN);
    checkIfNodeNotExist(FILTERING_NODE15_URN);
    checkIfNodeNotExist(FILTERING_NODE16_URN);
    checkIfNodeNotExist(FILTERING_NODE17_URN);
    checkIfNodeNotExist(FILTERING_NODE18_URN);
    checkIfNodeNotExist(FILTERING_NODE19_URN);
    checkIfNodeNotExist(FILTERING_NODE20_URN);
    // Filtering (platform)
    filter(FILTERING_NODE7_URN, "down", "postgres");
    ensureTitleHasText(FILTERING_NODE7_URN, "down", "3 of 13 shown");
    checkMatches(FILTERING_NODE7_URN, "down", "3");
    checkIfNodeExist(FILTERING_NODE8_URN);
    checkIfNodeExist(FILTERING_NODE9_URN);
    checkIfNodeExist(FILTERING_NODE10_URN);
    checkIfNodeNotExist(FILTERING_NODE11_URN);
    checkIfNodeNotExist(FILTERING_NODE12_URN);
    checkIfNodeNotExist(FILTERING_NODE13_URN);
    checkIfNodeNotExist(FILTERING_NODE14_URN);
    checkIfNodeNotExist(FILTERING_NODE15_URN);
    checkIfNodeNotExist(FILTERING_NODE16_URN);
    checkIfNodeNotExist(FILTERING_NODE17_URN);
    checkIfNodeNotExist(FILTERING_NODE18_URN);
    checkIfNodeNotExist(FILTERING_NODE19_URN);
    checkIfNodeNotExist(FILTERING_NODE20_URN);
    filter(FILTERING_NODE7_URN, "down", "snowflake");
    ensureTitleHasText(FILTERING_NODE7_URN, "down", "10 of 13 shown");
    checkMatches(FILTERING_NODE7_URN, "down", "10");
    checkIfNodeNotExist(FILTERING_NODE8_URN);
    checkIfNodeNotExist(FILTERING_NODE9_URN);
    checkIfNodeNotExist(FILTERING_NODE10_URN);
    checkIfNodeExist(FILTERING_NODE11_URN);
    checkIfNodeExist(FILTERING_NODE12_URN);
    checkIfNodeExist(FILTERING_NODE13_URN);
    checkIfNodeExist(FILTERING_NODE14_URN);
    checkIfNodeExist(FILTERING_NODE15_URN);
    checkIfNodeExist(FILTERING_NODE16_URN);
    checkIfNodeExist(FILTERING_NODE17_URN);
    checkIfNodeExist(FILTERING_NODE18_URN);
    checkIfNodeExist(FILTERING_NODE19_URN);
    checkIfNodeExist(FILTERING_NODE20_URN);

    cy.goToEntityLineageGraphV2(DATASET_ENTITY_TYPE, FILTERING_NODE1_URN);
    checkIfNodeExist(FILTERING_NODE1_URN);
    checkIfNodeExist(FILTERING_NODE7_URN);
    checkIfEdgeExist(FILTERING_NODE1_URN, FILTERING_NODE7_URN);
    expandOne(FILTERING_NODE7_URN);
    checkFilteringNodeExist(FILTERING_NODE7_URN, "down");
    ensureTitleHasText(FILTERING_NODE7_URN, "down", "4 of 13 shown");
    checkCounter(FILTERING_NODE7_URN, "down", "platform", "PostgreSQL", "3");
    checkCounter(FILTERING_NODE7_URN, "down", "platform", "Snowflake", "10");
    checkCounter(FILTERING_NODE7_URN, "down", "subtype", "Datasets", "13");
    checkIfNodeNotExist(FILTERING_NODE8_URN);
    checkIfNodeNotExist(FILTERING_NODE9_URN);
    checkIfNodeNotExist(FILTERING_NODE10_URN);
    checkIfNodeNotExist(FILTERING_NODE11_URN);
    checkIfNodeNotExist(FILTERING_NODE12_URN);
    checkIfNodeNotExist(FILTERING_NODE13_URN);
    checkIfNodeNotExist(FILTERING_NODE14_URN);
    checkIfNodeNotExist(FILTERING_NODE15_URN);
    checkIfNodeNotExist(FILTERING_NODE16_URN);
    checkIfNodeExist(FILTERING_NODE17_URN);
    checkIfNodeExist(FILTERING_NODE18_URN);
    checkIfNodeExist(FILTERING_NODE19_URN);
    checkIfNodeExist(FILTERING_NODE20_URN);
    // Filtering (no any nodes but filtering node should exist)
    filter(FILTERING_NODE7_URN, "down", "unknown");
    ensureTitleHasText(FILTERING_NODE7_URN, "down", "0 of 13 shown");
    checkMatches(FILTERING_NODE7_URN, "down", "0");
    checkIfNodeNotExist(FILTERING_NODE8_URN);
    checkIfNodeNotExist(FILTERING_NODE9_URN);
    checkIfNodeNotExist(FILTERING_NODE10_URN);
    checkIfNodeNotExist(FILTERING_NODE11_URN);
    checkIfNodeNotExist(FILTERING_NODE12_URN);
    checkIfNodeNotExist(FILTERING_NODE13_URN);
    checkIfNodeNotExist(FILTERING_NODE14_URN);
    checkIfNodeNotExist(FILTERING_NODE15_URN);
    checkIfNodeNotExist(FILTERING_NODE16_URN);
    checkIfNodeNotExist(FILTERING_NODE17_URN);
    checkIfNodeNotExist(FILTERING_NODE18_URN);
    checkIfNodeNotExist(FILTERING_NODE19_URN);
    checkIfNodeNotExist(FILTERING_NODE20_URN);
    // Clear filters (nodes should still be expanded)
    clearFilter(FILTERING_NODE7_URN, "down");
    checkFilteringNodeExist(FILTERING_NODE7_URN, "down");
    ensureTitleHasText(FILTERING_NODE7_URN, "down", "4 of 13 shown");
    checkCounter(FILTERING_NODE7_URN, "down", "platform", "PostgreSQL", "3");
    checkCounter(FILTERING_NODE7_URN, "down", "platform", "Snowflake", "10");
    checkCounter(FILTERING_NODE7_URN, "down", "subtype", "Datasets", "13");
    checkIfNodeNotExist(FILTERING_NODE8_URN);
    checkIfNodeNotExist(FILTERING_NODE9_URN);
    checkIfNodeNotExist(FILTERING_NODE10_URN);
    checkIfNodeNotExist(FILTERING_NODE11_URN);
    checkIfNodeNotExist(FILTERING_NODE12_URN);
    checkIfNodeNotExist(FILTERING_NODE13_URN);
    checkIfNodeNotExist(FILTERING_NODE14_URN);
    checkIfNodeNotExist(FILTERING_NODE15_URN);
    checkIfNodeNotExist(FILTERING_NODE16_URN);
    checkIfNodeExist(FILTERING_NODE17_URN);
    checkIfNodeExist(FILTERING_NODE18_URN);
    checkIfNodeExist(FILTERING_NODE19_URN);
    checkIfNodeExist(FILTERING_NODE20_URN);
  });
});
