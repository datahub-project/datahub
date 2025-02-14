import { getTimestampMillisNumDaysAgo } from "../../support/commands";

const JAN_1_2021_TIMESTAMP = 1609553357755;
const JAN_1_2022_TIMESTAMP = 1641089357755;
const DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)";
const TIMESTAMP_MILLIS_14_DAYS_AGO = getTimestampMillisNumDaysAgo(14);
const TIMESTAMP_MILLIS_7_DAYS_AGO = getTimestampMillisNumDaysAgo(7);
const TIMESTAMP_MILLIS_NOW = getTimestampMillisNumDaysAgo(0);
const GNP_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,economic_data.gnp,PROD)";
const TRANSACTION_ETL_URN =
  "urn:li:dataJob:(urn:li:dataFlow:(airflow,bq_etl,prod),transaction_etl)";
const MONTHLY_TEMPERATURE_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.monthly_temperature,PROD)";

const startAtDataSetLineage = () => {
  cy.login();
  cy.goToDataset(DATASET_URN, "SampleCypressKafkaDataset");
  cy.get('[data-node-key="Lineage"]').first().should("be.visible").click();
};

describe("impact analysis", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.on("uncaught:exception", (err, runnable) => false);
  });

  it("can see 1 hop of lineage by default", () => {
    startAtDataSetLineage();

    cy.ensureTextNotPresent("User Creations");
    cy.ensureTextNotPresent("User Deletions");
  });

  it("can see lineage multiple hops away", () => {
    startAtDataSetLineage();
    // click to show more relationships now that we default to 1 degree of dependency
    cy.contains("Impact Analysis").click();
    cy.clickOptionWithText("3+");

    cy.contains("User Creations");
    cy.contains("User Deletions");
  });

  it("can filter the lineage results as well", () => {
    startAtDataSetLineage();
    // click to show more relationships now that we default to 1 degree of dependency
    cy.contains("Impact Analysis").click();
    cy.clickOptionWithText("3+");

    cy.clickOptionWithText("Advanced");

    cy.clickOptionWithText("Add Filter");

    cy.clickOptionWithTestId("adv-search-add-filter-description");

    cy.get('[data-testid="edit-text-input"]').type("fct_users_deleted");

    cy.clickOptionWithTestId("edit-text-done-btn");

    cy.ensureTextNotPresent("User Creations");
    cy.waitTextVisible("User Deletions");
  });

  it("can view column level impact analysis and turn it off", () => {
    cy.login();
    cy.visit(
      `/dataset/${DATASET_URN}/Lineage?column=%5Bversion%3D2.0%5D.%5Btype%3Dboolean%5D.field_bar&is_lineage_mode=false`,
    );

    // impact analysis can take a beat- don't want to time out here
    cy.wait(5000);
    cy.contains("Impact Analysis").click();
    cy.contains("SampleCypressHdfsDataset");
    cy.contains("shipment_info");
    cy.contains("some-cypress-feature-1").should("not.exist");
    cy.contains("Baz Chart 1").should("not.exist");

    // find button to turn off column-level impact analysis
    cy.get('[data-testid="column-lineage-toggle"]').click({ force: true });

    cy.wait(2000);

    cy.contains("SampleCypressHdfsDataset");
    cy.contains("shipment_info").should("not.exist");
    cy.contains("some-cypress-feature-1");
    cy.contains("Baz Chart 1");
  });

  it("can filter lineage edges by time", () => {
    cy.login();
    cy.visit(
      `/dataset/${DATASET_URN}/Lineage?filter_degree___false___EQUAL___0=1&is_lineage_mode=false&page=1&unionType=0&start_time_millis=${JAN_1_2021_TIMESTAMP}&end_time_millis=${JAN_1_2022_TIMESTAMP}`,
    );

    // impact analysis can take a beat- don't want to time out here
    cy.wait(5000);
    cy.contains("Impact Analysis").click();
    cy.contains("SampleCypressHdfsDataset").should("not.exist");
    cy.contains("Downstream column: shipment_info").should("not.exist");
    cy.contains("some-cypress-feature-1").should("not.exist");
    cy.contains("Baz Chart 1").should("not.exist");
  });

  it("can see when the inputs to a data job change", () => {
    cy.login();
    // Between 14 days ago and 7 days ago, only transactions was an input
    cy.visit(
      `/tasks/${TRANSACTION_ETL_URN}/Lineage?filter_degree___false___EQUAL___0=1&is_lineage_mode=false&page=1&unionType=0&start_time_millis=${TIMESTAMP_MILLIS_14_DAYS_AGO}&end_time_millis=${TIMESTAMP_MILLIS_7_DAYS_AGO}`,
    );
    cy.wait(2000);
    cy.get("#entity-sidebar-tabs-tab-Lineage").click();
    // Downstream
    cy.contains("aggregated");
    // Upstream
    cy.clickOptionWithTestId(
      "compact-lineage-tab-direction-select-option-upstream",
    );
    cy.contains("transactions");
    cy.contains("user_profile").should("not.exist");
    // 1 day ago, factor_income was removed from the join
    cy.visit(
      `/tasks/${TRANSACTION_ETL_URN}/Lineage?filter_degree___false___EQUAL___0=1&is_lineage_mode=false&page=1&unionType=0&start_time_millis=${TIMESTAMP_MILLIS_7_DAYS_AGO}&end_time_millis=${TIMESTAMP_MILLIS_NOW}`,
    );
    // Downstream
    cy.wait(2000);
    cy.get("#entity-sidebar-tabs-tab-Lineage").click();
    cy.contains("aggregated");
    // Upstream
    cy.clickOptionWithTestId(
      "compact-lineage-tab-direction-select-option-upstream",
    );
    cy.contains("transactions");
    cy.contains("user_profile");
  });

  it("can see when a data job is replaced", () => {
    cy.login();
    // Between 14 days ago and 7 days ago, only temperature_etl_1 was an iput
    cy.visit(
      `/dataset/${MONTHLY_TEMPERATURE_DATASET_URN}/Lineage?filter_degree___false___EQUAL___0=1&is_lineage_mode=false&page=1&unionType=0&start_time_millis=${TIMESTAMP_MILLIS_14_DAYS_AGO}&end_time_millis=${TIMESTAMP_MILLIS_7_DAYS_AGO}`,
    );
    cy.wait(2000);
    cy.get("#entity-sidebar-tabs-tab-Lineage").click();
    cy.clickOptionWithTestId(
      "compact-lineage-tab-direction-select-option-upstream",
    );
    cy.contains("temperature_etl_1");
    // Since 7 days ago, temperature_etl_1 has been replaced by temperature_etl_2
    cy.visit(
      `/dataset/${MONTHLY_TEMPERATURE_DATASET_URN}/Lineage?filter_degree___false___EQUAL___0=1&is_lineage_mode=false&page=1&unionType=0&start_time_millis=${TIMESTAMP_MILLIS_7_DAYS_AGO}&end_time_millis=${TIMESTAMP_MILLIS_NOW}`,
    );
    cy.wait(2000);
    cy.get("#entity-sidebar-tabs-tab-Lineage").click();
    cy.clickOptionWithTestId(
      "compact-lineage-tab-direction-select-option-upstream",
    );
    cy.contains("temperature_etl_2");
  });
});
