import { getTimestampMillisNumDaysAgo } from "../../support/commands";

const DATASET_ENTITY_TYPE = "dataset";
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

describe("lineage_graph", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    const resizeObserverLoopErrRe = "ResizeObserver loop limit exceeded";
    cy.on("uncaught:exception", (err) => {
      if (err.message.includes(resizeObserverLoopErrRe)) {
        return false;
      }
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
});
