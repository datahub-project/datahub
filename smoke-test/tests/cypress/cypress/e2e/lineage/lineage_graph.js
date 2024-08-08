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
  it("can see full history", () => {
    cy.login();
    cy.goToEntityLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);

    cy.contains("SampleCypressKafka");
    cy.contains("SampleCypressHdfs");
    cy.contains("Baz Chart 1");
    cy.contains("some-cypress");
  });

  it("cannot see any lineage edges for 2021", () => {
    cy.login();
    cy.goToEntityLineageGraph(
      DATASET_ENTITY_TYPE,
      DATASET_URN,
      JAN_1_2021_TIMESTAMP,
      JAN_1_2022_TIMESTAMP,
    );

    cy.contains("SampleCypressKafka");
    cy.contains("SampleCypressHdfs").should("not.exist");
    cy.contains("Baz Chart 1").should("not.exist");
    cy.contains("some-cypress").should("not.exist");
  });

  it("can see when the inputs to a data job change", () => {
    cy.login();
    // Between 14 days ago and 7 days ago, only transactions was an input
    cy.goToEntityLineageGraph(
      TASKS_ENTITY_TYPE,
      TRANSACTION_ETL_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
    );
    cy.contains("transaction_etl");
    cy.contains("aggregated");
    cy.contains("transactions");
    cy.contains("user_profile").should("not.exist");
    // 1 day ago, user_profile was also added as an input
    cy.goToEntityLineageGraph(
      TASKS_ENTITY_TYPE,
      TRANSACTION_ETL_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    cy.contains("transaction_etl");
    cy.contains("aggregated");
    cy.contains("transactions");
    cy.contains("user_profile");
  });

  it("can see when a data job is replaced", () => {
    cy.login();
    // Between 14 days ago and 7 days ago, only temperature_etl_1 was an iput
    cy.goToEntityLineageGraph(
      DATASET_ENTITY_TYPE,
      MONTHLY_TEMPERATURE_DATASET_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
    );
    cy.contains("monthly_temperature");
    cy.contains("temperature_etl_1");
    cy.contains("temperature_etl_2").should("not.exist");
    // Since 7 days ago, temperature_etl_1 has been replaced by temperature_etl_2
    cy.goToEntityLineageGraph(
      DATASET_ENTITY_TYPE,
      MONTHLY_TEMPERATURE_DATASET_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    cy.contains("monthly_temperature");
    cy.contains("temperature_etl_1").should("not.exist");
    cy.contains("temperature_etl_2");
  });

  it("can see when a dataset join changes", () => {
    cy.login();
    // 8 days ago, both gdp and factor_income were joined to create gnp
    cy.goToEntityLineageGraph(
      DATASET_ENTITY_TYPE,
      GNP_DATASET_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    cy.contains("gnp");
    cy.contains("gdp");
    cy.contains("factor_income");
    // 1 day ago, factor_income was removed from the join
    cy.goToEntityLineageGraph(
      DATASET_ENTITY_TYPE,
      GNP_DATASET_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    cy.contains("gnp");
    cy.contains("gdp");
    cy.contains("factor_income").should("not.exist");
  });
});
