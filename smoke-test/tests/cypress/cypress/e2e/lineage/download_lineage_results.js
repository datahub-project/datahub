const test_dataset =
  "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)";
const first_degree = [
  "urn:li:chart:(looker,cypress_baz1)",
  "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)",
  "urn:li:mlFeature:(cypress-test-2,some-cypress-feature-1)",
];
const second_degree = [
  "urn:li:chart:(looker,cypress_baz2)",
  "urn:li:dashboard:(looker,cypress_baz)",
  "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)",
  "urn:li:mlPrimaryKey:(cypress-test-2,some-cypress-feature-2)",
  "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,cypress-model,PROD)",
];
const third_degree_plus = [
  "urn:li:dataJob:(urn:li:dataFlow:(airflow,cypress_dag_abc,PROD),cypress_task_123)",
  "urn:li:dataJob:(urn:li:dataFlow:(airflow,cypress_dag_abc,PROD),cypress_task_456)",
  "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)",
  "urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_created,PROD)",
  "urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_created_no_tag,PROD)",
  "urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_deleted,PROD)",
  "urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,cypress-model-package-group,PROD)",
];
const downloadCsvFile = (filename) => {
  cy.get('[data-testid="three-dot-menu"]').click();
  cy.get('[data-testid="download-as-csv-menu-item"]').click();
  cy.get('[data-testid="download-as-csv-input"]').clear().type(filename);
  cy.get('[data-testid="csv-modal-download-button"]').click().wait(5000);
  cy.ensureTextNotPresent("Creating CSV to download");
};

describe("download lineage results to .csv file", () => {
  beforeEach(() => {
    cy.on("uncaught:exception", (err, runnable) => false);
  });

  it("download and verify lineage results for 1st, 2nd and 3+ degree of dependencies", () => {
    cy.loginWithCredentials();
    cy.goToDataset(test_dataset, "SampleCypressKafkaDataset");
    cy.openEntityTab("Lineage");

    // Verify 1st degree of dependencies
    cy.contains(/1 - [3-4] of [3-4]/);
    downloadCsvFile("first_degree_results.csv");
    const first_degree_csv = cy.readFile(
      "cypress/downloads/first_degree_results.csv",
    );
    first_degree.forEach((urn) => {
      first_degree_csv.should("contain", urn);
    });
    second_degree.forEach((urn) => {
      first_degree_csv.should("not.contain", urn);
    });
    third_degree_plus.forEach((urn) => {
      first_degree_csv.should("not.contain", urn);
    });

    // Verify 1st and 2nd degree of dependencies
    cy.get('[data-testid="facet-degree-2"]').click().wait(5000);
    cy.contains(/1 - [8-9] of [8-9]/);
    downloadCsvFile("second_degree_results.csv");
    const second_degree_csv = cy.readFile(
      "cypress/downloads/second_degree_results.csv",
    );
    first_degree.forEach((urn) => {
      second_degree_csv.should("contain", urn);
    });
    second_degree.forEach((urn) => {
      second_degree_csv.should("contain", urn);
    });
    third_degree_plus.forEach((urn) => {
      second_degree_csv.should("not.contain", urn);
    });

    // Verify 1st 2nd and 3+ degree of dependencies(Verify multi page download)
    cy.get('[data-testid="facet-degree-3+"]').click().wait(5000);
    cy.contains(/1 - 10 of 1[3-6]/);
    downloadCsvFile("third_plus_degree_results.csv");
    const third_degree_csv = cy.readFile(
      "cypress/downloads/third_plus_degree_results.csv",
    );
    first_degree.forEach((urn) => {
      third_degree_csv.should("contain", urn);
    });
    second_degree.forEach((urn) => {
      third_degree_csv.should("contain", urn);
    });
    third_degree_plus.forEach((urn) => {
      third_degree_csv.should("contain", urn);
    });
  });
});
