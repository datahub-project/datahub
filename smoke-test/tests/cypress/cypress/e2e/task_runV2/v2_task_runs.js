describe("task runs", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });

  it("can visit dataset with runs aspect and verify the task run is present", () => {
    cy.visit("/");
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)/Runs",
    );

    // the run data should not be there since the run wrote
    cy.contains("manual__2022-03-30T11:35:08.970522+00:00");
    cy.contains("Failed");

    // inputs
    cy.contains("SampleCypressHiveDataset");

    // outputs
    cy.contains("fct_cypress_users_created_no_tag");
    cy.contains("cypress_logging_events");

    // task name
    cy.contains("User Creations");
  });

  it("can visit task with runs aspect and verify the task run is present", () => {
    cy.visit("/");
    cy.login();
    cy.visit(
      "tasks/urn:li:dataJob:(urn:li:dataFlow:(airflow,cypress_dag_abc,PROD),cypress_task_123)/Runs",
    );

    // Verify the run data is there
    cy.contains("manual__2022-03-30T11:35:08.970522+00:00");
    cy.contains("Failed");

    // inputs
    cy.contains("fct_cypress_users_created_no_tag");

    // outputs
    cy.contains("SampleCypressHiveDataset");
    cy.contains("cypress_logging_events");
  });
});
