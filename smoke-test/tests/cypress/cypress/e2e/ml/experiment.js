describe("experiment", () => {
  beforeEach(() => {
    cy.visit("/");
    cy.login();
  });

  it("can visit experiment end run", () => {
    // Then visit the specific page
    cy.visit(
      "/container/urn:li:container:airline_forecast_experiment/Summary?is_lineage_mode=false",
    );

    cy.contains("Airline Forecast Experiment");
    cy.contains("Experiment to forecast airline passenger numbers");

    // the model has a training run
    cy.contains("Simple Training Run").click();
    cy.contains("Airline Forecast Experiment");
  });

  it("can visit container and run", () => {
    cy.visit("/");
    cy.login();
    cy.visit(
      "/dataProcessInstance/urn:li:dataProcessInstance:simple_training_run",
    );

    // the run has subtype, na
    cy.contains("Simple Training Run");

    // the run has its details
    cy.contains("Failure");
    cy.contains("1 sec");
    cy.contains("simple_training_run");
    cy.contains("urn:li:corpuser:datahub");
    cy.contains("s3://my-bucket/output");

    // the run has its metrics and parameters
    cy.contains("accuracy");
    cy.contains("learning_rate");

    // the run has a container and can visit it
    cy.contains("Airline Forecast Experiment").click();
    cy.contains("Simple Training Run");
  });
});
