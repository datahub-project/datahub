describe("experiment", () => {
  it("can visit experiment end run", () => {
    cy.visit("/");
    cy.login();
//    replace the following line with the correct URL
    cy.visit(
      "/container/urn:li:container:airline_forecast_experiment/Summary?is_lineage_mode=false",
    );

    // the experiment has subtypes and platform
    cy.contains("ML Experiment");
    cy.contains("MLflow");
    // the model has its name and description
    cy.contains("Airline Forecast Experiment");
    cy.contains("Experiment to forecast airline passenger numbers")

    // the model has a training run
    cy.contains("Simple Training Run").click();
    cy.contains("ML Training Run");
    cy.contains("Airline Forecast Experiment");
  });

  it("can visit container and run", () => {
    cy.visit("/");
    cy.login();
    cy.visit(
      "/dataProcessInstance/urn:li:dataProcessInstance:simple_training_run",
    );
    // the run has subtype, na
    cy.contains("ML Training Run")
    cy.contains("Simple Training Run");

    // the run has its details
    cy.contains("Failure");
    cy.contains("1 secs"); // TODO: should be 1 sec
    cy.contains("simple_training_run");
    cy.contains("urn:li:corpuser:datahub");
    cy.contains("s3://my-bucket/output")

    // the run has its metrics and parameters
    cy.contains("accuracy");
    cy.contains("learning_rate");

    // the run has a container and can visit it
    cy.contains("Airline Forecast Experiment").click();
    cy.contains("ML Experiment");
    cy.contains("Simple Training Run")
  });
});
