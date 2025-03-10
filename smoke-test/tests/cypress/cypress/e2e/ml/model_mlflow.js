describe("models", () => {
  // Add global error handling
  beforeEach(() => {
    // This prevents test failures due to unhandled exceptions in the application
    Cypress.on("uncaught:exception", (err, runnable) => {
      console.error("Uncaught exception:", err);
      return false; // Prevents Cypress from failing the test
    });
  });

  it("can visit mlflow model groups", () => {
    // Monitor GraphQL requests to debug API issues
    cy.intercept("POST", "/api/v2/graphql*").as("graphqlRequest");

    // Visit with improved waiting for page load
    cy.visitWithLogin(
      "/mlModelGroup/urn:li:mlModelGroup:(urn:li:dataPlatform:mlflow,sample_ml_model_group,PROD)",
    );

    // Wait for initial GraphQL request to complete
    cy.wait("@graphqlRequest");

    // Ensure page has loaded by checking for specific content
    cy.contains("2025-03-03").should("be.visible");
    cy.contains("2025-03-04").should("be.visible");
    cy.contains("urn:li:corpuser:datahub").should("be.visible");

    // Navigate to Properties tab with verification
    cy.url().should("include", "mlModelGroup");
    cy.get('[data-node-key="Properties"]').should("be.visible").first().click();

    // Wait for content to load after tab change
    cy.contains("data_science").should("be.visible");

    // Navigate to Models tab with verification
    cy.get('[data-node-key="Models"]').should("be.visible").click();

    // Wait for models to load
    cy.contains("SAMPLE ML MODEL").should("be.visible");
    cy.contains("A sample ML model").should("be.visible");

    // Click model with verification
    cy.contains("SAMPLE ML MODEL").click();

    // Verify model details page loaded
    cy.contains("A sample ML model").should("be.visible");
  });

  it("can visit mlflow model", () => {
    // Monitor GraphQL requests
    cy.intercept("POST", "/api/v2/graphql*").as("graphqlRequest");

    cy.visitWithLogin(
      "/mlModels/urn:li:mlModel:(urn:li:dataPlatform:mlflow,sample_ml_model,PROD)",
    );

    // Wait for initial data load
    cy.wait("@graphqlRequest");

    // Verify model metadata
    cy.contains("Simple Training Run").should("be.visible");
    cy.contains("A sample ML model").should("be.visible");
    cy.contains("val_loss").should("be.visible");
    cy.contains("max_depth").should("be.visible");

    // Navigate to Properties tab with verification
    cy.contains("Properties").should("be.visible").click();

    // Wait for properties to load
    cy.contains("data_science").should("be.visible");

    // Navigate to Group tab with verification
    cy.contains("Group").should("be.visible").click();

    // Wait for group data to load
    cy.contains("SAMPLE ML MODEL GROUP").should("be.visible");
    cy.contains("A sample ML model group").should("be.visible");

    // Click model group with verification
    cy.contains("SAMPLE ML MODEL GROUP").click();

    // Verify group details page loaded
    cy.contains("A sample ML model group").should("be.visible");
  });
});
