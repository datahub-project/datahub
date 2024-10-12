describe("features", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });
  it("can visit feature tables and see features", () => {
    cy.visit("/");
    cy.login();
    cy.visit(
      "/featureTables/urn:li:mlFeatureTable:(urn:li:dataPlatform:sagemaker,cypress-feature-table)/Features?is_lineage_mode=false",
    );

    // the feature table descriptions should be there
    cy.contains("Yet another test feature group");
    cy.contains("this is a description from source system");

    // additional properties are visible
    cy.contains("CypressPrimaryKeyTag");
    cy.contains("CypressFeatureTag");

    // navigate to sources
    cy.contains("Sources").click(); //datahub-fork/smoke-test/tests/cypress/cypress/e2e/containers/containers.js

    // feature & primary key sources are visible
    cy.contains("SampleCypressHdfsDataset");
    cy.contains("SampleCypressKafkaDataset");

    // navigate to properties
    cy.get('[data-node-key="Properties"]').first().click();
    cy.contains("Properties").click();

    // custom properties are visible
    cy.contains("status");
    cy.contains("Created");
  });

  it("can visit feature page", () => {
    cy.visit("/");
    cy.login();
    cy.visit(
      "/features/urn:li:mlFeature:(cypress-test-2,some-cypress-feature-1)/Feature%20Tables?is_lineage_mode=false",
    );
    cy.contains("Feature Tables").should("exist");
  });

  it("can visit primary key page", () => {
    cy.visit("/");
    cy.login();
    cy.visit(
      "/mlPrimaryKeys/urn:li:mlPrimaryKey:(cypress-test-2,some-cypress-feature-2)/Feature%20Tables?is_lineage_mode=false",
    );
    cy.contains("Feature Tables").should("exist");
  });
});
