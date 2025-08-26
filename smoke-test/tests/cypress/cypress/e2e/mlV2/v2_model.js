const MODEL_DESCRIPTION = "ml model description";
const PROPERTY_NAME = "EnableNetworkIsolation";

describe("models", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });
  it("can visit models and groups", () => {
    cy.visitWithLogin(
      "/mlModels/urn:li:mlModel:(urn:li:dataPlatform:sagemaker,cypress-model,PROD)/Summary?is_lineage_mode=false",
    );

    // the model has metrics & hyper params
    cy.contains("another-metric");
    cy.contains("parameter-1");

    // check the description on the sidebar
    cy.getWithTestId("sidebar-section-content-Documentation").should(
      "contain",
      MODEL_DESCRIPTION,
    );

    // check the description on the documentation tab
    cy.openEntityTab("Documentation");
    cy.getWithTestId("documentation-tab-content").should(
      "contain",
      MODEL_DESCRIPTION,
    );

    // check the group on the groups tab
    cy.openEntityTab("Group");
    cy.contains("cypress-model-package-group");

    // check features on the features tab
    cy.openEntityTab("Features");
    cy.contains("some-cypress-feature-1");

    // check properties on the properties tab
    // cy.openEntityTab('Properties'); // FYI: conflict with tab on the sidebar
    cy.clickOptionWithTestId("Properties-entity-tab-header");
    cy.contains(PROPERTY_NAME);

    // check properties on the sidebar's properties tab
    cy.clickOptionWithTestId("entity-sidebar-tab-Properties");
    cy.getWithTestId("entity-profile-sidebar-container").should(
      "contain",
      PROPERTY_NAME,
    );
  });

  it("can visit models and groups", () => {
    cy.visitWithLogin(
      "/mlModelGroup/urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,cypress-model-package-group,PROD)",
    );
    // the model group has its model
    cy.contains("cypress-model");
    cy.contains("Just a model package group.");
  });
});
