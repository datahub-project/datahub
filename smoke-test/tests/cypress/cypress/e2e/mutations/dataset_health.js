const urn =
  "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_health_test,PROD)";
const datasetName = "cypress_health_test";

describe("dataset health test", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(false);
    cy.login();
  });

  it("go to dataset with failing assertions and active incidents and verify health of dataset", () => {
    cy.goToDataset(urn, datasetName);
    // Ensure that the “Health” badge is present and there is an active incident warning
    cy.get(`[data-testid="${urn}-health-icon"]`).should("be.visible");
    cy.get(`[data-testid="${urn}-health-icon"]`).trigger("mouseover", {
      force: true,
    });
    cy.waitTextVisible("This asset may be unhealthy");
    cy.waitTextVisible("assertions are failing");
    cy.get('[data-testid="assertions-details"]').click();
    cy.waitTextVisible("This asset may be unhealthy");
  });
});

describe("dataset health test V2", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.login();
  });

  it("go to dataset with failing assertions and active incidents and verify health of dataset", () => {
    cy.goToDataset(urn, datasetName);
    // Ensure that the “Health” badge is present and there is an active incident warning
    cy.get(`[data-testid="${urn}-health-icon"]`).should("be.visible");
    cy.get(`[data-testid="${urn}-health-icon"]`).first().trigger("mouseover", {
      force: true,
    });
    cy.waitTextVisible("1 active incident");
    cy.waitTextVisible("assertions are failing");
  });
});
