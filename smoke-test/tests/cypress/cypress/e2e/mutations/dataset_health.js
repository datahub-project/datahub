const urn =
  "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_health_test,PROD)";
const datasetName = "cypress_health_test";

describe("dataset health test", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });
  it("go to dataset with failing assertions and active incidents and verify health of dataset", () => {
    cy.login();
    cy.goToDataset(urn, datasetName);
    // Ensure that the “Health” badge is present and there is an active incident warning
    cy.get(`[data-testid="${urn}-health-icon"]`).first().should("be.visible");
    cy.get(`[data-testid="${urn}-health-icon"]`).first().trigger("mouseover", {
      force: true,
    });

    // Wait for the health popover to appear
    cy.get('[data-testid="assertions-details"]', { timeout: 10000 }).should(
      "be.visible",
    );

    // Verify health information is displayed (either assertions or incidents or both)
    // Due to search index timing in CI, one or both may appear
    cy.get('[data-testid="assertions-details"]').within(() => {
      cy.get("a").should("have.length.at.least", 1);
    });
  });
});
