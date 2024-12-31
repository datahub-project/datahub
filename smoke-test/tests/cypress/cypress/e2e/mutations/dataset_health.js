const urn =
  "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_health_test,PROD)";
const datasetName = "cypress_health_test";

describe("dataset health test", () => {
  it("go to dataset with failing assertions and active incidents and verify health of dataset", () => {
    cy.login();
    cy.goToDataset(urn, datasetName);
    // Ensure that the “Health” badge is present and there is an active incident warning
    cy.get(`[href="/dataset/${urn}/Quality"]`).should("be.visible");
    cy.get(`[href="/dataset/${urn}/Quality"] span`).trigger("mouseover", {
      force: true,
    });
    cy.waitTextVisible("This asset may be unhealthy");
    cy.waitTextVisible("Assertions 1 of 1 assertions are failing");
    cy.waitTextVisible("1 active incident");
  });
});
