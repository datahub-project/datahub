const urn =
  "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsAssertionDataset,PROD)";
const datasetName = "SampleCypressHdfsAssertionDataset";

describe("dataset assertion results test", () => {
  it("go to dataset with assertions and verify assertion results", () => {
    // View assertions list
    cy.login();
    cy.goToDataset(urn, datasetName);
    cy.clickOptionWithText("Quality");
    cy.waitTextVisible("All assertions are passing");
    // Assertion categories be expanded by default
    cy.waitTextVisible("Other");
    cy.ensureElementPresent(".assertion-result-dot");
    // View the results graph
    cy.clickOptionWithSpecificClass(".assertion-result-dot", 0);
    cy.waitTextVisible("Activity");
    cy.waitTextVisible("Last evaluated on");
    // Delete an assertion
    cy.get('[aria-label="delete"]').eq(1).click();
    cy.waitTextVisible("Confirm Assertion Removal");
    cy.clickOptionWithText("Yes");
    cy.waitTextVisible("Removed assertion.");
    cy.ensureTextNotPresent("All assertions are passing");
    cy.contains("No assertions have run");
  });
});
