const urn =
  "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsAssertionDataset,PROD)";
const datasetName = "SampleCypressHdfsAssertionDataset";

describe("dataset assertion results test", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });

  it.skip("go to dataset with assertions and verify assertion results", () => {
    // View assertions list
    cy.login();
    cy.goToDataset(urn, datasetName);
    cy.clickOptionWithText("Quality");
    cy.waitTextVisible("All assertions are passing");
    cy.waitTextVisible("Other");
    // Click on single assertion to open it up
    cy.clickOptionWithSpecificClass(".assertion-result-dot", 0);
    cy.waitTextVisible("Activity");
    cy.waitTextVisible("Last evaluated");
    cy.waitTextVisible("Last evaluated on");
  });
});
