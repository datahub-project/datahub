const urn =
  "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsAssertionDataset,PROD)";
const datasetName = "SampleCypressHdfsAssertionDataset";

describe("dataset assertion results test", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });

  it("go to dataset with assertions and verify assertion results", () => {
    //View assertions list
    cy.login();
    cy.goToDataset(urn, datasetName);
    cy.clickOptionWithText("Quality");
    cy.waitTextVisible("All assertions are passing");
    //Click on single assertion to open it up
    cy.clickOptionWithSpecificClass(".ant-table-expanded-row", 0);
    cy.waitTextVisible("Passing");
    //View the results graph
    cy.clickOptionWithText("Passing");
    cy.waitTextVisible("Last evaluated");
    cy.waitTextVisible("Last evaluated on");
  });
});
