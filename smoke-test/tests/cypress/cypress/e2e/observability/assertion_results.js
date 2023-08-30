const urn = "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsAssertionDataset,PROD)";
const datasetName = "SampleCypressHdfsAssertionDataset";

describe("dataset assertion results test", () => {
    it("go to dataset with assertions and verify assertion results", () => {
        //View assertions list
        cy.login();
        cy.goToDataset(urn, datasetName);
        cy.clickOptionWithText("Validation");
        cy.waitTextVisible("All assertions are passing");
        //Click on single assertion to open it up
        cy.clickOptionWithText("External");
        cy.waitTextVisible("Passed");
        //View the results graph
        cy.clickOptionWithText("Passed");
        cy.waitTextVisible("Evaluations");
        cy.waitTextVisible("Last evaluated on");
        //Delete an assertion
        cy.get("[aria-label='more']").eq(1).click();
        cy.clickOptionWithText("Delete");
        cy.waitTextVisible("Confirm Assertion Removal");
        cy.clickOptionWithText("Yes");
        cy.waitTextVisible("Removed assertion.");
        cy.ensureTextNotPresent("All assertions are passing");
        cy.contains("No assertions have run");
        });
});