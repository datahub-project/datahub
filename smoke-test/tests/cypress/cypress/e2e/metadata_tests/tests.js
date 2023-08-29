const urn = "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)";
const datasetName = "SampleCypressHdfsDataset";
const testName = "Cypress Tag Test";
const testDescription = "Cyprress test description";

describe("create, edit and remove metadata test", () => {
    beforeEach(() => {
        cy.on('uncaught:exception', (err, runnable) => { return false; });
    });

    it("create new test at governance > tests, test conditions and save the test", () => {
        cy.loginWithCredentials();
        cy.goToTestsList();
        cy.clickOptionWithText("New Test");
        cy.waitTextVisible("New Metadata Test");
        //select data assets
        cy.contains("Datasets, Dashboards, Charts...").prev().click();
        cy.get(".rc-virtual-list").find("div").contains("Datasets").click();
        cy.clickOptionWithText("+ Add");
        cy.clickOptionWithText("Property");
        cy.get('[role="dialog"] [type="search"]').eq(1).click();
        cy.clickOptionWithText("Urn");
        cy.get('[role="dialog"] [type="search"]').eq(2).click();
        cy.clickOptionWithText("Equals");
        cy.get('[role="dialog"] [type="search"]').eq(3).type(urn);
        cy.clickOptionWithText("Next");
        //define conditions
        cy.waitTextVisible("Define your test conditions");
        cy.clickOptionWithText("+ Add");
        cy.clickOptionWithText("Property");
        cy.focused().click();
        cy.clickOptionWithText("Tags");
        cy.get('[role="dialog"] [type="search"]').eq(1).click();
        cy.clickOptionWithText("Contains Any");
        cy.get('[role="dialog"] [type="search"]').eq(2).click().type("Cypress");
        cy.get(".rc-virtual-list").find("div").contains("Cypress").click();
        //test conditions
        cy.clickOptionWithText("Test Conditions");
        cy.get('[role="dialog"] [data-testid="search-input"]').type("hdfs");
        cy.waitTextVisible(datasetName);
        cy.clickOptionWithText("Run Test");
        cy.waitTextVisible("Passed");
        cy.get('[role="dialog"] [data-testid="search-input"]').clear().type("hive");
        cy.waitTextVisible("SampleCypressHiveDataset");
        cy.clickOptionWithText("Run Test");
        cy.waitTextVisible("Not selected");
        cy.clickOptionWithText("Close");
        //finish up
        cy.clickOptionWithText("Next");
        cy.get('[placeholder="A name for your test"]').type(testName);
        cy.get('[placeholder="The description for your test"]').type(testDescription);
        cy.clickOptionWithText("Save");
        cy.waitTextVisible("Successfully created Test!");
        cy.waitTextVisible(testName);
        cy.waitTextVisible(testDescription);
        cy.waitTextVisible("No results found");
    });

    it("edit the test to make it fail, verify the result, save test", () => {
        cy.loginWithCredentials();
        cy.goToTestsList();
        cy.contains(testName).click();
        cy.waitTextVisible("Edit Metadata Test");
        cy.clickOptionWithText("Next");
        cy.get('[role="dialog"] [type="search"]').eq(2).click();
        cy.get(".rc-virtual-list").find("div").contains("Cypress").click();
        cy.get(".rc-virtual-list").find("div").contains("TagToPropose").click();
        //test conditions, verify that test fails
        cy.clickOptionWithText("Test Conditions");
        cy.get('[role="dialog"] [data-testid="search-input"]').type("hdfs");
        cy.waitTextVisible(datasetName);
        cy.clickOptionWithText("Run Test");
        cy.waitTextVisible("Failed");
        cy.clickOptionWithText("Close");
        //save edited test
        cy.clickOptionWithText("Next");
        cy.clickOptionWithText("Save");
        cy.waitTextVisible("Successfully updated Test!");
        cy.waitTextVisible(testName);
        cy.waitTextVisible("No results found");
    });

    it("delete test", () => {
        cy.loginWithCredentials();
        cy.goToTestsList();
        cy.get('[data-testid="test-more-button-0"]').click();
        cy.clickOptionWithText("Delete");
        cy.waitTextVisible("Confirm Test Removal");
        cy.clickOptionWithText("Yes");
        cy.waitTextVisible("Removed test.");
        cy.waitTextVisible("No tests found.");
        cy.ensureTextNotPresent(testName);
    });
});