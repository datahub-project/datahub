function readyToTypeEditor() {
    return cy.get('.monaco-editor textarea:first')
    .click().focused();
}

describe("run managed ingestion", () => {
    it("create run managed ingestion source", () => {
        let number = Math.floor(Math.random() * 100000);
        let testName = `cypress test source ${number}`
        let cli_version = "0.9.3.3rc5";
        cy.login();
        cy.goToIngestionPage();
        cy.clickOptionWithText("Create new source");
        cy.clickOptionWithText("Other");

        cy.waitTextVisible("source-type");
        readyToTypeEditor().type('{ctrl}a').clear()
        readyToTypeEditor().type("source:");
        readyToTypeEditor().type("{enter}");
        readyToTypeEditor().type("    type: demo-data");
        readyToTypeEditor().type("{enter}");
        // no space because the editor starts new line at same indentation
        readyToTypeEditor().type("config: {}");
        cy.clickOptionWithText("Next")
        cy.clickOptionWithText("Next")

        cy.enterTextInTestId('source-name-input', testName)
        cy.clickOptionWithText("Advanced")
        cy.enterTextInTestId('cli-version-input', cli_version)
        cy.clickOptionWithText("Save & Run")
        cy.waitTextVisible(testName)

        cy.contains(testName).parent().within(() => {
            cy.contains("Succeeded", {timeout: 30000})
            cy.clickOptionWithTestId("delete-button");
        })
        cy.clickOptionWithText("Yes")
        cy.ensureTextNotPresent(testName)
    })
});