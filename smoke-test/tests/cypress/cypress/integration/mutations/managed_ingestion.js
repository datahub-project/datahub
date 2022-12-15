let testName = "cypress test source"

function readyToTypeEditor() {
    return cy.get('.monaco-editor textarea:first')
    .click().focused();
}

describe("run managed ingestion", () => {
    it("create run managed ingestion source", () => {
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

        cy.clickOptionWithText("Skip")

        cy.enterTextInTestId('source-name-input', testName)
        cy.clickOptionWithText("Advanced")
        cy.enterTextInTestId('cli-version-input', "0.9.3.3rc5")
        cy.clickOptionWithText("Save & Run")
        cy.waitTextVisible(testName)

        // managed ingestion takes few seconds
        cy.wait(10000)
        cy.waitTextVisible("Succeeded")

        cy.get(".delete-button").click()
        cy.clickOptionWithText("Yes")
        cy.ensureTextNotPresent(testName)
    })
});