function readyToTypeEditor() {
  return cy.get(".monaco-editor textarea:first").click().focused();
}

describe("run managed ingestion", () => {
  it("create run managed ingestion source", () => {
    const number = Math.floor(Math.random() * 100000);
    const testName = `cypress test source ${number}`;
    const cli_version = "0.12.0";
    cy.login();
    cy.goToIngestionPage();
    cy.clickOptionWithText("Create new source");
    cy.clickOptionWithTextToScrollintoView("Other");

    cy.waitTextVisible("source-type");
    readyToTypeEditor().type("{ctrl}a").clear();
    readyToTypeEditor().type("source:{enter}");
    readyToTypeEditor().type("    type: demo-data");
    readyToTypeEditor().type("{enter}");
    // no space because the editor starts new line at same indentation
    readyToTypeEditor().type("config: {}");
    cy.clickOptionWithText("Next");
    cy.clickOptionWithText("Next");

    cy.enterTextInTestId("source-name-input", testName);
    cy.clickOptionWithText("Advanced");
    cy.enterTextInTestId("cli-version-input", cli_version);
    cy.clickOptionWithTextToScrollintoView("Save & Run");
    cy.waitTextVisible(testName);

    cy.contains(testName)
      .parent()
      .within(() => {
        cy.contains("Succeeded", { timeout: 180000 });
        cy.clickOptionWithTestId("delete-button");
      });
    cy.clickOptionWithText("Yes");
    cy.ensureTextNotPresent(testName);
  });
});
