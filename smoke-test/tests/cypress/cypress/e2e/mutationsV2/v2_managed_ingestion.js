function clearMonacoEditor() {
  const selectAllKey = Cypress.platform === "darwin" ? "{cmd}a" : "{ctrl}a";
  return cy
    .get(".monaco-scrollable-element")
    .first()
    .click()
    .focused()
    .type(selectAllKey)
    .type("{backspace}");
}

function typeInMonacoEditor(text) {
  return cy
    .get(".monaco-scrollable-element")
    .first()
    .click()
    .focused()
    .type(text);
}

describe("run managed ingestion", () => {
  beforeEach(() => {
    cy.setFeatureFlags(true, (res) => {
      res.body.data.appConfig.featureFlags.showIngestionPageRedesign = false;
    });
  });

  it("create run managed ingestion source", () => {
    const number = Math.floor(Math.random() * 100000);
    const testName = `cypress test source ${number}`;
    const cli_version = "0.12.0";

    cy.login();
    cy.goToIngestionPage();
    cy.contains("Loading ingestion sources...").should("not.exist");
    cy.clickOptionWithTestId("create-ingestion-source-button");
    cy.get('[placeholder="Search data sources..."]').type("other");
    cy.clickOptionWithTextToScrollintoView("Other");

    cy.waitTextVisible("source-type");

    // Clear the editor first
    clearMonacoEditor();

    // Type your content
    typeInMonacoEditor("source:{enter}");
    typeInMonacoEditor("    type: demo-data{enter}");
    typeInMonacoEditor("config: {}");

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
        cy.clickOptionWithTestId(`delete-ingestion-source-${testName}`);
      });
    cy.get(`[data-testid="confirm-delete-ingestion-source"]`).click();
    cy.ensureTextNotPresent(testName);
  });
});
