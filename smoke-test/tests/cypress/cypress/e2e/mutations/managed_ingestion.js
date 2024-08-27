// TODO: Investigate why this test can never pass on CI, but passes locally after PR #21465
//
// function readyToTypeEditor() {
//  return cy.get(".monaco-editor textarea:first").click().focused();
// }
//
// describe("run managed ingestion", () => {
//  it("create run managed ingestion source", () => {
//    const number = Math.floor(Math.random() * 100000);
//    const testName = `cypress test source ${number}`;
//    const cli_version = "0.12.0";
//    cy.login();
//    cy.goToIngestionPage();
//    cy.clickOptionWithText("Create new source");
//    cy.clickOptionWithTextToScrollintoView("Other");
//
//    cy.waitTextVisible("source-type");
//    cy.wait(10000); // waits for 5 seconds
//
//    readyToTypeEditor().type("{ctrl}a").clear({ force: true });
//    readyToTypeEditor().type("source:{enter}", { force: true });
//    readyToTypeEditor().type("    type: demo-data", { force: true });
//    readyToTypeEditor().type("{enter}", { force: true });
//    // no space because the editor starts new line at same indentation
//    readyToTypeEditor().type("config: {}", { force: true });
//    cy.clickOptionWithText("Next");
//    cy.clickOptionWithText("Next");
//
//    cy.enterTextInTestId("source-name-input", testName);
//    cy.clickOptionWithText("Advanced");
//    cy.enterTextInTestId("cli-version-input", cli_version);
//    cy.clickOptionWithTextToScrollintoView("Save & Run");
//    cy.waitTextVisible(testName);
//
//    cy.contains(testName)
//      .parent()
//      .within(() => {
//        cy.contains("Succeeded", { timeout: 180000 });
//        cy.clickOptionWithTestId("delete-button");
//      });
//    cy.clickOptionWithText("Yes");
//    cy.ensureTextNotPresent(testName);
//  });
// });
