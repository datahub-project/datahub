const test_id = Math.floor(Math.random() * 100000);
const documentation_edited = `This is test${test_id} documentation EDITED`;
const wrong_url = "https://www.linkedincom";
const correct_url = "https://www.linkedin.com";

describe("edit documentation and link to dataset", () => {
  it("edit field documentation", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)/Schema",
    );
    cy.clickOptionWithText("field_foo");
    cy.clickOptionWithTestId("edit-field-description");
    cy.waitTextVisible("Update description");
    cy.waitTextVisible("Foo field description has changed");
    cy.getWithTestId("description-editor").clear().wait(1000);
    cy.focused().type(documentation_edited);
    cy.clickOptionWithTestId("description-modal-update-button");
    cy.waitTextVisible("Updated!");
    cy.waitTextVisible(documentation_edited);
    cy.waitTextVisible("(edited)");
    cy.clickOptionWithTestId("edit-field-description");
    cy.getWithTestId("description-editor").clear().wait(1000);
    cy.focused().type("Foo field description has changed");
    cy.clickOptionWithTestId("description-modal-update-button");
    cy.waitTextVisible("Updated!");
    cy.waitTextVisible("Foo field description has changed");
    cy.waitTextVisible("(edited)");
  });
});
