const test_id = Math.floor(Math.random() * 100000);
const documentation_edited = `This is test${test_id} documentation EDITED`;
const wrong_url = "https://www.linkedincom";
const correct_url = "https://www.linkedin.com";

describe("edit documentation and link to dataset", () => {
  it("open test dataset page, edit documentation", () => {
    // edit documentation and verify changes saved
    cy.loginWithCredentials();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)/Schema",
    );
    cy.openEntityTab("Documentation");
    cy.waitTextVisible("my hive dataset");
    cy.waitTextVisible("Sample doc");
    cy.clickOptionWithTestId("edit-documentation-button");
    cy.focused().clear();
    cy.focused().type(documentation_edited);
    cy.clickOptionWithTestId("description-editor-save-button");
    cy.waitTextVisible("Description Updated");
    cy.waitTextVisible(documentation_edited);
    // return documentation to original state
    cy.clickOptionWithTestId("edit-documentation-button");
    cy.focused().clear().wait(1000);
    cy.focused().type("my hive dataset");
    cy.clickOptionWithTestId("description-editor-save-button");
    cy.waitTextVisible("Description Updated");
    cy.waitTextVisible("my hive dataset");
  });

  it("open test dataset page, remove and add dataset link", () => {
    cy.loginWithCredentials();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)/Schema",
    );
    cy.openEntityTab("Documentation");
    cy.contains("Sample doc").trigger("mouseover", { force: true });
    cy.get('[data-icon="delete"]').click().wait(1000);
    cy.get("button")
      .contains("span", "Yes")
      .should("be.visible")
      .click({ force: true });
    cy.waitTextVisible("Link Removed");
    cy.clickOptionWithTestId("add-link-button").wait(1000);
    cy.enterTextInTestId("add-link-modal-url", wrong_url);
    cy.waitTextVisible("This field must be a valid url.");
    cy.focused().clear();
    cy.waitTextVisible("A URL is required.");
    cy.enterTextInTestId("add-link-modal-url", correct_url);
    cy.ensureTextNotPresent("This field must be a valid url.");
    cy.enterTextInTestId("add-link-modal-label", "Sample doc");
    cy.clickOptionWithTestId("add-link-modal-add-button");
    cy.waitTextVisible("Link Added");
    cy.openEntityTab("Documentation");
    cy.get(`[href='${correct_url}']`).should("be.visible");
  });

  it("open test domain page, remove and add dataset link", () => {
    cy.loginWithCredentials();
    cy.visit("/domain/urn:li:domain:marketing/Entities");
    cy.waitTextVisible("SampleCypressKafkaDataset");
    cy.clickOptionWithTestId("add-link-button").wait(1000);
    cy.enterTextInTestId("add-link-modal-url", wrong_url);
    cy.waitTextVisible("This field must be a valid url.");
    cy.focused().clear();
    cy.waitTextVisible("A URL is required.");
    cy.enterTextInTestId("add-link-modal-url", correct_url);
    cy.ensureTextNotPresent("This field must be a valid url.");
    cy.enterTextInTestId("add-link-modal-label", "Sample doc");
    cy.clickOptionWithTestId("add-link-modal-add-button");
    cy.waitTextVisible("Link Added");
    cy.openEntityTab("Documentation");
    cy.get("[data-testid='edit-documentation-button']").should("be.visible");
    cy.get(`[href='${correct_url}']`).should("be.visible");
    cy.contains("Sample doc").trigger("mouseover", { force: true });
    cy.get('[data-icon="delete"]').click().wait(1000);
    cy.get("button")
      .contains("span", "Yes")
      .should("be.visible")
      .click({ force: true });
    cy.waitTextVisible("Link Removed");
  });

  it("edit field documentation", () => {
    cy.loginWithCredentials();
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
