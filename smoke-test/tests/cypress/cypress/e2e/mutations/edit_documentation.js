const test_id = Math.floor(Math.random() * 100000);
const documentation_edited = `This is test${test_id} documentation EDITED`;
const wrong_url = "https://www.linkedincom";
const correct_url = "https://www.linkedin.com";

describe("edit documentation and link to dataset", () => {
  it("open test dataset page, edit documentation", () => {
    //edit documentation and verify changes saved
    cy.loginWithCredentials();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)/Schema"
    );
    cy.get("[role='tab']").contains("Documentation").click();
    cy.waitTextVisible("my hive dataset");
    cy.waitTextVisible("Sample doc");
    cy.clickOptionWithText("Edit");
    cy.focused().clear();
    cy.focused().type(documentation_edited);
    cy.get("button").contains("Save").click();
    cy.waitTextVisible("Description Updated");
    cy.waitTextVisible(documentation_edited);
    //return documentation to original state
    cy.clickOptionWithText("Edit");
    cy.focused().clear().wait(1000);
    cy.focused().type("my hive dataset");
    cy.get("button").contains("Save").click();
    cy.waitTextVisible("Description Updated");
    cy.waitTextVisible("my hive dataset");
  });

  it("open test dataset page, remove and add dataset link", () => {
    cy.loginWithCredentials();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)/Schema"
    );
    cy.get("[role='tab']").contains("Documentation").click();
    cy.contains("Sample doc").trigger("mouseover", { force: true });
    cy.get('[data-icon="delete"]').click();
    cy.waitTextVisible("Link Removed");
    cy.get("button").contains("Add Link").click();
    cy.get("#addLinkForm_url").type(wrong_url);
    cy.waitTextVisible("This field must be a valid url.");
    cy.focused().clear();
    cy.waitTextVisible("A URL is required.");
    cy.focused().type(correct_url);
    cy.ensureTextNotPresent("This field must be a valid url.");
    cy.get("#addLinkForm_label").type("Sample doc");
    cy.get('[role="dialog"] button').contains("Add").click();
    cy.waitTextVisible("Link Added");
    cy.get("[role='tab']").contains("Documentation").click();
    cy.get(`[href='${correct_url}']`).should("be.visible");
  });

  it("open test domain page, remove and add dataset link", () => {
    cy.loginWithCredentials();
    cy.visit("/domain/urn:li:domain:marketing/Entities");
    cy.get("[role='tab']").contains("Documentation").click();
    cy.get("button").contains("Add Link").click();
    cy.get("#addLinkForm_url").type(wrong_url);
    cy.waitTextVisible("This field must be a valid url.");
    cy.focused().clear();
    cy.waitTextVisible("A URL is required.");
    cy.focused().type(correct_url);
    cy.ensureTextNotPresent("This field must be a valid url.");
    cy.get("#addLinkForm_label").type("Sample doc");
    cy.get('[role="dialog"] button').contains("Add").click();
    cy.waitTextVisible("Link Added");
    cy.get("[role='tab']").contains("Documentation").click();
    cy.get(`[href='${correct_url}']`).should("be.visible");
    cy.contains("Sample doc").trigger("mouseover", { force: true });
    cy.get('[data-icon="delete"]').click();
    cy.waitTextVisible("Link Removed");
  });

  it("edit field documentation", () => {
    cy.loginWithCredentials();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)/Schema"
    );
    cy.get("tbody [data-icon='edit']").first().click({ force: true });
    cy.waitTextVisible("Update description");
    cy.waitTextVisible("Foo field description has changed");
    cy.focused().clear().wait(1000);
    cy.focused().type(documentation_edited);
    cy.get("button").contains("Update").click();
    cy.waitTextVisible("Updated!");
    cy.waitTextVisible(documentation_edited);
    cy.waitTextVisible("(edited)");
    cy.get("tbody [data-icon='edit']").first().click({ force: true });
    cy.focused().clear().wait(1000);
    cy.focused().type("Foo field description has changed");
    cy.get("button").contains("Update").click();
    cy.waitTextVisible("Updated!");
    cy.waitTextVisible("Foo field description has changed");
    cy.waitTextVisible("(edited)");
  });
});
