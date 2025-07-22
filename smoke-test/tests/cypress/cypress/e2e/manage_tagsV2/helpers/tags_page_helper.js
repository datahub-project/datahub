export default class TagsPageHelper {
  static openPage() {
    cy.visit("/tags");
  }

  static getTagUrn(name) {
    return `urn:li:tag:${name}`;
  }

  static create(name, description, shouldBeSuccessfullyCreated = true) {
    cy.clickOptionWithTestId("add-tag-button");
    cy.getWithTestId("tag-name-field").within(() =>
      cy.get("input").focus().type(name),
    );
    cy.getWithTestId("tag-description-field").within(() =>
      cy.get("input").focus().type(description),
    );
    cy.clickOptionWithTestId("create-tag-modal-create-button");

    if (shouldBeSuccessfullyCreated) {
      cy.waitTextVisible(`Tag "${name}" successfully created`);
    } else {
      cy.waitTextVisible("Failed to create tag. An unexpected error occurred");
      cy.clickOptionWithTestId("create-tag-modal-cancel-button");
    }
  }

  static remove(name) {
    cy.getWithTestId("tag-search-input").focus().type(name, { delay: 0 });
    cy.clickOptionWithTestId(`${TagsPageHelper.getTagUrn(name)}-actions`);
    cy.clickOptionWithTestId("action-delete");
    cy.clickOptionWithTestId("delete-tag-button");
    cy.getWithTestId("tag-search-input").clear();
  }

  static edit(name, newDescription) {
    cy.getWithTestId("tag-search-input").focus().type(name, { delay: 0 });
    cy.clickOptionWithTestId(`${TagsPageHelper.getTagUrn(name)}-actions`);
    cy.clickOptionWithTestId("action-edit");

    cy.getWithTestId("edit-tag-modal").within(() => {
      cy.getWithTestId("tag-description-field").within(() =>
        cy.get("input").focus().clear().type(newDescription),
      );
    });

    cy.clickOptionWithTestId("update-tag-button");
    cy.getWithTestId("tag-search-input").clear();
  }

  static ensureTagIsInTable(name, description) {
    cy.getWithTestId("tag-search-input").focus().type(name, { delay: 0 });
    cy.getWithTestId(`${TagsPageHelper.getTagUrn(name)}-name`).should(
      "contain",
      name,
    );
    cy.getWithTestId(`${TagsPageHelper.getTagUrn(name)}-description`).should(
      "contain",
      description,
    );
    cy.getWithTestId("tag-search-input").clear();
  }

  static ensureTagIsNotInTable(name) {
    cy.getWithTestId("tag-search-input").focus().type(name, { delay: 0 });
    cy.getWithTestId(`${TagsPageHelper.getTagUrn(name)}-name`).should(
      "not.exist",
    );
    cy.getWithTestId("tag-search-input").clear();
  }
}
