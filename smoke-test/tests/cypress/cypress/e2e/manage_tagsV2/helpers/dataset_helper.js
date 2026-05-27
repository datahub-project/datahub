export default class DatasetHelper {
  static openDataset(urn, name) {
    cy.goToDataset(urn, name);
  }

  static assignTag(name) {
    cy.get("#entity-profile-tags").within(() => {
      cy.get('[data-testid="add-tags-button"]').should("not.be.disabled");
      cy.clickOptionWithTestId("add-tags-button");
    });

    // AddTagsModal uses alchemy SimpleSelect: click the trigger to open the
    // portal-rendered dropdown, then type into its search input.
    cy.getWithTestId("tag-term-modal-input").click();
    cy.get('[data-testid="dropdown-search-input"]').type(name);
    cy.get(`[data-testid="tag-term-option-${name}"]`)
      .first()
      .click({ force: true });
    cy.clickOptionWithTestId("add-tag-term-from-modal-btn");
    cy.waitTextVisible("Added Tags!");
  }

  static ensureTagIsAssigned(name) {
    cy.getWithTestId(`tag-${name}`).should("be.visible");
  }

  static unassignTag(name) {
    cy.getWithTestId(`tag-${name}`).within(() => {
      cy.get('[data-testid="remove-icon"]').click();
    });

    cy.getWithTestId("modal-confirm-button").click();

    cy.waitTextVisible("Removed Tag!");
  }

  static ensureTagIsNotAssigned(name) {
    cy.getWithTestId(`tag-${name}`).should("not.exist");
  }

  static searchByTag(tagName) {
    cy.visit(
      `/search?filter_tags___false___EQUAL___0=urn%3Ali%3Atag%3A${tagName}&page=1&query=%2A&unionType=0`,
    );
  }

  static ensureEntityIsInSearchResults(urn) {
    cy.getWithTestId(`preview-${urn}`).should("be.visible");
  }

  static ensureEntityIsNotInSearchResults(urn) {
    cy.getWithTestId(`preview-${urn}`).should("not.exist");
  }
}
