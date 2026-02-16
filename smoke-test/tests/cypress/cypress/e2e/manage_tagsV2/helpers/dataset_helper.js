export default class DatasetHelper {
  static openDataset(urn, name) {
    cy.goToDataset(urn, name);
  }

  static assignTag(name) {
    cy.get("#entity-profile-tags").within(() => {
      cy.clickOptionWithTestId("AddRoundedIcon");
    });

    cy.getWithTestId("tag-term-modal-input").within(() => {
      cy.get("input").focus({ force: true }).type(name);
    });

    cy.get(`[name="${name}"]`).click();
    cy.clickOptionWithTestId("add-tag-term-from-modal-btn");
    cy.waitTextVisible("Added Tags!");

    // Wait for mutation to complete and UI to update
    cy.wait(1000);
  }

  static ensureTagIsAssigned(name) {
    // Increased timeout for eventual consistency
    cy.getWithTestId(`tag-${name}`, { timeout: 10000 }).should("be.visible");
  }

  static unassignTag(name) {
    cy.getWithTestId(`tag-${name}`).within(() => {
      cy.get(".ant-tag-close-icon").click();
    });

    cy.getWithTestId("modal-confirm-button").click();

    cy.waitTextVisible("Removed Tag!");

    // Wait for mutation to complete and UI to update
    cy.wait(1000);
  }

  static ensureTagIsNotAssigned(name) {
    // Increased timeout for eventual consistency
    cy.getWithTestId(`tag-${name}`, { timeout: 10000 }).should("not.exist");
  }

  static searchByTag(tagName) {
    cy.visit(
      `/search?filter_tags___false___EQUAL___0=urn%3Ali%3Atag%3A${tagName}&page=1&query=%2A&unionType=0`,
    );

    // Wait for search results to load and index to be queried
    cy.wait(2000);
  }

  static ensureEntityIsInSearchResults(urn) {
    // Increased timeout for search indexing
    cy.getWithTestId(`preview-${urn}`, { timeout: 15000 }).should("be.visible");
  }

  static ensureEntityIsNotInSearchResults(urn) {
    // Wait and then verify entity is not in results
    // Increased timeout to ensure index has been updated
    cy.getWithTestId(`preview-${urn}`, { timeout: 15000 }).should("not.exist");
  }
}
