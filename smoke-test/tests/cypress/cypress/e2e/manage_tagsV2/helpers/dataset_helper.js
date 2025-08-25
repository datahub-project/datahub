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
  }

  static ensureTagIsAssigned(name) {
    cy.getWithTestId(`tag-${name}`).should("be.visible");
  }

  static unassignTag(name) {
    cy.getWithTestId(`tag-${name}`).within(() => {
      cy.get(".ant-tag-close-icon").click();
    });

    cy.get(".ant-modal-confirm-confirm").within(() => {
      cy.get(".ant-btn-primary").click();
    });

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
