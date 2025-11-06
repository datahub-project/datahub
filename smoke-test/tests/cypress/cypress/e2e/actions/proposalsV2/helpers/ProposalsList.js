/* eslint-disable class-methods-use-this */
class ProposalsList {
  static filterIds = {
    type: "Type",
    status: "Status",
    createdBy: "Created-By",
  };

  visit() {
    cy.visit("/requests/proposals");
  }

  visitInbox() {
    cy.get('[data-testid="proposal-groups"]')
      .should("be.visible")
      .contains("Inbox")
      .click();
  }

  visitMyProposals() {
    cy.get('[data-testid="proposal-groups"]')
      .should("be.visible")
      .contains("My Requests")
      .click();
  }

  clickRowContaining(name) {
    this.findRowContaining(name)
      .closest("tr")
      .click({ position: "topLeft", force: true });
  }

  findRowContaining(name) {
    return cy
      .get('[data-testid="proposals-table"]')
      .find("tbody tr")
      .contains(name);
  }

  getFilter(filterName) {
    const filterNameId = ProposalsList.filterIds[filterName];
    return cy
      .get("#proposals-filters-section")
      .should("be.visible")
      .get(`[data-testid="filter-dropdown-${filterNameId}"]`)
      .should("be.visible");
  }

  clickFilterOption(filterName, optionName) {
    this.getFilter(filterName).click();
    cy.getWithTestId("filter-dropdown").should("be.visible");
    cy.getWithTestId("filter-dropdown")
      .contains(optionName)
      .should("exist")
      .click();
    this.getFilter(filterName).click();
  }

  getEntitySelectComponent() {
    return cy.getWithTestId("filter-dropdown-Entity").should("be.visible");
  }

  findEntityOptionContaining(entity) {
    return cy
      .getWithTestId("entity-select-options")
      .find(`[data-testid="entity-${entity.simpleUrn}"]`);
  }

  selectEntityOptionContaining(entity) {
    this.getEntitySelectComponent().click();
    cy.getWithTestId("dropdown-search-input")
      .should("be.visible")
      .click()
      .clear()
      .type(entity.name, { delay: 0 });
    this.findEntityOptionContaining(entity).should("be.visible").click();
    cy.get(".clear-search").click();
    this.getEntitySelectComponent().click();
  }

  getSidebar() {
    return cy.get("#entity-profile-sidebar");
  }

  getFooter() {
    return cy.get('[data-testid="proposals-footer-actions"]');
  }
}

export const proposalsList = new ProposalsList();
