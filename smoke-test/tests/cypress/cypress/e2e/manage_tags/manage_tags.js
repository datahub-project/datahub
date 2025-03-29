describe("manage tags", () => {
  it("Manage Tags Page - Verify search bar placeholder", () => {
    cy.login();
    cy.visit("/tags");
    cy.get('[data-testid="search-bar-input"]').should(
      "have.attr",
      "placeholder",
      "Search tags...",
    );
  });
  it("Manage Tags Page - Verify Title, Search, and Results", () => {
    cy.login();
    cy.visit("/tags");
    cy.get('[data-testid="page-title"]').should("contain.text", "Manage Tags");
    cy.get('[data-testid="urn:li:tag:Cypress-name"]').should(
      "contain.text",
      "Cypress",
    );
    cy.get('[data-testid="search-bar-input"]').type("Cypress");
    cy.get('[data-testid="urn:li:tag:Cypress-name"]').should(
      "contain.text",
      "Cypress",
    );
  });
  it("Manage Tags Page - Verify search not exists", () => {
    cy.login();
    cy.visit("/tags");
    cy.get('[data-testid="page-title"]').should("contain.text", "Manage Tags");
    cy.get('[data-testid="urn:li:tag:Cypress-name"]').should(
      "contain.text",
      "Cypress",
    );
    cy.get('[data-testid="search-bar-input"]').type("test");
    cy.get('[data-testid="tags-not-found"]').should(
      "contain.text",
      "No tags found for your search query",
    );
  });
});
