describe("tags - search not exists", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.login();
  });

  it("verify search not exists", () => {
    cy.visit("/tags");
    cy.get('[data-testid="page-title"]').should("contain.text", "Manage Tags");
    cy.get('[data-testid="urn:li:tag:Cypress-name"]').should(
      "contain.text",
      "Cypress",
    );
    cy.get('[data-testid="tag-search-input"]').type("invalidvalue");
    cy.get('[data-testid="tags-not-found"]').should(
      "contain.text",
      "No tags found for your search query",
    );
  });
});
