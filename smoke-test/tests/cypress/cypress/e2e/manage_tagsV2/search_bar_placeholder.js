describe("tags - search bar placeholder", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.login();
  });

  it("verify search bar placeholder", () => {
    cy.visit("/tags");
    cy.get('[data-testid="tag-search-input"]').should(
      "have.attr",
      "placeholder",
      "Search tags...",
    );
  });
});
