describe("home", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.skipIntroducePage();
    cy.on("uncaught:exception", (err, runnable) => false);
  });
  it("home page shows ", () => {
    cy.login();
    cy.visit("/");
    cy.get('[xmlns="http://www.w3.org/2000/svg"]').should("exist");
    cy.get('[data-testid="home-page-content-container"').should("exist");
    cy.get('[data-testid="nav-menu-links"').should("exist");
  });
});
