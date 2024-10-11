describe("home", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });
  it.skip("home page shows ", () => {
    cy.login();
    cy.visit("/");
    cy.handleIntroducePage();
    cy.get('[xmlns="http://www.w3.org/2000/svg"]').should("exist");
    cy.get('[id^="v2-home-pag').should("exist");
    cy.get('[class^="NavLinksMenu__LinksWrapper').should("exist");
  });
});
