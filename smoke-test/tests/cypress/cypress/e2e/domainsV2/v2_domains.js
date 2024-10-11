describe("domains", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    Cypress.on("uncaught:exception", (err, runnable) => false);
  });

  it.skip("can see elements inside the domain", () => {
    cy.login();
    cy.goToDomain("urn:li:domain:marketing/Entities");
    cy.contains("Marketing");
    cy.get('[data-node-key="Assets"]').click();
    cy.contains("Baz Chart 1");
    cy.contains("1 - 1 of 1");
  });
});
