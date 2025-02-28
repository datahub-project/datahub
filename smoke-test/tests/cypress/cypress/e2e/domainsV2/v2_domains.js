describe("domains", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    Cypress.on("uncaught:exception", (err, runnable) => false);
  });

  it("can see elements inside the domain", () => {
    cy.login();
    cy.goToDomain("urn:li:domain:testing/Entities");
    cy.contains("Testing");
    cy.get('[data-node-key="Assets"]').click();
    cy.contains("Baz Dashboard");
    cy.contains("1 - 1 of 1");
  });
});
