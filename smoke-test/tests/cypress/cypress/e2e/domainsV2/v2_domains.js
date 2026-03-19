describe("domains", () => {
  it("can see elements inside the domain", () => {
    cy.login();
    cy.goToDomain("urn:li:domain:testing/Entities");
    cy.contains("Testing");
    cy.get('[data-node-key="Assets"]').click();
    cy.contains("Baz Dashboard");
    cy.contains("CypressTerm");
    cy.contains("1 - 2 of 2");
  });
});
