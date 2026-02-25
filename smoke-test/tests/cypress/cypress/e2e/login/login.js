describe("login", () => {
  it("logs in", () => {
    cy.visit("/");
    cy.get("input[data-testid=username]").type("datahub");
    cy.get("input[data-testid=password]").type("datahub");
    cy.get('[data-testid="sign-in"]').click();
    cy.contains(`Welcome back, ${Cypress.env("ADMIN_DISPLAYNAME")}`);
  });
});
