// Migrated to Playwright — see e2e-test/ui/playwright/tests/
describe.skip("login", () => {
  it("logs in", () => {
    cy.visit("/");
    cy.get("input[data-testid=username]").type(Cypress.env("ADMIN_USERNAME"));
    cy.get("input[data-testid=password]").type(Cypress.env("ADMIN_PASSWORD"));
    cy.get('[data-testid="sign-in"]').click();
    cy.skipIntroducePage();
    cy.contains("Discover");
  });
});
