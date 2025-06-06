describe("applications", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    Cypress.on("uncaught:exception", (err, runnable) => false);
  });

  it("can see elements inside the application", () => {
    cy.login();
    cy.goToApplication("urn:li:application:d63587c6-cacc-4590-851c-4f51ca429b51/Assets");
    cy.contains("cypress_logging_events");
    cy.contains("1 - 1 of 1");
  });
});
