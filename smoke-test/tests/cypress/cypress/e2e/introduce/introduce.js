describe("login", () => {
  beforeEach(() => {
    cy.setFeatureFlags(true, (res) => {
      res.body.data.appConfig.featureFlags.showHomePageRedesign = false;
    });
  });

  it("logs in and fills out the introduce page", () => {
    cy.visit("/");
    cy.loginWithCredentials();
    cy.visit("/introduce");
    cy.get('[data-testid="introduce-role-select"]').click();
    cy.get('[data-testid="role-option-Data Analyst"]').click({ force: true });
    cy.get(".ant-select-selection-overflow").click();
    cy.get('[src*="bigquerylogo.png"]').should("be.visible").click();
    cy.get("body").click();
    cy.get(".ant-btn-primary").click();
    cy.get('[data-testid="home-page-content-container"').should("exist");
  });
});
