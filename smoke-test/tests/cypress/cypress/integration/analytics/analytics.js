describe('analytics', () => {
  it('can go to a chart and see analytics in Section Views', () => {
    cy.login();

    cy.goToAnalytics();
    cy.ensureTextNotPresent("dashboards");

    cy.goToChart("urn:li:chart:(looker,cypress_baz1)");
    cy.waitTextVisible("Baz Chart 1");
    cy.openEntityTab("Dashboards");

    cy.goToAnalytics();
    cy.wait(2000);
    cy.waitTextVisible("dashboards");
  });
})
