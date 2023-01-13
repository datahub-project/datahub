describe('analytics', () => {
  it('can go to a chart and see analytics in Section Views', () => {
    cy.login();

    cy.goToChart("urn:li:chart:(looker,cypress_baz1)");
    cy.waitTextVisible("Baz Chart 1");
    cy.openEntityTab("Dashboards");
    cy.wait(1000);

    cy.goToAnalytics();
    cy.contains("Section Views across Entity Types").scrollIntoView({
      ensureScrollable: false
    })
    cy.waitTextPresent("dashboards");
  });
})
