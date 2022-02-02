describe('analytics', () => {
  it('can go to a dataset and see analytics in Section Views', () => {
    cy.login();

    cy.visit("/analytics");
    cy.contains("documentation").should('not.exist');

    cy.visit("/chart/urn:li:chart:(looker,baz1)");
    cy.get("#rc-tabs-0-panel-Dashboards").click({ force: true });

    cy.visit("/analytics");
    cy.contains("documentation");
  });
})
