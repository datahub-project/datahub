describe('analytics', () => {
  it('can go to a dataset and see analytics', () => {
    cy.login();

    cy.visit("/analytics");
    cy.contains("documentation").should('not.exist');
    cy.contains("dashboards").should('not.exist');

    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)');
    cy.get("#rc-tabs-0-tab-Documentation").click();

    cy.visit("/chart/urn:li:chart:(looker,baz1)");
    cy.get("#rc-tabs-0-panel-Dashboards").click({ force: true });

    cy.visit("/analytics");
    cy.contains("documentation");
    cy.contains("dashboards");
  });
})
