describe("analytics", () => {
  it("can go to a chart and see analytics in tab views", () => {
    cy.login();

    cy.goToChart("urn:li:chart:(looker,cypress_baz1)");
    cy.waitTextVisible("Baz Chart 1");
    cy.openEntityTab("Dashboards");
    cy.wait(1000);

    cy.goToAnalytics();
    cy.contains("Tab Views By Entity Type (Past Week)").scrollIntoView({
      ensureScrollable: false,
    });
    cy.waitTextPresent("dashboards");
  });
});
