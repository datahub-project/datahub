describe("operations", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });
  // skip this test as it's broken with the new v2 stats experience. We have a whole other set of tests for the v2 stats experience
  it.skip("can visit dataset with operation aspect and verify last updated is present", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.bigquery_usage_logs.cypress_logging_events,PROD)/Stats?is_lineage_mode=false",
    );
    cy.contains("test-project.bigquery_usage_logs.cypress_logging_events");

    // Last updated text is present
    cy.contains("Last Updated");
  });
});
