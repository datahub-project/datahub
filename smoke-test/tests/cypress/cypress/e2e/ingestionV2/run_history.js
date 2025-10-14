import {
  setThemeV2AndIngestionRedesignFlags,
  createAndRunIngestionSource,
  deleteIngestionSource,
  shouldNavigateToRunHistoryTab,
  shouldNavigateToSourcesTab,
  navigateToTab,
  createIngestionSource,
  runIngestionSource,
  filterBySource,
  goToIngestionPage,
} from "./utils";

describe("run history tab in manage data sources", () => {
  beforeEach(() => {
    setThemeV2AndIngestionRedesignFlags(true);
    cy.loginWithCredentials();
    goToIngestionPage();
  });

  Cypress.on("uncaught:exception", (err, runnable) => false);

  it("navigate to run history tab from last run column of sources tab", () => {
    const sourceName = "test ingestion source history";
    createAndRunIngestionSource(sourceName);
    cy.wait(2000);
    cy.contains("td", sourceName)
      .siblings("td")
      .find('[data-testid="ingestion-source-last-run"]')
      .click();
    shouldNavigateToRunHistoryTab();
    navigateToTab("Sources");
    deleteIngestionSource(sourceName);
  });

  it("navigate to run history tab from View run history option in dropdown of sources", () => {
    const sourceName = "ingestion source history";
    createAndRunIngestionSource(sourceName);
    cy.wait(2000);
    cy.contains("td", sourceName)
      .siblings("td")
      .find('[data-testid="ingestion-more-options"]')
      .click();
    cy.get("body .ant-dropdown-menu").contains("View Run History").click();
    shouldNavigateToRunHistoryTab();
    navigateToTab("Sources");
    deleteIngestionSource(sourceName);
  });

  it("view past executions in run history tab", () => {
    const sourceName = "source for past executions";
    createAndRunIngestionSource(sourceName);
    cy.wait(2000);
    navigateToTab("RunHistory");
    cy.get('[data-testid="executions-table"]').within(() => {
      cy.contains('[data-testid="ingestion-source-name"]', sourceName).should(
        "be.visible",
      );
    });
    navigateToTab("Sources");
    deleteIngestionSource(sourceName);
  });

  it("navigate to sources tab from source name in run history tab", () => {
    const sourceName = "source for tab navigation";
    createAndRunIngestionSource(sourceName);
    cy.wait(2000);
    navigateToTab("RunHistory");
    cy.get('[data-testid="executions-table"]').within(() => {
      cy.contains('[data-testid="ingestion-source-name"]', sourceName)
        .should("be.visible")
        .first()
        .click({ force: true });
    });
    shouldNavigateToSourcesTab();
    cy.wait(2000);
    deleteIngestionSource(sourceName);
  });

  it("filter execution requests by source name", () => {
    const sourceName1 = "Source1";
    const sourceName2 = "Source2";

    createIngestionSource(sourceName1);
    createIngestionSource(sourceName2);
    runIngestionSource(sourceName1);
    runIngestionSource(sourceName2);
    navigateToTab("RunHistory");
    filterBySource(sourceName1);
    cy.get('[data-testid="executions-table"]').within(() => {
      cy.contains("td", sourceName1).should("be.visible");
      cy.contains("td", sourceName2).should("not.exist");
    });

    navigateToTab("Sources");
    deleteIngestionSource(sourceName1);
    deleteIngestionSource(sourceName2);
  });
});
