import {
  setIngestionRedesignFlags,
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

const testId = Math.floor(Math.random() * 100000);

describe("run history tab in manage data sources", () => {
  const createdSources = [];

  beforeEach(() => {
    setIngestionRedesignFlags(true);
    cy.login();
    goToIngestionPage();
  });

  afterEach(() => {
    // Guarantee cleanup even if a test fails mid-way
    if (createdSources.length > 0) {
      // Use goToIngestionPage() rather than navigateToTab("Sources") so that
      // cleanup always starts from a known page state. Tests that navigate to
      // the Run History tab leave the Sources tab out of view, causing
      // navigateToTab to time out waiting for the tab element.
      goToIngestionPage();
      createdSources.forEach((name) => {
        deleteIngestionSource(name);
      });
      createdSources.length = 0;
    }
  });

  Cypress.on("uncaught:exception", (err) => {
    if (err.message.includes("ResizeObserver")) return false;
    // Suppress null focus errors from the app (pre-existing React bug where
    // .focus() is called on an unmounted element) to prevent flaky test aborts.
    if (
      err.message.includes("Cannot read properties of null (reading 'focus')")
    )
      return false;
    return true;
  });

  it("navigate to run history tab from last run column of sources tab", () => {
    const sourceName = `test ingestion source history ${testId}`;
    createdSources.push(sourceName);
    createAndRunIngestionSource(sourceName);
    cy.contains("td", sourceName, { timeout: 10000 })
      .siblings("td")
      .find('[data-testid="ingestion-source-last-run"]')
      .click();
    shouldNavigateToRunHistoryTab();
  });

  it("navigate to run history tab from View run history option in dropdown of sources", () => {
    const sourceName = `ingestion source history ${testId}`;
    createdSources.push(sourceName);
    createAndRunIngestionSource(sourceName);
    cy.contains("td", sourceName, { timeout: 10000 })
      .siblings("td")
      .find('[data-testid="ingestion-more-options"]')
      .click();
    cy.get("body .ant-dropdown-menu").contains("View Run History").click();
    shouldNavigateToRunHistoryTab();
  });

  it("view past executions in run history tab", () => {
    const sourceName = `source for past executions ${testId}`;
    createdSources.push(sourceName);
    createAndRunIngestionSource(sourceName);
    navigateToTab("RunHistory");
    cy.get('[data-testid="executions-table"]').within(() => {
      cy.contains('[data-testid="ingestion-source-name"]', sourceName, {
        timeout: 10000,
      }).should("be.visible");
    });
  });

  it("navigate to sources tab from source name in run history tab", () => {
    const sourceName = `source for tab navigation ${testId}`;
    createdSources.push(sourceName);
    createAndRunIngestionSource(sourceName);
    navigateToTab("RunHistory");
    cy.get('[data-testid="executions-table"]').within(() => {
      cy.contains('[data-testid="ingestion-source-name"]', sourceName, {
        timeout: 10000,
      })
        .should("be.visible")
        .first()
        .click({ force: true });
    });
    shouldNavigateToSourcesTab();
  });

  it("filter execution requests by source name", () => {
    const sourceName1 = `Source1 ${testId}`;
    const sourceName2 = `Source2 ${testId}`;
    createdSources.push(sourceName1, sourceName2);

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
  });
});
