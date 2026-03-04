import {
  setThemeV2AndIngestionRedesignFlags,
  createAndRunIngestionSource,
  createIngestionSource,
  updateIngestionSource,
  deleteIngestionSource,
  runIngestionSource,
  verifyScheduleIsAppliedOnTable,
  goToIngestionPage,
} from "./utils";

const number = crypto.getRandomValues(new Uint32Array(1))[0];

const ingestionSourceDetails = {
  accound_id: `account${number}`,
  warehouse_id: `warehouse${number}`,
  username: `user${number}`,
  password: `password${number}`,
  role: `role${number}`,
  authentication_type: "Username & Password",
};

describe("ingestion sources", () => {
  beforeEach(() => {
    setThemeV2AndIngestionRedesignFlags(true);
    cy.loginWithCredentials();
    goToIngestionPage();
  });

  Cypress.on("uncaught:exception", (err, runnable) => false);

  it("create a new ingestion source with details", () => {
    const sourceName = "new ingestion source";
    createIngestionSource(sourceName, {
      sourceDetails: ingestionSourceDetails,
      schedule: { hour: "01" },
    });
    cy.waitTextVisible(sourceName);
    verifyScheduleIsAppliedOnTable(sourceName, "01:00 am");

    deleteIngestionSource(sourceName);
  });

  it("edit an ingestion source", () => {
    const sourceName = "ingestion source";
    const updatedSourceName = "updated ingestion source";
    createIngestionSource(sourceName);
    verifyScheduleIsAppliedOnTable(sourceName, "12:00 am"); // the default schedule
    updateIngestionSource(sourceName, updatedSourceName, {
      schedule: { hour: "01" },
    });
    cy.waitTextVisible(updatedSourceName);
    verifyScheduleIsAppliedOnTable(updatedSourceName, "01:00 am");
    deleteIngestionSource(updatedSourceName);
  });

  it("delete an ingestion source", () => {
    const sourceName = "ingestion source to delete";
    createIngestionSource(sourceName);
    deleteIngestionSource(sourceName);
    cy.ensureTextNotPresent(sourceName);
  });

  it("create and run an ingestion source", () => {
    const sourceName = "create and run source";
    createAndRunIngestionSource(sourceName);
    deleteIngestionSource(sourceName);
  });

  it("execute an ingestion source", () => {
    const sourceName = "ingestion source to run";
    createIngestionSource(sourceName);
    cy.wait(3000);
    runIngestionSource(sourceName);
    cy.contains("td", sourceName)
      .parent()
      .within(() => {
        cy.contains("Failed", { timeout: 100000 });
      });
    deleteIngestionSource(sourceName);
  });

  it("search for an ingestion source", () => {
    const sourceName1 = "first ingestion source";
    const sourceName2 = "second ingestion source";

    createIngestionSource(sourceName1);
    createIngestionSource(sourceName2);
    cy.get('[data-testid="ingestion-sources-search"]').should("be.visible");
    cy.get('[data-testid="ingestion-sources-search"]').type(sourceName1);
    cy.wait(3000);
    cy.waitTextVisible(sourceName1);
    cy.ensureTextNotPresent(sourceName2);
    cy.get('[data-testid="ingestion-sources-search"]').clear();
    cy.wait(2000);
    deleteIngestionSource(sourceName1);
    deleteIngestionSource(sourceName2);
  });

  it("view run details page", () => {
    const sourceName = "run details ingestion source";
    createAndRunIngestionSource(sourceName);
    cy.waitTextVisible(sourceName);
    cy.contains("td", sourceName)
      .siblings("td")
      .find('[data-testid="ingestion-source-table-status"]')
      .click();
    cy.waitTextVisible("Run Details");
    cy.waitTextVisible("Success");
    cy.get('[data-testid="run-details-summary-tab"]').should("be.visible");
    cy.get('[data-testid="run-details-logs-tab"]').should("be.visible");
    cy.get('[data-testid="run-details-recipe-tab"]').should("be.visible");

    cy.contains("Manage Data Sources").click();
    deleteIngestionSource(sourceName);
  });
});
