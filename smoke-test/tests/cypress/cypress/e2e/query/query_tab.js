const DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)";
const runId = Date.now();

const addNewQuery = () => {
  cy.get('[data-testid="add-query-button"]').click();
  cy.get('[data-mode-id="sql"]').click().type(` + Test Query-${runId}`);
  cy.get('[data-testid="query-builder-title-input"]')
    .click()
    .type(`Test Table-${runId}`);
  cy.get(".ProseMirror").click().type(`Test Description-${runId}`);
  cy.get('[data-testid="query-builder-save-button"]').click();
  cy.waitTextVisible("Created Query!");
};

const editQuery = () => {
  cy.get('[data-testid="query-edit-button-0"]').click();
  cy.get('[data-mode-id="sql"]').click().type(` + Edited Query-${runId}`);
  cy.get('[data-testid="query-builder-title-input"]')
    .click()
    .clear()
    .type(`Edited Table-${runId}`);
  cy.get(".ProseMirror").click().clear().type(`Edited Description-${runId}`);
  cy.get('[data-testid="query-builder-save-button"]').click();
  cy.waitTextVisible("Edited Query!");
};

const deleteQuery = () => {
  cy.get('[data-testid="query-more-button-0"]').click();
  cy.clickOptionWithText("Delete");
  cy.clickOptionWithText("Yes");
  cy.waitTextVisible("Deleted Query!");
};

const verifyViewCardDetails = (query, title, description) => {
  cy.get('[data-testid="query-content-0"]')
    .scrollIntoView()
    .should("be.visible")
    .click();
  cy.get(".ant-modal-content").waitTextVisible(query);
  cy.get(".ant-modal-content").waitTextVisible(title);
  cy.get(".ant-modal-content").waitTextVisible(description);
};

describe("manage queries", () => {
  beforeEach(() => {
    cy.loginWithCredentials();
    cy.goToDataset(DATASET_URN, "SampleCypressHdfsDataset");
    cy.openEntityTab("Queries");
  });

  it("go to queries tab on dataset page then create query and verify & view the card", () => {
    cy.wait(1000);
    cy.url().should("include", "Queries");
    cy.ensureTextNotPresent("Recent Queries");
    addNewQuery();
    cy.waitTextVisible(`+ Test Query-${runId}`);
    cy.waitTextVisible(`Test Table-${runId}`);
    cy.waitTextVisible(`Test Description-${runId}`);
    cy.waitTextVisible("Created on");
    verifyViewCardDetails(
      `+ Test Query-${runId}`,
      `Test Table-${runId}`,
      `Test Description-${runId}`,
    );
  });

  it("go to queries tab on dataset page then edit the query and verify edited Query card", () => {
    editQuery();
    verifyViewCardDetails(
      `+ Test Query-${runId} + Edited Query-${runId}`,
      `Edited Table-${runId}`,
      `Edited Description-${runId}`,
    );
  });

  it("go to queries tab on dataset page then delete the query and verify that query should be gone", () => {
    deleteQuery();
    cy.ensureTextNotPresent(`+ Test Query-${runId} + Edited Query-${runId}`);
    cy.ensureTextNotPresent(`Edited Table-${runId}`);
    cy.ensureTextNotPresent(`Edited Description-${runId}`);
  });
});
