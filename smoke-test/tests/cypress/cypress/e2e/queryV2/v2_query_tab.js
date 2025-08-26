const DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)";
const runId = Date.now();

const addNewQuery = () => {
  cy.get('[data-testid="add-query-button"]').click();
  cy.get('[data-mode-id="sql"]').click().type(` + Test Query-${runId}`);
  cy.get('[data-testid="query-builder-title-input"]')
    .click()
    .type(`Test Table-${runId}`);
  cy.get(".ProseMirror").last().click().type(`Test Description-${runId}`);
  cy.get('[data-testid="query-builder-save-button"]').click();
  cy.waitTextVisible("Created Query!");
  cy.wait(3000);
  cy.reload();
};

const editQuery = () => {
  cy.get('[data-testid="edit-query"]').eq(0).click({ force: true });
  cy.get('[data-mode-id="sql"]').click().type(` + Edited Query-${runId}`);
  cy.get('[data-testid="query-builder-title-input"]')
    .click()
    .clear()
    .type(`Edited Table-${runId}`);
  cy.get(".ProseMirror")
    .last()
    .click()
    .clear()
    .type(`Edited Description-${runId}`);
  cy.get('[data-testid="query-builder-save-button"]').click();
  cy.waitTextVisible("Edited Query!");
  cy.reload();
};

const deleteQuery = () => {
  cy.get('[data-testid="delete-query"]').eq(0).click({ force: true });
  cy.clickOptionWithText("Yes");
  cy.waitTextVisible("Deleted Query!");
  cy.reload();
};

const verifyViewCardDetails = (query) => {
  cy.get('[data-testid="query-content-undefined"]')
    .first()
    .should("be.visible")
    .click();
  cy.get(".ant-modal-content").waitTextVisible(query);
};

describe("manage queries", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.goToDataset(DATASET_URN, "SampleCypressHdfsDataset");
    cy.get("body").click();
    cy.openEntityTab("Queries");
  });

  it("go to queries tab on dataset page then create query and verify & view the card", () => {
    cy.ensureTextNotPresent("Recent Queries");
    addNewQuery();
    cy.waitTextVisible(`+ Test Query-${runId}`);
    cy.waitTextVisible(`Test Table-${runId}`);
    cy.waitTextVisible(`Test Description-${runId}`);
    verifyViewCardDetails(`+ Test Query-${runId}`);
  });

  it("go to queries tab on dataset page then edit the query and verify edited Query card", () => {
    editQuery();
    cy.waitTextVisible(`Edited Table-${runId}`);
    cy.waitTextVisible(`Edited Description-${runId}`);
    verifyViewCardDetails(`+ Test Query-${runId} + Edited Query-${runId}`);
  });

  it("go to queries tab on dataset page then delete the query and verify that query should be gone", () => {
    deleteQuery();
    cy.ensureTextNotPresent(`+ Test Query-${runId} + Edited Query-${runId}`);
    cy.ensureTextNotPresent(`Edited Table-${runId}`);
    cy.ensureTextNotPresent(`Edited Description-${runId}`);
  });
});
