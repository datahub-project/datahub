const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)';

describe("manage queries", () => {
  beforeEach(() => {
    cy.login();
    cy.goToDataset(
      DATASET_URN,
      "SampleCypressHdfsDataset"
    );
    cy.hideOnboardingTour();
    cy.openEntityTab("Queries")
  })

  it("go to querys tab on dataset page then, create, edit, make default, delete a view", () => {
    const runId = Date.now()

    // Headers
    cy.waitTextVisible("Highlighted Queries");
    cy.ensureTextNotPresent("Recent Queries");

    // Add new Query
    cy.get('[data-testid="add-query-button"]').click();
    cy.get('[class="query-builder-editor-input"]').click();
    cy.get('[class="query-builder-editor-input"]').type(` + Test Query-${runId}`);
    cy.get('[data-testid="query-builder-title-input"]').click();
    cy.get('[data-testid="query-builder-title-input"]').type(`Test Table-${runId}`);
    cy.get('.ProseMirror').click();
    cy.get('.ProseMirror').type(`Test Description-${runId}`);
    cy.get('[data-testid="query-builder-save-button"]').click();

    // Verify the card
    cy.waitTextVisible(`+ Test Query-${runId}`);
    cy.waitTextVisible(`Test Table-${runId}`);
    cy.waitTextVisible(`Test Description-${runId}`);
    cy.waitTextVisible("Created on");

    // View the Query
    cy.get('[data-testid="query-content-0"]').click();
    cy.get('[data-testid="query-modal-close-button"]').click();
    cy.waitTextVisible(`+ Test Query-${runId}`);
    cy.waitTextVisible(`Test Table-${runId}`);
    cy.waitTextVisible(`Test Description-${runId}`);

    // Edit the Query
    cy.get('[data-testid="query-edit-button-0"]').click()
    cy.get('[class="query-builder-editor-input"]').click();
    cy.get('[class="query-builder-editor-input"]').type(` + Edited Query-${runId}`);
    cy.get('[data-testid="query-builder-title-input"]').click();
    cy.get('[data-testid="query-builder-title-input"]').clear();
    cy.get('[data-testid="query-builder-title-input"]').type(`Edited Table-${runId}`);
    cy.get('.ProseMirror').click();
    cy.get('.ProseMirror').clear();
    cy.get('.ProseMirror').type(`Edited Description-${runId}`);
    cy.get('[data-testid="query-builder-save-button"]').click();

    // Verify the card
    cy.waitTextVisible(`+ Test Query-${runId} + Edited Query-${runId}`);
    cy.waitTextVisible(`Edited Description-${runId}`);
    cy.waitTextVisible(`Edited Description-${runId}`);

    // Delete the Query
    cy.get('[data-testid="query-more-button-0"]').click();
    cy.get('[data-testid="query-delete-button-0"]').click();
    cy.contains('Yes').click();

    // Query should be gone
    cy.ensureTextNotPresent(`+ Test Query-${runId} + Edited Query-${runId}`);
    cy.ensureTextNotPresent(`Edited Description-${runId}`);
    cy.ensureTextNotPresent(`Edited Description-${runId}`);
  });
});