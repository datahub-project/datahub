export function clickColumn(column) {
  cy.get(`[id="column-${column}"]`).click({ force: true });
}

export function updateColumnDescription(description) {
  cy.clickOptionWithTestId("edit-field-description");
  cy.getWithTestId("description-editor").clear().type(description);
  cy.clickOptionWithTestId("description-modal-update-button");
}

export function ensureColumnDescriptionInSchemaTable(column, description) {
  cy.get(`[id="column-${column}"]`).within(() => {
    cy.get(".description-column").within(() => {
      cy.getWithTestId("compact-markdown-viewver").should(
        "have.text",
        description,
      );
    });
  });
}

export function ensureColumnDescriptionInSidebar(description) {
  cy.getWithTestId("sidebar-section-content-Description").should(
    "have.text",
    description,
  );
}

export function clearColumnsDescription() {
  cy.clickOptionWithTestId("edit-field-description");
  cy.getWithTestId("description-editor").clear();
  cy.clickOptionWithTestId("description-modal-update-button");
}

export function openDatasetTab(tab) {
  cy.clickOptionWithTestId(`${tab}-entity-tab-header`);
}

export function addDatasetDocumentation(documentation) {
  cy.clickOptionWithTestId("add-documentation");
  cy.getWithTestId("description-editor")
    .should("be.visible")
    .within(() => {
      cy.get(".remirror-editor-wrapper").clear().type(documentation);
    });
  cy.clickOptionWithTestId("description-editor-save-button");
}

export function getSidebarSection(section) {
  return cy.getWithTestId(`sidebar-section-content-${section}`);
}

export function ensureDatasetHasDocumentationInSidebar(documentation) {
  getSidebarSection("Documentation").within(() => {
    cy.get(".remirror-editor-wrapper").should("have.text", documentation);
  });
}

export function ensureDatasetHasDocumentation(documentation) {
  cy.getWithTestId("documentation-editor-content").should(
    "have.text",
    documentation,
  );
}

export function clearDatasetDocumentation() {
  cy.clickOptionWithTestId("edit-documentation-button");

  cy.getWithTestId("description-editor")
    .should("be.visible")
    .within(() => {
      cy.get(".remirror-editor").clear();
    });
  cy.clickOptionWithTestId("description-editor-save-button");
}
