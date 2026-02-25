export class ColumnStatsTableHelper {
  rowTestIdPrefix = "row-";

  schemaFieldDrawerContentTestId = "schema-field-drawer-content";

  schemaFieldDrawerStatsTabContainerTestId = "stats-tab-container";

  getRow(name) {
    return cy.getWithTestId(`${this.rowTestIdPrefix}${name}`);
  }

  ensureTableHasValues(rows) {
    rows.forEach((row) => {
      this.getRow(row.name)
        .scrollIntoView()
        .within(() => {
          Object.entries(row.fields).forEach(([fieldName, fieldValue]) => {
            cy.getWithTestId(fieldName).should("contain", fieldValue);
          });
        });
    });
  }

  clickOnRow(name) {
    this.getRow(name).click();
  }

  ensureShemaFieldDrawerOpenedOnStatsTab() {
    cy.getWithTestId(this.schemaFieldDrawerContentTestId).should("be.visible");
    cy.getWithTestId(this.schemaFieldDrawerStatsTabContainerTestId).should(
      "be.visible",
    );
  }
}
