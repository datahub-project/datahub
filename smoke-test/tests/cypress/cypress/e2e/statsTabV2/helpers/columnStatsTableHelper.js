/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
