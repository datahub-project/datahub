/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export class BaseChartHelper {
  constructor(chartName) {
    this.chartName = chartName;
  }

  getChartCard() {
    return cy.getWithTestId(`${this.chartName}-card`);
  }

  getChart() {
    return cy.getWithTestId(`${this.chartName}-chart`);
  }

  ensureChartVisible() {
    this.getChart().scrollIntoView().should("be.visible");
  }

  ensureChartHasNoData() {
    this.getChartCard().scrollIntoView();
    cy.getWithTestId(`${this.chartName}-chart-empty`)
      .scrollIntoView()
      .should("be.visible");
  }

  ensureHasNoPermissions() {
    this.getChartCard()
      .scrollIntoView()
      .within(() => {
        cy.getWithTestId("no-permissions").should("exist");
      });
  }
}
