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
