import { BaseChartHelper } from "./baseChartHelper";

export class ChartHelper extends BaseChartHelper {
  TIME_RANGE_WEEK = "WEEK";

  TIME_RANGE_MONTH = "MONTH";

  TIME_RANGE_QUARTER = "QUARTER";

  TIME_RANGE_HALF_OF_YEAR = "HALF_OF_YEAR";

  TIME_RANGE_YEAR = "YEAR";

  timeRangeSelectTestId = "timerange-select";

  ensureTineRangeSelectHasSelectedValue(value) {
    this.getChartCard()
      .scrollIntoView()
      .within(() => {
        this.getTimeRangeSelect()
          .scrollIntoView()
          .within(() => {
            cy.getWithTestId(`value-${value}`).should("exist");
          });
      });
  }

  // Should be called within chart card only
  getTimeRangeSelect() {
    return cy.getWithTestId(this.timeRangeSelectTestId);
  }

  ensureTimeRangeSelectExists() {
    this.getChartCard().within(() => {
      ChartHelper.getTimeRangeSelect().should("exist");
    });
  }

  ensureTimeRangeSelectDoesNotExist() {
    this.getChartCard()
      .scrollIntoView()
      .within(() => {
        ChartHelper.getTimeRangeSelect().should("not.exist");
      });
  }

  toggleTimeRangeSelect() {
    this.getChartCard().within(() => {
      ChartHelper.getTimeRangeSelect().click();
    });
  }

  ensureTimeRangeSelectHasOptions(options) {
    this.toggleTimeRangeSelect();

    options.forEach((option) =>
      cy.getWithTestId(`option-${option}`).should("exist"),
    );

    this.toggleTimeRangeSelect();
  }

  ensureTimeRangeSelectHasNotOptions(options) {
    this.toggleTimeRangeSelect();

    options.forEach((option) =>
      cy.getWithTestId(`option-${option}`).should("not.exist"),
    );

    this.toggleTimeRangeSelect();
  }
}
