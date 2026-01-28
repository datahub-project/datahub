import { BaseChartHelper } from "./baseChartHelper";

export class ChangeHistoryChartHelper extends BaseChartHelper {
  OPERATION_TYPE_CREATE = "CREATE";

  OPERATION_TYPE_UPDATE = "UPDATE";

  OPERATION_TYPE_DELETE = "DELETE";

  OPERATION_TYPE_INSERT = "INSERT";

  OPERATION_TYPE_ALTER = "ALTER";

  OPERATION_TYPE_DROP = "DROP";

  drawer_testid = "change-history-details-drawer-title-container";

  static getDay(day) {
    return cy.getWithTestId(`day-${day}`);
  }

  openDayPopover(day) {
    this.getChartCard().within(() => {
      ChangeHistoryChartHelper.getDay(day)
        .scrollIntoView()
        .trigger("mouseover");
    });
  }

  closeDayPopover(day) {
    this.getChartCard().within(() => {
      ChangeHistoryChartHelper.getDay(day).scrollIntoView().trigger("mouseout");
    });
  }

  static getDayPopover(day) {
    return cy.getWithTestId(`day-popover-${day}`);
  }

  ensureDayPopoverHasValue(day, totalChangesText, changes) {
    this.openDayPopover(day);
    ChangeHistoryChartHelper.getDayPopover(day).within(() => {
      cy.getWithTestId("total-changes").should("contain", totalChangesText);
      changes.forEach((change) => {
        cy.getWithTestId(`operation-${change.type}`).within(() => {
          cy.getWithTestId("operation-name").should("contain", change.name);
          cy.getWithTestId("operation-value").should("contain", change.value);
        });
      });
    });
    this.closeDayPopover(day);
  }

  ensureDayPopoverHasNoOperations(day) {
    this.openDayPopover(day);
    ChangeHistoryChartHelper.getDayPopover(day).within(() => {
      cy.getWithTestId("no-changes-this-day").should("be.visible");
    });
    this.closeDayPopover(day);
  }

  ensureDayPopoverHasNoDataReported(day) {
    this.openDayPopover(day);
    ChangeHistoryChartHelper.getDayPopover(day).within(() => {
      cy.getWithTestId("no-data-reported").should("be.visible");
    });
    this.closeDayPopover(day);
  }

  toggleTypesSelect() {
    this.getChartCard().within(() => {
      cy.waitTextVisible("Change Type");
      cy.getWithTestId("types-select").click();
    });
  }

  static toggleTypesSelectOption(changeType) {
    cy.clickOptionWithTestId(`option-${changeType}`);
  }

  toggleTypesSelectOptions(changeTypes) {
    this.toggleTypesSelect();
    changeTypes.forEach((changeType) => {
      ChangeHistoryChartHelper.toggleTypesSelectOption(changeType);
    });
    this.toggleTypesSelect();
  }

  static getSummaryPill(changeType) {
    return cy.getWithTestId(`summary-pill-${changeType}`);
  }

  toggleSummaryPill(changeType) {
    this.getChartCard().within(() => {
      ChangeHistoryChartHelper.getSummaryPill(changeType).click();
    });
  }

  openDayDrawer(day) {
    this.getChartCard().within(() => {
      ChangeHistoryChartHelper.getDay(day).scrollIntoView().click();
    });
  }

  getDrawer() {
    return cy.getWithTestId(this.drawer_testid);
  }

  ensureDayDrawerIsVisible() {
    this.getDrawer().should("be.visible");
  }

  closeDayDrawer() {
    this.getDrawer().within(() => {
      cy.getWithTestId("drawer-close-button").click();
    });
  }
}
