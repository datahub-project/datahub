import { ChartHelper } from "./chartHelper";
import { HighlightCardHelper } from "./highlightCardHelper";
import { ChangeHistoryChartHelper } from "./changeHistoryChartHelper";
import { ColumnStatsTableHelper } from "./columnStatsTableHelper";

export class StatsTabHelper {
  // cards from highlights
  static rowsCard = new HighlightCardHelper("rows");

  static columnsCard = new HighlightCardHelper("columns");

  static usersCard = new HighlightCardHelper("users");

  static queriesCard = new HighlightCardHelper("queries");

  static changesCard = new HighlightCardHelper("changes");

  // charts
  static rowsCoutChart = new ChartHelper("row-count");

  static queryCountChart = new ChartHelper("query-count");

  static storageSizeChart = new ChartHelper("storage-size");

  static changeHistoryChart = new ChangeHistoryChartHelper("change-history");

  // tables
  static columnStatsTable = new ColumnStatsTableHelper();

  static getTab() {
    return cy.getWithTestId("Stats-entity-tab-header").parent();
  }

  static goToTab(datasetUrn, datasetName, login = false) {
    cy.visit(`/dataset/${datasetUrn}/Stats`);
    cy.wait(3000);
    cy.waitTextVisible(datasetName);
  }

  static clickOnTab() {
    StatsTabHelper.getTab().click();
    cy.wait(3000); // wait for tab is loaded (because some charts could be reordered on UI depending on their data)
  }

  static openToRemove(entityUrn, entityName) {
    cy.goToDataset(entityUrn, entityName);
    StatsTabHelper.clickOnTab();
  }

  static ensureTabIsSelected() {
    StatsTabHelper.getTab().should("have.attr", "aria-selected", "true");
  }

  static ensureTabIsEnabled() {
    StatsTabHelper.getTab().should("not.have.attr", "aria-disabled");
  }

  static ensureTabIsDisabled() {
    StatsTabHelper.getTab().should("have.attr", "aria-disabled", "true");
  }
}
