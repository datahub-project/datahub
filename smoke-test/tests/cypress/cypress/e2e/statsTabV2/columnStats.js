import { StatsTabHelper } from "./helpers/statsTabHelper";

const DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)";
const DATASET_NAME = "customers";

const openStatsTabOfSampleEntity = () => {
  StatsTabHelper.goToTab(DATASET_URN, DATASET_NAME);
};

describe("Column stats", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.login();
  });

  it("should show column stats in the table", () => {
    openStatsTabOfSampleEntity();

    StatsTabHelper.columnStatsTable.ensureTableHasValues([
      {
        name: "customer_id",
        fields: {
          column: "customer_id",
          type: "Number",
          nullPercentage: "0%",
          uniqueValues: "100",
        },
      },
      {
        name: "first_name",
        fields: {
          column: "first_name",
          type: "String",
          nullPercentage: "0%",
          uniqueValues: "79",
        },
      },
      {
        name: "last_name",
        fields: {
          column: "last_name",
          type: "String",
          nullPercentage: "0%",
          uniqueValues: "19",
        },
      },
      {
        name: "first_order",
        fields: {
          column: "first_order",
          type: "Date",
          nullPercentage: "38.00%",
          uniqueValues: "46",
          min: "2018-01-01",
          max: "2018-04-07",
        },
      },
      {
        name: "most_recent_order",
        fields: {
          column: "most_recent_order",
          type: "Date",
          nullPercentage: "38.00%",
          uniqueValues: "52",
          min: "2018-01-09",
          max: "2018-04-09",
        },
      },
      {
        name: "number_of_orders",
        fields: {
          column: "number_of_orders",
          type: "Number",
          nullPercentage: "38.00%",
          uniqueValues: "4",
          min: "1",
          max: "5",
        },
      },
      {
        name: "customer_lifetime_value",
        fields: {
          column: "customer_lifetime_value",
          type: "Number",
          nullPercentage: "38.00%",
          uniqueValues: "35",
          min: "1.0",
          max: "99.0",
        },
      },
    ]);
  });

  it("should open the column drawer by clicking on row", () => {
    openStatsTabOfSampleEntity();

    StatsTabHelper.columnStatsTable.clickOnRow("customer_id");
    StatsTabHelper.columnStatsTable.ensureShemaFieldDrawerOpenedOnStatsTab();
  });
});
