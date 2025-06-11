import { hasOperationName } from "../utils";

const DATASET_ENTITY_TYPE = "dataset";
const DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)";

function updateEnvVars() {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.alias = "gqlappConfigQuery";

      req.on("response", (res) => {
        res.body.data.appConfig.featureFlags.themeV2Enabled = false;
        res.body.data.appConfig.featureFlags.datasetHealthDashboardEnabled = false;
      });
    } else if (hasOperationName(req, "getMe")) {
      req.alias = "gqlgetMeQuery";
      req.on("response", (res) => {
        res.body.data.me.corpUser.settings.appearance.showThemeV2 = false;
      });
    }
  });
}

describe("column-level lineage graph test", () => {
  beforeEach(() => {
    updateEnvVars();
  });
  it("navigate to lineage graph view and verify that column-level lineage is showing correctly", () => {
    cy.login();
    cy.goToEntityLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);
    // verify columns not shown by default
    cy.waitTextVisible("SampleCypressHdfs");
    cy.waitTextVisible("SampleCypressHive");
    cy.waitTextVisible("cypress_logging");
    cy.ensureTextNotPresent("shipment_info");
    cy.ensureTextNotPresent("field_foo");
    cy.ensureTextNotPresent("field_baz");
    cy.ensureTextNotPresent("event_name");
    cy.ensureTextNotPresent("event_data");
    cy.ensureTextNotPresent("timestamp");
    cy.ensureTextNotPresent("browser");
    cy.clickOptionWithTestId("column-toggle");
    // verify columns appear and belong co correct dataset
    cy.waitTextVisible("shipment_info");
    cy.waitTextVisible("shipment_info.date");
    cy.waitTextVisible("shipment_info.target");
    cy.waitTextVisible("shipment_info.destination");
    cy.waitTextVisible("shipment_info.geo_info");
    cy.waitTextVisible("field_foo");
    cy.waitTextVisible("field_baz");
    cy.waitTextVisible("event_name");
    cy.waitTextVisible("event_data");
    cy.waitTextVisible("timestamp");
    cy.waitTextVisible("browser");
    // verify columns can be hidden and shown again
    cy.contains("Hide").click({ force: true });
    cy.ensureTextNotPresent("field_foo");
    cy.ensureTextNotPresent("field_baz");
    cy.get("[aria-label='down']").eq(1).click({ force: true });
    cy.waitTextVisible("field_foo");
    cy.waitTextVisible("field_baz");
    // verify columns can be disabled successfully
    cy.clickOptionWithTestId("column-toggle");
    cy.ensureTextNotPresent("shipment_info");
    cy.ensureTextNotPresent("field_foo");
    cy.ensureTextNotPresent("field_baz");
    cy.ensureTextNotPresent("event_name");
    cy.ensureTextNotPresent("event_data");
    cy.ensureTextNotPresent("timestamp");
    cy.ensureTextNotPresent("browser");
  });
});
