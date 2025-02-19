import { aliasQuery, hasOperationName } from "../utils";

const datasetUrn =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD)";
const datasetName = "daily_temperature";

const clickElement = (locator) => {
  cy.get(locator).click();
};

const setAssertionMonitorsFlag = (isOn) => {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.reply((res) => {
        // Modify the response body directly
        res.body.data.appConfig.featureFlags.assertionMonitorsEnabled = isOn;
        res.body.data.appConfig.featureFlags.themeV2Enabled = true;
        res.body.data.appConfig.featureFlags.themeV2Default = true;
      });
    }
  });
};

describe("create and manage freshness assertion", () => {
  beforeEach(() => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  it("create freshness assertion, stop and restart monitor,manage and remove assertion", () => {
    // create freshness assertion, submit, verify assertion on ui
    setAssertionMonitorsFlag(true);
    cy.goToDataset(datasetUrn, datasetName, true);
    cy.openEntityTab("Quality");
    clickElement("#acryl-validation-tab-assertions-sub-tab");
    cy.waitTextVisible("No assertions have run");
    clickElement("#create-assertion-btn-main");
    cy.waitTextVisible("New Assertion Monitor");
    cy.contains("h4", "Freshness").should("be.visible").click();
    cy.waitTextVisible("Pass if this table has updated...");
    cy.waitTextVisible("If this assertion fails...");
    cy.waitTextVisible("If this assertion passes...");
    cy.get("button").contains("Next").click({ waitForAnimations: true });
    cy.waitTextVisible(
      "If not specified, a name will be generated from the assertion settings.",
    );
    cy.get("button").contains("Save").click();
    cy.waitTextVisible("Created!");
    cy.ensureTextNotPresent("Created!");
    // verifyAssertionCount("add");
    cy.waitTextVisible("as of 0 minutes past the hour, every 6 hours");
    clickElement(".acryl-assertions-table-row");
    cy.waitTextVisible("Freshness check results over time");
    cy.waitTextVisible("Runs at 0 minutes past the hour, every 6 hours.");
    // stop the monitor, verify that assertion stopped successfully
    clickElement("body");
    cy.waitTextVisible("as of 0 minutes past the hour, every 6 hours");
    clickElement('[data-testid="assertion-start-stop-action"]');
    cy.waitTextVisible("Stopped!");
    cy.ensureTextNotPresent("Stopped!");
    cy.get(".ant-tooltip-inner").contains("Start").should("be.visible");
    // restart the monitor, verify that assertion restarted successfully
    cy.waitTextVisible("as of 0 minutes past the hour, every 6 hours");
    clickElement('[aria-label="caret-right"]');
    cy.waitTextVisible("Start Monitoring");
    cy.get("button").contains("Yes").click();
    cy.waitTextVisible("Started!");
    cy.ensureTextNotPresent("Started!");
    cy.get('[data-testid="assertion-start-stop-action"]').trigger("mouseover");
    cy.get(".ant-tooltip-inner").contains("Stop").should("be.visible");
    cy.get(".ant-tooltip-inner").contains("Start").should("not.exist");

    // manage the assertion and save result
    cy.get(".acryl-assertions-table-row").last().click();
    cy.waitTextVisible("Freshness check results over time");
    cy.get(".ant-drawer-content").contains("Settings").click();
    clickElement('[data-testid="edit-assertion-button"]');
    cy.contains("Auto-raise incident").click();
    clickElement('[data-testid="save-assertion-button"]');
    cy.waitTextVisible("Updated!");
    cy.ensureTextNotPresent("Updated!");
    clickElement("body");
    cy.get(".acryl-assertions-table-row").last().click();
    cy.waitTextVisible("Freshness check results over time");
    cy.get(".ant-drawer-content").contains("Settings").click();
    clickElement('[data-testid="edit-assertion-button"]');
    cy.contains("Auto-raise incident").click();
    clickElement('[data-testid="save-assertion-button"]');
    cy.waitTextVisible("Updated!");
    cy.ensureTextNotPresent("Updated!");

    // remove assertion
    clickElement("body");
    cy.waitTextVisible("as of 0 minutes past the hour, every 6 hours ");
    cy.get(".acryl-assertions-table-row").find("button").last().click();
    cy.get(".ant-dropdown-menu-item")
      .find(".anticon-delete")
      .closest(".ant-dropdown-menu-item") // Traverse back up to the parent Menu.Item
      .click();
    cy.waitTextVisible("Confirm Assertion Removal");
    cy.get("button").contains("Yes").click();
    cy.waitTextVisible("Removed assertion.");
    // verifyAssertionCount("remove");
  });
});
