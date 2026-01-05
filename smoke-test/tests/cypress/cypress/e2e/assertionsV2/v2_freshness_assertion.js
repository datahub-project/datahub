import { aliasQuery, hasOperationName } from "../utils";

const datasetUrn =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD)";
const datasetName = "daily_temperature";

const clickElement = (locator) => {
  cy.get(locator).click();
};

// Helper to clean up any existing assertions on the dataset before the test runs.
// This ensures tests are resilient to leftover state from previous failed runs.
const cleanupExistingAssertions = () => {
  // Wait for page to load - either assertion rows appear or empty state message
  cy.get(".acryl-assertions-table-row, .ant-empty-description", {
    timeout: 10000,
  }).should("exist");
  cy.get("body").then(($body) => {
    if ($body.find(".acryl-assertions-table-row").length > 0) {
      // Remove assertions one by one
      const removeAssertion = () => {
        cy.get("body").then(($innerBody) => {
          if ($innerBody.find(".acryl-assertions-table-row").length > 0) {
            cy.get(".acryl-assertions-table-row")
              .first()
              .find("button")
              .last()
              .click();
            cy.get(".ant-dropdown-menu-item")
              .find(".anticon-delete")
              .closest(".ant-dropdown-menu-item")
              .click();
            cy.waitTextVisible("Confirm Assertion Removal");
            cy.get("button").contains("Yes").click();
            cy.waitTextVisible("Removed assertion.");
            cy.ensureTextNotPresent("Removed assertion.");
            // Recursively check for more assertions
            removeAssertion();
          }
        });
      };
      removeAssertion();
    }
  });
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
    // Clean up any existing assertions from previous failed test runs
    cleanupExistingAssertions();
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
    cy.waitTextVisible("12:00 am");
    clickElement(".acryl-assertions-table-row");
    cy.waitTextVisible("Freshness check results over time");
    cy.waitTextVisible("12:00 AM");
    // stop the monitor, verify that assertion stopped successfully
    clickElement("body");
    cy.waitTextVisible("12:00 am");
    clickElement('[data-testid="assertion-start-stop-action"]');
    cy.waitTextVisible("Stopped!");
    cy.ensureTextNotPresent("Stopped!");
    cy.get(".ant-popover-inner-content").contains("Start").should("be.visible");
    // restart the monitor, verify that assertion restarted successfully
    cy.waitTextVisible("12:00 am");
    clickElement('[data-testid="assertion-start-icon"]');
    cy.waitTextVisible("Start Monitoring");
    cy.get("button").contains("Yes").click();
    cy.waitTextVisible("Started!");
    cy.ensureTextNotPresent("Started!");
    cy.get('[data-testid="assertion-start-stop-action"]').trigger("mouseover");
    cy.get(".ant-popover-inner-content").contains("Stop").should("be.visible");
    cy.get(".ant-popover-inner-content").contains("Start").should("not.exist");

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
    cy.waitTextVisible("12:00 am");
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
