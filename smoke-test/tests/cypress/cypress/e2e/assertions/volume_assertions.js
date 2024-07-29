import { aliasQuery, hasOperationName } from "../utils";

const datasetUrn =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD)";
const datasetName = "daily_temperature";

const enableButtonWithId = (id) => {
  cy.get(id).should("be.enabled");
};

const setAssertionMonitorsFlag = (isOn) => {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.reply((res) => {
        // Modify the response body directly
        res.body.data.appConfig.featureFlags.assertionMonitorsEnabled = isOn;
      });
    }
  });
};

const clickElement = (locator) => {
  cy.get(locator).click();
};

describe("create and manage volume assertion", () => {
  beforeEach(() => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  const verifyAssertionCount = (operation) => {
    let beforeCount;
    let afterCount;
    cy.contains("Assertions (")
      .invoke("text")
      .then((text) => {
        beforeCount = parseInt(text.match(/\d+/)[0]);
        cy.reload();
        cy.waitTextVisible("daily_temperature");
        cy.wait(3000);
        cy.contains("Assertions (")
          .invoke("text")
          .then((text) => {
            afterCount = parseInt(text.match(/\d+/)[0]);
            const expectedCount =
              operation === "add" ? beforeCount + 1 : beforeCount - 1;
            expect(afterCount).equals(expectedCount);
          });
      });
  };

  it("create volume assertion, stop and restart monitor, manage and remove assertion", () => {
    // create volume assertion, submit, verify assertion on ui
    setAssertionMonitorsFlag(true);
    cy.loginWithCredentials();
    cy.goToDataset(datasetUrn, datasetName);
    cy.openEntityTab("Quality");
    cy.waitTextVisible("No assertions have run");
    enableButtonWithId("#create-assertion-btn-main");
    clickElement("#create-assertion-btn-main");
    cy.waitTextVisible("New Assertion Monitor");
    cy.contains("h4", "Volume").should("be.visible").click();
    cy.waitTextVisible("Check table volume");
    cy.get("button").contains("Next").click();
    cy.waitTextVisible("Please select an option");
    clickElement("#volume-type");
    cy.clickOptionWithText("Is less than or equal to");
    cy.waitTextVisible("If this assertion fails...");
    cy.waitTextVisible("If this assertion passes...");
    cy.get("button").contains("Next").click();
    cy.waitTextVisible(
      "If not specified, a name will be generated from the assertion settings.",
    );
    cy.get("button").contains("Save").click();
    verifyAssertionCount("add");
    cy.waitTextVisible("Table has at most 1,000 rows");
    cy.get(".ant-table-row-level-0").last().click();
    cy.waitTextVisible("Row count over time");
    cy.waitTextVisible("Runs at 0 minutes past the hour, every 6 hours.");

    // stop the monitor, verify that assertion stopped successfully
    clickElement("body");
    cy.get(".ant-table-cell").find("button").first().click();
    cy.waitTextVisible("Stopped!");
    cy.ensureTextNotPresent("Stopped!");
    cy.get(".ant-tooltip-inner").contains("Start").should("be.visible");

    // restart the monitor, verify that assertion restarted successfully
    cy.waitTextVisible("Table has at most 1,000 rows");
    clickElement('[aria-label="caret-right"]');
    cy.waitTextVisible("Start Monitoring");
    cy.get("button").contains("Yes").click();
    cy.waitTextVisible("Started!");
    cy.ensureTextNotPresent("Started!");
    cy.get(".ant-table-cell").find("button").first().trigger("mouseover");
    cy.get(".ant-tooltip-inner").contains("Stop").should("be.visible");
    cy.get(".ant-tooltip-inner").contains("Start").should("not.exist");

    // manage the assertion and save result
    cy.get(".ant-table-row-level-0").last().click();
    cy.waitTextVisible("Row count over time");
    cy.get(".ant-drawer-content").contains("Settings").click();
    cy.get('[aria-label="edit"]').last().click();
    cy.contains("Auto-raise incident").click();
    clickElement(".anticon-save");
    cy.waitTextVisible("Updated!");
    cy.ensureTextNotPresent("Updated!");
    clickElement("body");
    cy.get(".ant-table-row-level-0").last().click();
    cy.waitTextVisible("Row count over time");
    cy.get(".ant-drawer-content").contains("Settings").click();
    cy.get('[aria-label="edit"]').last().click();
    cy.contains("Auto-raise incident").click();
    clickElement(".anticon-save");
    cy.waitTextVisible("Updated!");
    cy.ensureTextNotPresent("Updated!");

    // remove assertion
    clickElement("body");
    cy.waitTextVisible("Table has at most 1,000 rows");
    cy.get(".ant-table-cell").find("button").last().click();
    cy.waitTextVisible("Confirm Assertion Removal");
    cy.get("button").contains("Yes").click();
    cy.waitTextVisible("Removed assertion.");
    verifyAssertionCount("remove");
  });
});
