import { aliasQuery, hasOperationName } from "../utils";

const datasetUrn =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD)";
const datasetName = "daily_temperature";

describe("create and manage freshness assertion", () => {
  beforeEach(() => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

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

  it("create freshness assertion, stop and restart monitor, manage and remove assertion", () => {
    // create freshness assertion, submit, verify assertion on ui
    setAssertionMonitorsFlag(true);
    cy.loginWithCredentials();
    cy.goToDataset(datasetUrn, datasetName);
    cy.openEntityTab("Quality");
    cy.waitTextVisible("No assertions have run");
    cy.get("#create-assertion-btn-main").click();
    cy.waitTextVisible("New Assertion Monitor");
    cy.clickOptionWithText("Freshness");
    cy.waitTextVisible("Schedule checks at");
    cy.waitTextVisible("If this assertion fails...");
    cy.waitTextVisible("If this assertion passes...");
    cy.get("button").contains("Next").click();
    cy.waitTextVisible(
      "If not specified, a name will be generated from the assertion settings."
    );
    cy.get("button").contains("Save").click();
    cy.ensureElementPresent('[data-row-key="Freshness"]');
    cy.clickOptionWithText("Freshness");
    cy.waitTextVisible("as of 0 minutes past the hour, every 6 hours");
    cy.get(".ant-table-row-level-0").last().click();
    cy.waitTextVisible("Schedule details");
    cy.reload();
    cy.waitTextVisible("Assertions (1)");
    cy.waitTextVisible("as of 0 minutes past the hour, every 6 hours ");
    cy.get(".ant-table-cell").find("button").first().click();
    cy.waitTextVisible("Stopped!");
    cy.ensureTextNotPresent("Stopped!");
    cy.get(".ant-tooltip-inner").contains("Start").should("be.visible");
    cy.reload();
    cy.waitTextVisible("Assertions (1)");
    cy.waitTextVisible("as of 0 minutes past the hour, every 6 hours");
    cy.get('[aria-label="caret-right"]').click();
    cy.waitTextVisible("Start Monitoring");
    cy.get("button").contains("Yes").click();
    cy.waitTextVisible("Started!");
    cy.ensureTextNotPresent("Started!");
    cy.get(".ant-table-cell").find("button").first().trigger("mouseover");
    cy.get(".ant-tooltip-inner").contains("Stop").should("be.visible");
    cy.get(".ant-tooltip-inner").contains("Start").should("not.exist");
    cy.reload();
    cy.waitTextVisible("Assertions (1)");
    cy.get(".ant-table-row-level-0").last().click();
    cy.waitTextVisible("Freshness check results over time");
    cy.get(".ant-drawer-content").contains("Settings").click();
    cy.get('[aria-label="edit"]').last().click();
    cy.contains("Auto-raise incident").click();
    cy.get(".anticon-save").click();
    cy.waitTextVisible("Updated!");
    cy.ensureTextNotPresent("Updated!");
    cy.reload();
    cy.waitTextVisible("Assertions (1)");
    cy.get(".ant-table-row-level-0").last().click();
    cy.waitTextVisible("Freshness check results over time");
    cy.get(".ant-drawer-content").contains("Settings").click();
    cy.get('[aria-label="edit"]').last().click();
    cy.contains("Auto-raise incident").click();
    cy.get(".anticon-save").click();
    cy.waitTextVisible("Updated!");
    cy.ensureTextNotPresent("Updated!");
    cy.reload();
    cy.waitTextVisible("Assertions (1)");
    cy.waitTextVisible("as of 0 minutes past the hour, every 6 hours ");
    cy.get(".ant-table-cell").find("button").last().click();
    cy.waitTextVisible("Confirm Assertion Removal");
    cy.get("button").contains("Yes").click();
    cy.waitTextVisible("Removed assertion.");
    cy.reload();
    cy.waitTextVisible("Assertions (0)");
    cy.ensureTextNotPresent("Freshness");
    cy.waitTextVisible("No Assertions Found");
  });
});
