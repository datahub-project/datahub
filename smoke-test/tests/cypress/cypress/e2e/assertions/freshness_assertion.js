import { aliasQuery, hasOperationName } from "../utils";
const datasetUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD)";
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
      //create freshness assertion, submit, verify assertion on ui
      setAssertionMonitorsFlag(true);
      cy.loginWithCredentials();
      cy.goToDataset(datasetUrn, datasetName);
      cy.openEntityTab("Validation");
      cy.waitTextVisible("No assertions have run");
      cy.clickOptionWithText("Create Assertion");
      cy.waitTextVisible("New Assertion Monitor");
      cy.clickOptionWithText("Freshness");
      cy.waitTextVisible("Check for table changes");
      cy.get("button").contains("Next").click();
      cy.waitTextVisible("If this assertion fails...");
      cy.waitTextVisible("If this assertion passes...");
      cy.get("button").contains("Save").click();
      cy.waitTextVisible("Created new Assertion Monitor!");
      cy.waitTextVisible("Assertions (1)");
      cy.clickOptionWithText("Freshness");
      cy.waitTextVisible("No Evaluations");
      cy.waitTextVisible("Dataset is updated at 0 minutes past the hour, every 6 hours");
      cy.clickOptionWithText("No Evaluations");
      cy.waitTextVisible("Evaluations");
      cy.waitTextVisible("Runs at 0 minutes past the hour, every 6 hours.");
      //stop the monitor, verify that assertion stopped successfully
      cy.goToDataset(datasetUrn, datasetName);
      cy.openEntityTab("Validation");
      cy.waitTextVisible("Assertions (1)");
      cy.clickOptionWithText("Freshness");
      cy.waitTextVisible("Dataset is updated at 0 minutes past the hour, every 6 hours ");
      cy.get('[data-icon="more"]').eq(1).click();
      cy.get('[role="menuitem"]').contains("Stop").click();
      cy.waitTextVisible("Stopped assertion.");
      cy.waitTextVisible("Not running");
      cy.ensureTextNotPresent("No Evaluations");
      cy.get("button").contains("TURN ON").should("be.visible");
      //restart the monitor, verify that assertion restarted successfully
      cy.goToDataset(datasetUrn, datasetName);
      cy.openEntityTab("Validation");
      cy.waitTextVisible("Assertions (1)");
      cy.clickOptionWithText("Freshness");
      cy.waitTextVisible("Dataset is updated at 0 minutes past the hour, every 6 hours ");
      cy.get("button").contains("TURN ON").click();
      cy.waitTextVisible("Start Assertion Monitoring");
      cy.get("button").contains("Yes").click();
      cy.waitTextVisible("Started assertion.");
      cy.ensureTextNotPresent("Not running");
      cy.waitTextVisible("No Evaluations");
      cy.ensureTextNotPresent("TURN ON");
      //manage the assertion and save result
      cy.goToDataset(datasetUrn, datasetName);
      cy.openEntityTab("Validation");
      cy.waitTextVisible("Assertions (1)");
      cy.clickOptionWithText("Freshness");
      cy.waitTextVisible("Dataset is updated at 0 minutes past the hour, every 6 hours ");
      cy.get('[data-icon="more"]').eq(1).click();
      cy.get('[role="menuitem"]').contains("Manage").click();
      cy.waitTextVisible("Manage Assertion");
      cy.clickOptionWithText("Auto-raise incident").wait(1000);
      cy.get("button").contains("Save").click();
      cy.waitTextVisible("Updated Assertion!");
      //refresh the page, verify that the updates are reflected correctly in manage assertion modal
      cy.goToDataset(datasetUrn, datasetName);
      cy.openEntityTab("Validation");
      cy.waitTextVisible("Assertions (1)");
      cy.clickOptionWithText("Freshness");
      cy.waitTextVisible("Dataset is updated at 0 minutes past the hour, every 6 hours ");
      cy.get('[data-icon="more"]').eq(1).click();
      cy.get('[role="menuitem"]').contains("Manage").click();
      cy.waitTextVisible("Manage Assertion");
      cy.get(".ant-checkbox-checked").next().should("have.text", "Auto-raise incident");
      cy.get("button").contains("Cancel").click();
      //remove assertion
      cy.goToDataset(datasetUrn, datasetName);
      cy.openEntityTab("Validation");
      cy.waitTextVisible("Assertions (1)");
      cy.clickOptionWithText("Freshness");
      cy.waitTextVisible("Dataset is updated at 0 minutes past the hour, every 6 hours ");
      cy.get('[data-icon="more"]').eq(1).click();
      cy.get('[role="menuitem"]').contains("Delete").click();
      cy.waitTextVisible("Confirm Assertion Removal");
      cy.get("button").contains("Yes").click();
      cy.waitTextVisible("Removed assertion.");
      cy.waitTextVisible("Assertions (0)");
      cy.ensureTextNotPresent("Freshness");
      cy.waitTextVisible("No Assertions Found");
    });
});