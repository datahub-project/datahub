import { aliasQuery, hasOperationName } from "../utils";
const test_id = Math.floor(Math.random() * 100000);
const datasetUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD)";
const datasetName = "daily_temperature";

describe("entity subscription test", () => {
    beforeEach(() => {
      cy.on('uncaught:exception', (err, runnable) => { return false; });
      cy.intercept("POST", "/api/v2/graphql", (req) => {
        aliasQuery(req, "appConfig");
      });
    });
    
    const setSubscriptionsEnabledFlag = (isOn) => {
      cy.intercept("POST", "/api/v2/graphql", (req) => {
        if (hasOperationName(req, "appConfig")) {
          req.reply((res) => {
            // Modify the response body directly
            res.body.data.appConfig.featureFlags.subscriptionsEnabled = isOn;
          });
        }
      });
    };

    it("subscribe to entity, edit and remove subscription", () => {
      //configure slack integration in settings
      setSubscriptionsEnabledFlag(true);
      cy.loginWithCredentials();
      cy.goToIntegrationsSettings();
      cy.clickOptionWithText("Slack");
      cy.waitTextVisible("Configure an integration with Slack");
      cy.clickOptionWithText("Bot Token");
      cy.contains("Enter a Slack bot token").next().type(test_id);
      cy.contains("Set a default channel.").next().type(test_id);
      cy.get("button").contains("Connect to Slack").click();
      cy.waitTextVisible("Updated Slack Settings!");
      //subscribe to dataset
      cy.goToDataset(datasetUrn, datasetName);
      cy.get("#entity-profile-subscriptions").click();
      cy.waitTextVisible(`Subscribe to ${datasetName}`);
      cy.get(".ant-tree-checkbox").click({ multiple: true });
      cy.get('[value="SUBSCRIPTION"]').click();
      cy.get("#slackFormValue").type(test_id);
      cy.get("button").contains("Subscribe & Notify").click();
      cy.waitTextVisible("You are now subscribed to this entity.").wait(3000);
      //verify subscription in settings
      cy.goToSubscriptionsSettings();
      cy.waitTextVisible(datasetName);
      //edit subscription, verify changes applied successfully
      cy.get("*[class^='EditSubscriptionColumn']").click();
      cy.waitTextVisible(`Subscribe to ${datasetName}`);
      cy.get(".ant-tree-checkbox").click({ multiple: true });
      cy.get("button").contains("Update").click();
      cy.waitTextVisible("You have updated your subscription to this entity.").wait(3000);
      //unsubscribe from the dataset page, verify changes applied successfully
      cy.goToDataset(datasetUrn, datasetName);
      cy.get("#entity-profile-subscriptions").click();
      cy.waitTextVisible(`Subscribe to ${datasetName}`);
      cy.get("button").contains("Unsubscribe").click();
      cy.waitTextVisible("You have unsubscribed from this entity.").wait(3000);
      cy.goToSubscriptionsSettings();
      cy.ensureTextNotPresent(datasetName);
      cy.waitTextVisible("You are not currently subscribed to any entities.");
      //remove subscription from my subscriptions settings page
      cy.goToDataset(datasetUrn, datasetName);
      cy.get("#entity-profile-subscriptions").click();
      cy.waitTextVisible(`Subscribe to ${datasetName}`);
      cy.get(".ant-tree-checkbox").click({ multiple: true });
      cy.get('[value="SUBSCRIPTION"]').click();
      cy.get("#slackFormValue").type(test_id);
      cy.get("button").contains("Subscribe & Notify").click();
      cy.waitTextVisible("You are now subscribed to this entity.").wait(3000);
      cy.goToSubscriptionsSettings();
      cy.waitTextVisible(datasetName);
      cy.get("*[class^='EditSubscriptionColumn']").click();
      cy.waitTextVisible(`Subscribe to ${datasetName}`);
      cy.get("button").contains("Unsubscribe").click();
      cy.waitTextVisible("You have unsubscribed from this entity.").wait(5000);
      cy.ensureTextNotPresent(datasetName);
      cy.waitTextVisible("You are not currently subscribed to any entities.");
      //remove slack integration in settings
      cy.goToIntegrationsSettings();
      cy.clickOptionWithText("Slack");
      cy.waitTextVisible("Configure an integration with Slack");
      cy.clickOptionWithText("Bot Token");
      cy.contains("Enter a Slack bot token").next().clear();
      cy.contains("Set a default channel.").next().clear();
      cy.get("button").contains("Re-connect to Slack").click();
      cy.waitTextVisible("Updated Slack Settings!");
    });
});