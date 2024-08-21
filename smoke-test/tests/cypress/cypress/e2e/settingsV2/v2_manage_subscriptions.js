import { aliasQuery, hasOperationName } from "../utils";

const test_id = Math.floor(Math.random() * 100000);
const test_email = `${test_id}@acryl.io`;
const datasetUrn =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD)";
const datasetName = "daily_temperature";

describe("entity subscription test", () => {
  beforeEach(() => {
    cy.on("uncaught:exception", (err, runnable) => false);
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
          res.body.data.appConfig.featureFlags.emailNotificationsEnabled = isOn;
          res.body.data.appConfig.featureFlags.slackBotTokensConfigEnabled =
            isOn;
          res.body.data.appConfig.featureFlags.themeV2Enabled = true;
          res.body.data.appConfig.featureFlags.themeV2Default = true;
        });
      }
    });
  };

  const handleNotifications = (value, source, locator) => {
    cy.get(".ant-space-item")
      .eq(value)
      .then(($container) => {
        const numberOfSpans = $container.find("span").length;
        if (numberOfSpans > 1) {
          cy.get(`[data-testid="${source}-channel-edit-button"]`).click();
        } else if (numberOfSpans <= 1) {
          cy.clickOptionWithTestId(locator);
        }
      });
  };
  it("subscribe to entity, edit and remove subscription", () => {
    // Configure a slack integration in settings
    setSubscriptionsEnabledFlag(true);
    cy.loginWithCredentials();
    cy.handleIntroducePage();
    cy.goToIntegrationsSettings();
    cy.waitTextVisible("Users & Groups");
    cy.clickOptionWithText("Slack");
    cy.clickOptionWithText("App Configuration Token");
    cy.clickOptionWithText("Bot Token");
    cy.enterTextInTestId("bot-token-input", test_id);
    cy.clickOptionWithTestId("connect-to-slack-button");

    // Subscribe to dataset
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithTestId("MoreVertOutlinedIcon");
    cy.get("ul.ant-dropdown-menu-light")
      .find(".ant-dropdown-menu-submenu")
      .contains("Subscribe")
      .click();
    cy.clickOptionWithText("Subscribe Me");
    cy.get(".ant-tree-checkbox").click({ multiple: true });

    // Slack
    handleNotifications(1, "slack", "alternative-slack-member-id");
    cy.enterTextInTestId("alternative-slack-member-id", test_id);

    // Email
    handleNotifications(0, "email", "alternative-email");
    cy.enterTextInTestId("alternative-email", test_email);

    cy.clickOptionWithTestId("subscribe-button");
    cy.waitTextVisible("You are now subscribed to this entity.").wait(3000);

    // Verify subscription in settings
    cy.goToSubscriptionsSettings();

    // Edit subscription, verify that changes applied successfully
    cy.get('[data-icon="edit"]').click();
    cy.get(".ant-tree-checkbox").click({ multiple: true });
    cy.clickOptionWithTestId("subscribe-button");
    cy.waitTextVisible(
      "You have updated your subscription to this entity.",
    ).wait(3000);

    // Unsubscribe from the dataset page, verify changes applied successfully
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithTestId("MoreVertOutlinedIcon");
    cy.get("ul.ant-dropdown-menu-light")
      .find(".ant-dropdown-menu-submenu")
      .contains("Subscribe")
      .click();
    cy.clickOptionWithText("Manage My Subscription");
    cy.clickOptionWithTestId("cancel-button");
    cy.waitTextVisible("You have unsubscribed from this entity.").wait(3000);
    cy.goToSubscriptionsSettings();
    cy.ensureTextNotPresent(datasetName);
    cy.waitTextVisible("You are not currently subscribed to any entities.");

    // Remove subscription from my subscriptions settings page
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithTestId("MoreVertOutlinedIcon");
    cy.get("ul.ant-dropdown-menu-light")
      .find(".ant-dropdown-menu-submenu")
      .contains("Subscribe")
      .click();
    cy.clickOptionWithText("Subscribe Me");
    cy.get(".ant-tree-checkbox").click({ multiple: true });

    // Slack
    cy.get('[data-testid="slack-channel-edit-button"]').click();
    cy.enterTextInTestId("alternative-slack-member-id", test_id);

    // Email
    cy.get('[data-testid="email-channel-edit-button"]').click();
    cy.enterTextInTestId("alternative-email", test_email);

    cy.clickOptionWithTestId("subscribe-button");
    cy.waitTextVisible("You are now subscribed to this entity.").wait(3000);
    cy.goToSubscriptionsSettings();
    cy.get('[data-icon="edit"]').click();
    cy.clickOptionWithTestId("cancel-button");
    cy.waitTextVisible("You have unsubscribed from this entity.").wait(5000);
    cy.ensureTextNotPresent(datasetName);
    cy.waitTextVisible("You are not currently subscribed to any entities.");

    // Remove slack integration in settings
    cy.goToIntegrationsSettings();
    cy.clickOptionWithText("Slack");
    cy.waitTextVisible("Users & Groups");
    cy.waitTextVisible("Configure an integration with Slack");
    cy.clickOptionWithText("App Configuration Token");
    cy.clickOptionWithText("Bot Token");
    cy.get('[data-testid="bot-token-input"]').clear();
    cy.clickOptionWithTestId("connect-to-slack-button");
  });
});
