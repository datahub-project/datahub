import { aliasQuery, hasOperationName } from "../utils";

const test_id = Math.floor(Math.random() * 100000);
const test_email = `${test_id}@acryl.io`;
const datasetUrn =
  "urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD)";
const datasetName = "daily_temperature";
const group_name = `Test group ${test_id}`;
const username = Cypress.env("ADMIN_USERNAME");

describe("group subscription test", () => {
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
          res.body.data.appConfig.featureFlags.subscriptionsEnabled = isOn;
          res.body.data.appConfig.featureFlags.emailNotificationsEnabled = isOn;
        });
      }
    });
  };

  it.skip("subscribe group to entity, edit and remove subscription", () => {
    // Configure a slack integration in settings
    setSubscriptionsEnabledFlag(true);
    cy.loginWithCredentials();
    cy.goToIntegrationsSettings();
    cy.clickOptionWithText("Slack");
    cy.clickOptionWithText("Bot Token");
    cy.enterTextInTestId("bot-token-input", test_id);
    cy.enterTextInTestId("default-channel-input", test_id);
    cy.clickOptionWithTestId("connect-to-slack-button");
    cy.waitTextVisible("Updated Slack Settings!");

    // Create a group
    cy.visit("/settings/identities/groups");
    cy.clickOptionWithTestId("create-group-button");
    cy.enterTextInTestId("modal-group-name-input", group_name);
    cy.enterTextInTestId(
      "modal-group-description-input",
      "Test group description",
    );
    cy.contains("Advanced").click();
    cy.enterTextInTestId("group-id-input", test_id);
    cy.clickOptionWithTestId("modal-create-group-button");
    cy.waitTextVisible("Created group!");
    cy.waitTextVisible(group_name);
    cy.logout();

    // Add user to a group
    cy.loginWithCredentials();
    cy.visit(`/group/urn:li:corpGroup:${test_id}/members`);
    cy.clickOptionWithTestId("add-group-member-button");
    cy.clickOptionWithTestId("search-for-users-input");
    cy.enterTextInTestId("search-for-users-input", username);
    cy.get(".ant-select-item-option")
      .contains(username, { matchCase: false })
      .click();
    cy.focused().blur();
    cy.clickOptionWithTestId("modal-add-member-button");
    cy.waitTextVisible("Group members added!");
    cy.contains(username, { timeout: 10000, matchCase: false }).should(
      "be.visible",
    );

    // Subscribe group to dataset
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithTestId("subscription-dropdown");
    cy.clickOptionWithText("Manage Group Subscriptions");
    cy.openMultiSelect("select-group-dropdown");
    cy.clickOptionWithText(group_name);

    cy.get(".ant-tree-checkbox").click({ multiple: true });

    // Slack
    cy.get('[data-testid="slack-channel-edit-button"]').click();
    cy.enterTextInTestId("alternative-slack-member-id", test_id);

    // Email
    cy.get('[data-testid="email-channel-edit-button"]').click();
    cy.enterTextInTestId("alternative-email", test_email);

    cy.clickOptionWithTestId("subscribe-button");
    cy.waitTextVisible("Your group is now subscribed to this entity.").wait(
      3000,
    );

    // Verify subscription in not present in My Subscriptions
    cy.goToSubscriptionsSettings();
    cy.ensureTextNotPresent(datasetName);

    // Edit subscription, verify that changes applied successfully
    cy.visit(`/group/urn:li:corpGroup:${test_id}/subscriptions`);
    cy.waitTextVisible(datasetName);
    cy.get('.ant-table-row [data-icon="edit"]').click();
    cy.get(".ant-tree-checkbox").click({ multiple: true });
    cy.clickOptionWithTestId("subscribe-button");
    cy.waitTextVisible(
      "You have updated the subscription to this entity for your group.",
    ).wait(3000);

    // Unsubscribe from the dataset page, verify that changes applied successfully
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithTestId("subscription-dropdown");
    cy.clickOptionWithText("Manage Group Subscription");

    cy.openMultiSelect("select-group-dropdown");
    cy.clickOptionWithText(group_name);

    cy.get("[data-testid='cancel-button']").contains("Unsubscribe").click();
    cy.waitTextVisible(
      "You have unsubscribed your group from this entity.",
    ).wait(3000);
    cy.visit(`/group/urn:li:corpGroup:${test_id}/subscriptions`);
    cy.ensureTextNotPresent(datasetName);
    cy.waitTextVisible("You are not currently subscribed to any entities.");

    // Remove Slack integration in Settings
    cy.goToIntegrationsSettings();
    cy.clickOptionWithText("Slack");
    cy.waitTextVisible("Configure an integration with Slack");
    cy.clickOptionWithText("Bot Token");
    cy.get('[data-testid="bot-token-input"]').clear();
    cy.get('[data-testid="default-channel-input"]').clear();
    cy.clickOptionWithTestId("connect-to-slack-button");
    cy.waitTextVisible("Updated Slack Settings!");

    // Remove a group -- todo remove this if it continues to fail
    cy.visit("/settings/identities/groups");
    cy.clickOptionWithTestId("entity-header-dropdown");
    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    cy.waitTextVisible("Deleted Group!");
    cy.ensureTextNotPresent(`Test group EDITED ${test_id}`);
  });
});
