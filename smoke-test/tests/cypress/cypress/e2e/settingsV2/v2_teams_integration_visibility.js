import { aliasQuery, hasOperationName } from "../utils";

describe("teams integration visibility test", () => {
  before(() => {
    cy.on("uncaught:exception", () => false);
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  const setTeamsNotificationsEnabled = (isEnabled) => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      console.log("Intercepting GraphQL request:", req.body.operationName);

      if (hasOperationName(req, "appConfig")) {
        console.log("Found appConfig request, setting up response intercept");
        req.alias = "gqlappConfigQuery";
        req.on("response", (res) => {
          console.log("Original response:", res.body);

          // Safely access nested properties
          if (res.body && res.body.data && res.body.data.appConfig) {
            const appConfig = res.body.data.appConfig;

            // Ensure featureFlags exists
            if (!appConfig.featureFlags) {
              appConfig.featureFlags = {};
            }

            console.log("Setting teamsNotificationsEnabled to:", isEnabled);
            appConfig.featureFlags.teamsNotificationsEnabled = isEnabled;
            appConfig.featureFlags.themeV2Enabled = true;
            appConfig.featureFlags.themeV2Default = true;

            console.log("Modified response:", res.body);

            // Ensure no global Teams settings are present
            if (!isEnabled) {
              if (appConfig.integrationSettings) {
                appConfig.integrationSettings.teamsSettings = null;
              }
            }
          } else {
            console.log("Response structure unexpected:", res.body);
          }
        });
      }
    });
  };

  it("should hide Teams option from Platform Integrations page when Teams is disabled", () => {
    setTeamsNotificationsEnabled(false);
    cy.loginWithCredentials();
    cy.skipIntroducePage();

    // Navigate to platform integrations settings
    cy.visit("/settings/integrations");
    cy.waitTextVisible("Manage integrations with third party tools");

    // Wait for page to fully load
    cy.wait(2000);

    // Verify Teams integration option is NOT visible
    cy.get("body").should("not.contain", "Microsoft Teams");
    cy.get("body").should("not.contain", "Connect Microsoft Teams");

    // Verify other integrations are still visible (Slack should be there)
    cy.contains("Slack").should("be.visible");
  });

  it("should hide Teams notification options from Personal Notifications Settings when Teams is disabled", () => {
    setTeamsNotificationsEnabled(false);
    cy.loginWithCredentials();
    cy.skipIntroducePage();

    // Navigate to personal notification settings
    cy.visit("/settings/personal-notifications");
    cy.waitTextVisible("My Notifications");

    // Check that Teams column is not visible anywhere in the table
    cy.contains("Teams").should("not.exist");
    cy.contains("Microsoft Teams").should("not.exist");

    // Verify other notification options are still visible
    cy.contains("Email").should("be.visible");
    cy.contains("Slack").should("be.visible");
  });

  it("should show Teams options when Teams is enabled", () => {
    setTeamsNotificationsEnabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();

    // Navigate to platform integrations settings
    cy.visit("/settings/integrations");
    cy.waitTextVisible("Manage integrations with third party tools");

    // Verify Teams integration option IS visible
    cy.contains("Teams").should("be.visible");

    // Navigate to personal notification settings
    cy.visit("/settings/personal-notifications");
    cy.waitTextVisible("My Notifications");

    // Teams notification section should be visible (though may be disabled without configuration)
    cy.contains("Teams").should("be.visible");
  });

  // it.only("should hide Teams options from subscription drawer when Teams is disabled", () => {
  //   setTeamsNotificationsEnabled(false);
  //   cy.loginWithCredentials();
  //   cy.skipIntroducePage();

  //   // Navigate to a dataset and open subscription drawer
  //   cy.visit(
  //     "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)/Schema",
  //   );
  //   cy.waitTextVisible("Schema");

  //   // Open the subscription drawer
  //   cy.get('[data-testid="bell-icon"]').should("be.visible").click();
  //   cy.waitTextVisible("Subscribe to notifications");

  //   // Verify Teams is not available as a notification option
  //   cy.get('[data-testid="notification-drawer"]').within(() => {
  //     cy.contains("Microsoft Teams").should("not.exist");
  //     cy.contains("Teams").should("not.exist");

  //     // Verify other notification options are available
  //     cy.contains("Email").should("be.visible");
  //   });

  //   // Close the drawer
  //   cy.get('[data-testid="subscription-drawer-close"]').click();
  // });

  it("should hide Teams defaults from Platform Notification Scenarios Settings when Teams is disabled", () => {
    setTeamsNotificationsEnabled(false);
    cy.loginWithCredentials();
    cy.skipIntroducePage();

    // Navigate to platform notification settings (admin feature)
    cy.visit("/settings/notifications/scenarios");

    cy.contains("Teams").should("not.exist");
  });

  it("should show Teams options from Platform Notification Scenarios Settings when Teams is enabled", () => {
    setTeamsNotificationsEnabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();

    // Navigate to platform notification settings (admin feature)
    cy.visit("/settings/notifications/scenarios");

    cy.contains("Teams").should("be.visible");
  });
});
