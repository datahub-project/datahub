import { aliasQuery, hasOperationName } from "../utils";

describe("personal notifications test", () => {
  before(() => {
    cy.on("uncaught:exception", () => false);
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  const setEmailNotificationsEnabled = (isOn) => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.reply((res) => {
          res.body.data.appConfig.featureFlags.emailNotificationsEnabled = isOn;
          res.body.data.appConfig.featureFlags.themeV2Enabled = true;
          res.body.data.appConfig.featureFlags.themeV2Default = true;
        });
      }
    });
  };

  it("sanity check: enable email notifications and toggle the scenario settings", () => {
    setEmailNotificationsEnabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();

    // Navigate to notification settings
    cy.goToMyNotificationSettings();

    // Enable Email Notifications if not already enabled
    cy.get('[data-testid="email-notifications-enabled-switch"]')
      .invoke("attr", "aria-checked")
      .then((isChecked) => {
        if (isChecked !== "true") {
          cy.clickOptionWithTestId("email-notifications-enabled-switch");
        }
      });

    // Check if Edit Email button exists before clicking
    cy.get("body").then(($body) => {
      if (
        $body.find('[data-testid="email-notifications-edit-email-button"]')
          .length
      ) {
        cy.get('[data-testid="email-notifications-edit-email-button"]').click();
      }
    });

    // Proceed with email input (whether edit button was clicked or not)
    cy.get('[data-testid="email-notifications-edit-email-input"]')
      .clear()
      .type("test@test.com");

    // Click Save Email button
    cy.get('[data-testid="email-notifications-save-email-button"]').click();

    // Enable scenario settings while ensuring no error messages appear
    const notificationTypes = [
      "notification-type-email-proposer_proposal_status_change",
      "notification-type-email-new_proposal",
      "notification-type-email-proposal_status_change",
    ];

    notificationTypes.forEach((testId) => {
      cy.get(`[data-testid="${testId}"]`)
        .invoke("attr", "aria-checked")
        .then((isChecked) => {
          if (isChecked !== "true") {
            cy.clickOptionWithTestId(testId);
          }
        });

      cy.contains("Failed to update settings").should("not.exist");
    });
  });
});
