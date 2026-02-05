import { aliasQuery, hasOperationName, getUniqueTestId } from "../utils";

describe("manage service accounts", () => {
  before(() => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  const setFeatureFlags = (inviteUsersEnabled = false) => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.alias = "gqlappConfigQuery";
        req.on("response", (res) => {
          res.body.data.appConfig.authConfig.tokenAuthEnabled = true;
          res.body.data.appConfig.featureFlags.themeV2Enabled = true;
          res.body.data.appConfig.featureFlags.themeV2Default = true;
          res.body.data.appConfig.featureFlags.showNavBarRedesign = true;
          res.body.data.appConfig.featureFlags.inviteUsersEnabled =
            inviteUsersEnabled;
        });
      }
    });
  };

  const navigateToServiceAccountsTab = () => {
    // Navigate to the service accounts tab
    cy.visit("/settings/identities/service-accounts");
    cy.wait(2000);
  };

  it("create, generate token, and delete service account", () => {
    // Use a different ID for this test to avoid conflicts
    const newTestId = getUniqueTestId();
    const newServiceAccountName = `Test Service Account New UI ${newTestId}`;
    const newServiceAccountDescription = `New UI test service account ${newTestId}`;
    const newTokenName = `Test Token New UI ${newTestId}`;
    const newTokenDescription = `New UI test token ${newTestId}`;

    setFeatureFlags(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();

    navigateToServiceAccountsTab();

    // Create service account with new UI
    cy.get('[data-testid="create-service-account-button"]').should(
      "be.visible",
    );
    cy.get('[data-testid="create-service-account-button"]').click();

    cy.get('[data-testid="create-service-account-modal"]').should("be.visible");
    cy.get('[data-testid="service-account-display-name-input"]').type(
      newServiceAccountName,
    );
    cy.get('[data-testid="service-account-description-input"]').type(
      newServiceAccountDescription,
    );
    cy.get('[data-testid="create-service-account-submit-button"]').click();

    cy.waitTextVisible("Service account created successfully!");
    cy.waitTextVisible(newServiceAccountName);

    // Generate access token
    cy.contains(newServiceAccountName)
      .parents("tr")
      .within(() => {
        cy.get('[data-testid^="service-account-menu-"]').click();
      });

    cy.get('[data-testid="menu-item-create-token"]').click({
      force: true,
    });
    cy.get('[data-testid="create-token-modal"]').should("be.visible");
    cy.get('[data-testid="create-access-token-name"]').type(newTokenName);
    cy.get('[data-testid="create-access-token-description"]').type(
      newTokenDescription,
    );
    cy.get('[data-testid="create-access-token-button"]').click();

    cy.waitTextVisible("New Access Token");
    cy.get('[data-testid="access-token-value"]').should("be.visible");
    cy.get('[data-testid="access-token-modal-close-button"]').click();

    // Delete the service account
    navigateToServiceAccountsTab();
    cy.waitTextVisible(newServiceAccountName);

    cy.contains(newServiceAccountName)
      .parents("tr")
      .within(() => {
        cy.get('[data-testid^="service-account-menu-"]').click();
      });

    cy.get('[data-testid="menu-item-delete"]').click({
      force: true,
    });
    cy.get('[data-testid="delete-service-account-modal"]').should("be.visible");
    cy.get('[data-testid="delete-service-account-confirm-button"]').click();

    cy.waitTextVisible("Service account deleted");
    cy.ensureTextNotPresent(newServiceAccountName);
  });

  // Test edge case: Cancel creating service account
  it("cancel creating service account should not create one", () => {
    const cancelTestId = getUniqueTestId();
    const cancelServiceAccountName = `Cancel Test ${cancelTestId}`;

    setFeatureFlags(false);
    cy.loginWithCredentials();
    cy.skipIntroducePage();

    navigateToServiceAccountsTab();

    // Click create button
    cy.get('[data-testid="create-service-account-button"]').click();
    cy.get('[data-testid="create-service-account-modal"]').should("be.visible");

    // Fill in name but cancel
    cy.get('[data-testid="service-account-display-name-input"]').type(
      cancelServiceAccountName,
    );
    cy.get('[data-testid="create-service-account-cancel-button"]').click();

    // Modal should close
    cy.get('[data-testid="create-service-account-modal"]').should("not.exist");

    // Service account should not appear in list
    cy.ensureTextNotPresent(cancelServiceAccountName);
  });

  // Test edge case: Cancel deleting service account
  it("cancel deleting service account should keep it in the list", () => {
    const keepTestId = getUniqueTestId();
    const keepServiceAccountName = `Keep Test ${keepTestId}`;

    setFeatureFlags(false);
    cy.loginWithCredentials();
    cy.skipIntroducePage();

    navigateToServiceAccountsTab();

    // First create a service account
    cy.get('[data-testid="create-service-account-button"]').click();
    cy.get('[data-testid="create-service-account-modal"]').should("be.visible");
    cy.get('[data-testid="service-account-display-name-input"]').type(
      keepServiceAccountName,
    );
    cy.get('[data-testid="create-service-account-submit-button"]').click();
    cy.waitTextVisible("Service account created successfully!");
    cy.waitTextVisible(keepServiceAccountName);

    // Try to delete but cancel
    cy.contains(keepServiceAccountName)
      .parents("tr")
      .within(() => {
        cy.get('[data-testid^="service-account-menu-"]').click();
      });

    cy.get('[data-testid="menu-item-delete"]').click({
      force: true,
    });
    cy.get('[data-testid="delete-service-account-modal"]').should("be.visible");
    cy.get('[data-testid="delete-service-account-cancel-button"]').click();

    // Modal should close
    cy.get('[data-testid="delete-service-account-modal"]').should("not.exist");

    // Service account should still be in list
    cy.waitTextVisible(keepServiceAccountName);

    // Clean up: actually delete it now
    cy.contains(keepServiceAccountName)
      .parents("tr")
      .within(() => {
        cy.get('[data-testid^="service-account-menu-"]').click();
      });

    cy.get('[data-testid="menu-item-delete"]').click({
      force: true,
    });
    cy.get('[data-testid="delete-service-account-confirm-button"]').click();
    cy.waitTextVisible("Service account deleted");
  });
});
