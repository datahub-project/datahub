import { aliasQuery, hasOperationName, getUniqueTestId } from "../utils";

const testId = getUniqueTestId();
const SERVICE_ACCOUNT_NAME = `Test Service Account ${testId}`;
const SERVICE_ACCOUNT_DESCRIPTION = `Automated test service account created at ${testId}`;
const TOKEN_NAME = `Test Token ${testId}`;
const TOKEN_DESCRIPTION = `Automated test token for service account ${testId}`;

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

  const createServiceAccount = () => {
    // Click the create service account button
    cy.get('[data-testid="create-service-account-button"]').should(
      "be.visible",
    );
    cy.get('[data-testid="create-service-account-button"]').click();

    // Wait for modal to appear
    cy.get('[data-testid="create-service-account-modal"]').should("be.visible");

    // Fill in the form
    cy.get('[data-testid="service-account-display-name-input"]').type(
      SERVICE_ACCOUNT_NAME,
    );
    cy.get('[data-testid="service-account-description-input"]').type(
      SERVICE_ACCOUNT_DESCRIPTION,
    );

    // Submit the form
    cy.get('[data-testid="create-service-account-submit-button"]').click();

    // Wait for success message and modal to close
    cy.waitTextVisible("Service account created successfully!");

    // Verify the service account appears in the list
    cy.waitTextVisible(SERVICE_ACCOUNT_NAME);
  };

  const createTokenForServiceAccount = () => {
    // Find the service account row and click its menu
    cy.contains(SERVICE_ACCOUNT_NAME)
      .parents("tr")
      .within(() => {
        cy.get('[data-testid^="service-account-menu-"]').click();
      });

    // Click create token menu item
    cy.get('[data-testid="menu-item-create-token"]').click({
      force: true,
    });

    // Wait for token modal to appear
    cy.get('[data-testid="create-token-modal"]').should("be.visible");

    // Fill in token details
    cy.get('[data-testid="create-access-token-name"]').type(TOKEN_NAME);
    cy.get('[data-testid="create-access-token-description"]').type(
      TOKEN_DESCRIPTION,
    );

    // Create the token
    cy.get('[data-testid="create-access-token-button"]').click();

    // Verify the token was created - should show access token modal
    cy.waitTextVisible("New Access Token");
    cy.get('[data-testid="access-token-value"]').should("be.visible");

    // Verify the token value looks like a JWT
    cy.get('[data-testid="access-token-value"]')
      .invoke("text")
      .should("match", /^[a-zA-Z0-9-_]+\.[a-zA-Z0-9-_]+\.[a-zA-Z0-9-_]+$/);

    // Close the token modal
    cy.get('[data-testid="access-token-modal-close-button"]').click();
  };

  const deleteServiceAccount = () => {
    // Navigate back to service accounts (in case we were redirected to tokens page)
    navigateToServiceAccountsTab();

    // Wait for the service account to be visible
    cy.waitTextVisible(SERVICE_ACCOUNT_NAME);

    // Find the service account row and click its menu
    cy.contains(SERVICE_ACCOUNT_NAME)
      .parents("tr")
      .within(() => {
        cy.get('[data-testid^="service-account-menu-"]').click();
      });

    // Click delete menu item
    cy.get('[data-testid="menu-item-delete"]').click({
      force: true,
    });

    // Confirm deletion in modal
    cy.get('[data-testid="delete-service-account-modal"]').should("be.visible");
    cy.get('[data-testid="delete-service-account-confirm-button"]').click();

    // Wait for success message
    cy.waitTextVisible("Service account deleted");

    // Verify the service account is no longer in the list
    cy.ensureTextNotPresent(SERVICE_ACCOUNT_NAME);
  };

  // Test with inviteUsersEnabled = false (old UI pattern - button inside tab)
  it("create, generate token, and delete service account (old UI)", () => {
    setFeatureFlags(false);
    cy.loginWithCredentials();
    cy.skipIntroducePage();

    navigateToServiceAccountsTab();

    // Create service account
    createServiceAccount();

    // Generate access token for the service account
    createTokenForServiceAccount();

    // Delete the service account
    deleteServiceAccount();
  });

  // Test with inviteUsersEnabled = true (new UI pattern - button in header)
  it("create, generate token, and delete service account (new UI with invite users)", () => {
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
