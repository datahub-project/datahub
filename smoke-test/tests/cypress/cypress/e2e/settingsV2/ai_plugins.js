/**
 * Cypress tests for AI Plugins functionality.
 *
 * NOTE: These tests require the AI_PLUGINS_ENABLED feature flag to be true.
 * The feature is disabled by default, so these tests are skipped in CI.
 * To run locally, set AI_PLUGINS_ENABLED=true in your environment.
 *
 * TODO: Enable these tests once the AI Plugins feature is launched.
 */

// Skip all tests - feature flag is disabled by default
describe.skip("AI Plugins Admin Management", () => {
  const testId = Math.floor(Math.random() * 100000);
  const pluginName = `Test MCP Plugin ${testId}`;
  const pluginNameEdited = `Test MCP Plugin ${testId} EDITED`;
  const pluginUrl = "https://api.example.com/mcp";
  const pluginUrlEdited = "https://api.example-edited.com/mcp";

  /**
   * Navigate to the AI Plugins admin tab.
   */
  function navigateToAiPluginsTab() {
    cy.visit("/settings/ai");
    cy.waitTextVisible("AI");
    cy.contains("Plugins").should("be.visible").click();
    cy.get('[data-testid="ai-plugins-tab"]', { timeout: 10000 }).should(
      "exist",
    );
  }

  /**
   * Open the create plugin modal.
   */
  function openCreatePluginModal() {
    cy.contains("button", "Create").click();
    cy.get('[data-testid="create-plugin-modal"]', { timeout: 10000 }).should(
      "exist",
    );
  }

  /**
   * Fill in the plugin form with basic info.
   */
  function fillPluginForm(name, url) {
    cy.get('[data-testid="plugin-name-input"]').clear().type(name);
    cy.get('[data-testid="plugin-url-input"]').clear().type(url);
  }

  /**
   * Submit the plugin form.
   */
  function submitPluginForm() {
    cy.get('[data-testid="plugin-submit-button"]').click();
  }

  /**
   * Cancel the plugin form.
   */
  function cancelPluginForm() {
    cy.get('[data-testid="plugin-cancel-button"]').click();
  }

  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();
  });

  it("should navigate to AI Plugins tab", () => {
    navigateToAiPluginsTab();
    cy.get('[data-testid="ai-plugins-tab"]').should("be.visible");
  });

  it("should open and close create plugin modal", () => {
    navigateToAiPluginsTab();
    openCreatePluginModal();

    // Verify modal is open
    cy.get('[data-testid="create-plugin-modal"]').should("be.visible");
    cy.contains("Create AI Plugin").should("be.visible");

    // Cancel the modal
    cancelPluginForm();
    cy.get('[data-testid="create-plugin-modal"]').should("not.exist");
  });

  it("should create a new AI plugin", () => {
    navigateToAiPluginsTab();
    openCreatePluginModal();

    // Fill in plugin details
    fillPluginForm(pluginName, pluginUrl);

    // Submit the form
    submitPluginForm();

    // Verify success message
    cy.waitTextVisible("AI plugin created successfully");

    // Verify plugin appears in the table
    cy.get('[data-testid="ai-plugins-table"]').should("be.visible");
    cy.contains(pluginName).should("be.visible");
  });

  it("should edit an existing AI plugin", () => {
    navigateToAiPluginsTab();

    // Find the plugin we created and click edit
    cy.contains("tr", pluginName, { timeout: 10000 }).should("be.visible");

    // Open actions menu and click Edit
    cy.contains("tr", pluginName).within(() => {
      cy.get("button").last().click();
    });
    cy.contains("Edit").click();

    // Verify edit modal is open
    cy.get('[data-testid="create-plugin-modal"]').should("be.visible");
    cy.contains("Edit AI Plugin").should("be.visible");

    // Update the plugin name and URL
    fillPluginForm(pluginNameEdited, pluginUrlEdited);

    // Submit the form
    submitPluginForm();

    // Verify success message
    cy.waitTextVisible("AI plugin updated successfully");

    // Verify updated plugin appears in the table
    cy.contains(pluginNameEdited).should("be.visible");
  });

  it("should delete an AI plugin", () => {
    navigateToAiPluginsTab();

    // Find the plugin we edited and click delete
    cy.contains("tr", pluginNameEdited, { timeout: 10000 }).should(
      "be.visible",
    );

    // Open actions menu and click Delete
    cy.contains("tr", pluginNameEdited).within(() => {
      cy.get("button").last().click();
    });
    cy.contains("Delete").click();

    // Verify success message
    cy.waitTextVisible("Plugin deleted successfully");

    // Verify plugin is removed from the table
    cy.contains(pluginNameEdited).should("not.exist");
  });
});

// Skip all tests - feature flag is disabled by default
describe.skip("My AI Settings - User Page", () => {
  /**
   * Navigate to My AI Settings user page.
   */
  function navigateToMyAiSettings() {
    cy.visit("/settings/my-ai-settings");
    cy.get('[data-testid="my-ai-settings-page"]', { timeout: 10000 }).should(
      "exist",
    );
  }

  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();
  });

  it("should navigate to My AI Settings page", () => {
    navigateToMyAiSettings();
    cy.contains("My AI Settings").should("be.visible");
    cy.contains("Manage your AI plugin preferences").should("be.visible");
  });

  it("should display plugins list or empty state", () => {
    navigateToMyAiSettings();

    // Check if plugins list is displayed (either with plugins or empty state)
    cy.get('[data-testid="my-ai-settings-page"]').should("be.visible");

    // Either plugins-list exists or empty state message is shown
    cy.get("body").then(($body) => {
      if ($body.find('[data-testid="plugins-list"]').length > 0) {
        cy.get('[data-testid="plugins-list"]').should("be.visible");
      } else {
        cy.contains("No AI plugins are available").should("be.visible");
      }
    });
  });
});
