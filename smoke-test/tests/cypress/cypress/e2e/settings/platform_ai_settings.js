import { aliasQuery, hasOperationName } from "../utils";

const DOC_AI_INSTRUCTIONS = `Document all datasets with specific emphasis on:
- Business context and purpose
- Data lineage and sources
- Quality metrics and freshness

Always mention our retail domain expertise.`;

const AI_ASSISTANT_INSTRUCTIONS = `You are a DataHub assistant helping with retail data management.

Key guidelines:
- Focus on retail industry examples
- Mention customer segmentation when relevant
- Use our internal terminology: 'customer segments' instead of 'user groups'`;

describe("Platform AI Settings", () => {
  beforeEach(() => {
    cy.on("uncaught:exception", (err, runnable) => false);
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  const setAiFeaturesEnabled = (isEnabled) => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.reply((res) => {
          // Modify the response body directly
          res.body.data.appConfig.featureFlags.aiFeaturesEnabled = isEnabled;
          res.body.data.appConfig.featureFlags.themeV2Enabled = true;
          res.body.data.appConfig.featureFlags.themeV2Default = true;
        });
      }
    });
  };

  it("should display AI settings sections when AI features are enabled", () => {
    setAiFeaturesEnabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.visit("/settings/ai");
    cy.waitTextVisible("Configure AI-powered features");

    // Verify main sections are visible
    cy.get('[data-testid="ai-assistant-section-title"]')
      .should("be.visible")
      .contains("Ask DataHub");
    cy.get('[data-testid="ai-documentation-section-title"]')
      .should("be.visible")
      .contains("AI Documentation Generation");

    // Verify AI Documentation toggle exists
    cy.get('[data-testid="ai-docs-toggle"]').should("exist");

    // Verify Ask DataHub instruction section exists (always visible)
    cy.get('[data-testid="ai-assistant-instructions-section"]').should(
      "be.visible",
    );
    cy.get('[data-testid="ai-assistant-instructions-textarea"]').should(
      "be.visible",
    );

    // Enable AI Documentation to see its instructions
    cy.get('[data-testid="ai-docs-toggle"]').then(($toggle) => {
      const isChecked = $toggle.attr("aria-checked") === "true";
      if (!isChecked) {
        cy.get('[data-testid="ai-docs-toggle"]').click();
        cy.waitTextVisible("AI documentation generation enabled");
      }
    });

    // Verify docs instruction sections exist (visible when toggle is ON)
    cy.get('[data-testid="docs-ai-instructions-section"]').should("be.visible");
    cy.get('[data-testid="docs-ai-instructions-textarea"]').should(
      "be.visible",
    );

    // Character counters only appear when content is >= 8000 characters
    // So we don't check for them when empty
  });

  it("should toggle AI documentation and save instructions successfully", () => {
    setAiFeaturesEnabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.visit("/settings/ai");
    cy.waitTextVisible("Configure AI-powered features");

    // Check current toggle state and toggle if needed
    cy.get('[data-testid="ai-docs-toggle"]').then(($toggle) => {
      const isChecked = $toggle.attr("aria-checked") === "true";
      if (!isChecked) {
        cy.get('[data-testid="ai-docs-toggle"]').click();
        cy.waitTextVisible("AI documentation generation enabled");
      }
    });

    // Add documentation AI instructions
    cy.get('[data-testid="docs-ai-instructions-textarea"]').clear();
    cy.get('[data-testid="docs-ai-instructions-textarea"]').type(
      DOC_AI_INSTRUCTIONS,
    );

    // Trigger blur to save
    cy.get('[data-testid="docs-ai-instructions-textarea"]').blur();
    cy.waitTextVisible("Saved instructions!");

    // Character count only shows when >= 8000 characters
    // DOC_AI_INSTRUCTIONS is short, so counter won't be visible

    // Reload page and verify instructions persisted
    cy.reload();
    cy.waitTextVisible("Configure AI-powered features");

    // Toggle state should be persisted, but verify instructions textarea is visible
    cy.get('[data-testid="docs-ai-instructions-textarea"]').should(
      "have.value",
      DOC_AI_INSTRUCTIONS,
    );
  });

  it("should save AI assistant instructions successfully", () => {
    setAiFeaturesEnabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.visit("/settings/ai");
    cy.waitTextVisible("Configure AI-powered features");

    // Add AI assistant instructions
    cy.get('[data-testid="ai-assistant-instructions-textarea"]').clear();
    cy.get('[data-testid="ai-assistant-instructions-textarea"]').type(
      AI_ASSISTANT_INSTRUCTIONS,
    );

    // Trigger blur to save
    cy.get('[data-testid="ai-assistant-instructions-textarea"]').blur();
    cy.waitTextVisible("Saved instructions!");

    // Character count only shows when >= 8000 characters
    // AI_ASSISTANT_INSTRUCTIONS is short, so counter won't be visible

    // Reload page and verify instructions persisted
    cy.reload();
    cy.waitTextVisible("Configure AI-powered features");
    cy.get('[data-testid="ai-assistant-instructions-textarea"]').should(
      "have.value",
      AI_ASSISTANT_INSTRUCTIONS,
    );
  });

  it("should preserve newlines and formatting in instructions", () => {
    setAiFeaturesEnabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.visit("/settings/ai");
    cy.waitTextVisible("Configure AI-powered features");

    // Enable AI Documentation to see instructions
    cy.get('[data-testid="ai-docs-toggle"]').then(($toggle) => {
      const isChecked = $toggle.attr("aria-checked") === "true";
      if (!isChecked) {
        cy.get('[data-testid="ai-docs-toggle"]').click();
        cy.waitTextVisible("AI documentation generation enabled");
      }
    });

    const multiLineInstructions = `Line 1

Line 3 with gap above

- Bullet point
- Another bullet`;

    // Add multi-line instructions to docs AI
    cy.get('[data-testid="docs-ai-instructions-textarea"]').clear();
    cy.get('[data-testid="docs-ai-instructions-textarea"]').type(
      multiLineInstructions,
    );

    // Save and verify
    cy.get('[data-testid="docs-ai-instructions-textarea"]').blur();
    cy.waitTextVisible("Saved instructions!");

    // Reload and verify formatting preserved
    cy.reload();
    cy.waitTextVisible("Configure AI-powered features");

    // Re-enable toggle after reload to see instructions
    cy.get('[data-testid="ai-docs-toggle"]').then(($toggle) => {
      const isChecked = $toggle.attr("aria-checked") === "true";
      if (!isChecked) {
        cy.get('[data-testid="ai-docs-toggle"]').click();
        cy.waitTextVisible("AI documentation generation enabled");
      }
    });

    cy.get('[data-testid="docs-ai-instructions-textarea"]').should(
      "have.value",
      multiLineInstructions,
    );
  });

  it("should handle empty instructions correctly", () => {
    setAiFeaturesEnabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.visit("/settings/ai");
    cy.waitTextVisible("Configure AI-powered features");

    // Enable AI Documentation to see instructions
    cy.get('[data-testid="ai-docs-toggle"]').then(($toggle) => {
      const isChecked = $toggle.attr("aria-checked") === "true";
      if (!isChecked) {
        cy.get('[data-testid="ai-docs-toggle"]').click();
        cy.waitTextVisible("AI documentation generation enabled");
      }
    });

    // First add some instructions
    cy.get('[data-testid="docs-ai-instructions-textarea"]').clear();
    cy.get('[data-testid="docs-ai-instructions-textarea"]').type(
      "Some initial instructions",
    );

    cy.get('[data-testid="docs-ai-instructions-textarea"]').blur();
    cy.waitTextVisible("Saved instructions!");

    // Then clear them
    cy.get('[data-testid="docs-ai-instructions-textarea"]').clear();
    cy.get('[data-testid="docs-ai-instructions-textarea"]').blur();
    cy.waitTextVisible("Saved instructions!");

    // Character count is hidden when empty (only shows at >= 8000 characters)
    cy.get('[data-testid="docs-ai-character-count"]').should("not.exist");

    // Reload and verify empty state persisted
    cy.reload();
    cy.waitTextVisible("Configure AI-powered features");

    // Re-enable toggle after reload to see instructions
    cy.get('[data-testid="ai-docs-toggle"]').then(($toggle) => {
      const isChecked = $toggle.attr("aria-checked") === "true";
      if (!isChecked) {
        cy.get('[data-testid="ai-docs-toggle"]').click();
        cy.waitTextVisible("AI documentation generation enabled");
      }
    });

    cy.get('[data-testid="docs-ai-instructions-textarea"]').should(
      "have.value",
      "",
    );
  });

  it("should toggle AI documentation off successfully", () => {
    setAiFeaturesEnabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.visit("/settings/ai");
    cy.waitTextVisible("Configure AI-powered features");

    // Check current toggle state and ensure it's enabled first
    cy.get('[data-testid="ai-docs-toggle"]').then(($toggle) => {
      const isChecked = $toggle.attr("aria-checked") === "true";
      if (!isChecked) {
        cy.get('[data-testid="ai-docs-toggle"]').click();
        cy.waitTextVisible("AI documentation generation enabled");
      }
    });

    // Then disable it
    cy.get('[data-testid="ai-docs-toggle"]').click();
    cy.waitTextVisible("AI documentation generation disabled");

    // Verify toggle state after reload
    cy.reload();
    cy.waitTextVisible("Configure AI-powered features");
    cy.get('[data-testid="ai-docs-toggle"]').should(
      "have.attr",
      "aria-checked",
      "false",
    );
  });
});
