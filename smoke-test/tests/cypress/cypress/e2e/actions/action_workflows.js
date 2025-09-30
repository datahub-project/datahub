import { aliasQuery, hasOperationName } from "../utils";

const datasetUrn =
  "urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)";
const datasetName = "DatasetToProposeOn";

describe("Action Workflows Test", () => {
  beforeEach(() => {
    cy.on("uncaught:exception", (err, runnable) => false);
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  const setWorkflowsEnabledFlags = () => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.reply((res) => {
          // Enable required feature flags for workflows
          res.body.data.appConfig.featureFlags.actionWorkflowsEnabled = true;
          res.body.data.appConfig.featureFlags.showTaskCenterRedesign = true;
          res.body.data.appConfig.featureFlags.themeV2Enabled = true;
          res.body.data.appConfig.featureFlags.themeV2Default = true;
        });
      }
    });
  };

  const selectEntityInModal = () => {
    // Ensure modal is visible first
    cy.get('[data-testid="workflow-form"]').should("be.visible");

    cy.get('[data-testid="entity-search-select-select"]')
      .scrollIntoView()
      .should("be.visible")
      .click();

    // Wait for dropdown and handle search
    cy.wait(2000);

    // Type in search input
    cy.get(
      '[data-testid="entity-search-select-input"], input[placeholder*="Search"]',
    )
      .first()
      .clear()
      .type(datasetName, { delay: 100 });

    cy.wait(3000);

    // Click first option
    cy.get(
      '[data-testid*="entity-search-option"], [role="option"], .ant-select-item',
    )
      .first()
      .click();
  };

  const handleWorkflowProposal = (action) => {
    // Navigate to task center proposals tab
    cy.visit("/requests/proposals");
    cy.wait(5000);

    // Look for workflow requests specifically
    cy.get('[data-testid="proposals-table"]').should("be.visible");

    cy.getWithTestId(action).first().click({ force: true });
    cy.contains("Submit").click({ force: true });
  };

  it("should create workflow request from home page", () => {
    setWorkflowsEnabledFlags();
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.visit("/");

    // Wait for workflows section
    cy.get('[data-testid="workflows-you-can-start"]', {
      timeout: 15000,
    }).should("be.visible");

    // Click workflow with specific text "Request Dataset Access"
    cy.get('[data-testid*="workflow-item"]').then(($items) => {
      if ($items.length > 0) {
        // Look for the workflow item containing "Request Dataset Access" text
        cy.contains('[data-testid*="workflow-item"]', "Request Dataset Access")
          .should("be.visible")
          .click();
      } else {
        throw new Error("No workflow items found");
      }
    });

    // Wait for modal
    cy.get('[data-testid="workflow-form"]', { timeout: 10000 }).should(
      "be.visible",
    );

    // Select entity
    selectEntityInModal();
    cy.wait(2000);

    // Fill form
    cy.get('form input[type="text"], form textarea')
      .first()
      .clear()
      .type("Test workflow request");
    cy.get('[data-testid="workflow-description"]')
      .clear()
      .type("Test from home page");

    // Submit
    cy.get('[data-testid="workflow-submit-button"]').click({ force: true });
    cy.wait(3000);

    // Reject the proposal
    handleWorkflowProposal("decline-button");
  });

  it("should create workflow request from entity profile", () => {
    setWorkflowsEnabledFlags();
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.goToDataset(datasetUrn, datasetName);
    cy.wait(3000);

    // Find workflows menu action
    cy.get('[data-testid="entity-menu-actions"]', { timeout: 10000 }).should(
      "be.visible",
    );

    cy.get("body").then(($body) => {
      if ($body.find('[data-testid="workflows-menu-action"]').length > 0) {
        cy.get('[data-testid="workflows-menu-action"]').click();
      } else {
        // Look for more options menu
        cy.get('[data-testid*="more"], .ant-dropdown-trigger').first().click();
        cy.get('[data-testid="workflows-menu-action"]').click();
      }
    });

    // Select workflow with specific text "Request Access"
    cy.contains('[data-testid*="workflow-dropdown-item"]', "Request Access")
      .should("be.visible")
      .click();
    cy.get('[data-testid="workflow-form"]', { timeout: 10000 }).should(
      "be.visible",
    );

    // Fill form (entity should be pre-selected)
    cy.get('form input[type="text"], form textarea')
      .first()
      .clear()
      .type("Entity profile test");
    cy.get('[data-testid="workflow-submit-button"]').click();
    cy.wait(3000);

    // Accept the proposal
    handleWorkflowProposal("approve-button");
  });

  it("should disable submit button if validation errors are present", () => {
    setWorkflowsEnabledFlags();
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.visit("/");

    // Start workflow
    cy.get('[data-testid="workflows-you-can-start"]').should("be.visible");

    // Click workflow with specific text "Request Dataset Access"
    cy.get('[data-testid*="workflow-item"]').then(($items) => {
      if ($items.length > 0) {
        // Look for the workflow item containing "Request Dataset Access" text
        cy.contains('[data-testid*="workflow-item"]', "Request Dataset Access")
          .should("be.visible")
          .click();
      } else {
        throw new Error("No workflow items found");
      }
    });

    cy.get('[data-testid="workflow-form"]', { timeout: 10000 }).should(
      "be.visible",
    );

    // Select entity
    selectEntityInModal();

    // Submit button should be disabled
    cy.get('[data-testid="workflow-submit-button"]').should("be.disabled");

    // Fix by filling required field
    cy.get('form input[type="text"], form textarea')
      .first()
      .type("Required field value");
    cy.get('[data-testid="workflow-submit-button"]')
      .should("not.be.disabled")
      .click();
    cy.wait(3000);
  });

  it("should cancel workflow creation", () => {
    setWorkflowsEnabledFlags();
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.visit("/");

    // Start workflow
    cy.get('[data-testid="workflows-you-can-start"]').should("be.visible");

    // Click workflow with specific text "Request Dataset Access"
    cy.get('[data-testid*="workflow-item"]').then(($items) => {
      if ($items.length > 0) {
        // Look for the workflow item containing "Request Dataset Access" text
        cy.contains('[data-testid*="workflow-item"]', "Request Dataset Access")
          .should("be.visible")
          .click();
      } else {
        throw new Error("No workflow items found");
      }
    });

    cy.get('[data-testid="workflow-form"]', { timeout: 10000 }).should(
      "be.visible",
    );

    // Fill some data
    selectEntityInModal();
    cy.get('form input[type="text"], form textarea')
      .first()
      .type("This will be cancelled");

    // Cancel
    cy.get('[data-testid="workflow-cancel-button"]').click();
    cy.get('[data-testid="workflow-form"]').should("not.exist");
  });
});
