import { hasOperationName } from "../utils";
import {
  updateDescription,
  ensureDescriptionContainsText,
  ensureAboutSectionIsVisible,
} from "../summaryTab/utils";

// Pre-loaded external document URN from acryl-main-data.json
const EXTERNAL_DOC_URN = "urn:li:document:cypress-external-doc-test";
const EXTERNAL_DOC_TITLE = "Cypress External Document";

// Helper to enable context documents and nav bar redesign feature flags
function enableContextDocuments() {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.alias = "gqlappConfigQuery";
      req.on("response", (res) => {
        res.body.data.appConfig.featureFlags.contextDocumentsEnabled = true;
        res.body.data.appConfig.featureFlags.showNavBarRedesign = true;
        res.body.data.appConfig.featureFlags.showHomePageRedesign = true;
      });
    }
  });
}

describe("External Document Profile Tests", () => {
  // Suppress common errors
  Cypress.on("uncaught:exception", (err) => {
    if (err.message.includes("ResizeObserver loop")) {
      return false;
    }
    if (err.message.includes("Failed to to get quick filters")) {
      return false;
    }
    if (
      err.message.includes(
        "usePageTemplateContext must be used within a PageTemplateProvider",
      )
    ) {
      return false;
    }
    if (
      err.message.includes(
        "Cannot access 'DataProcessInstanceEntity' before initialization",
      )
    ) {
      return false;
    }
    return true;
  });

  beforeEach(() => {
    enableContextDocuments();
    cy.login();
  });

  it("should load external document and edit description", () => {
    // Navigate to the pre-loaded external document profile
    cy.visit(`/document/${encodeURIComponent(EXTERNAL_DOC_URN)}`);
    cy.wait(2000);

    // Verify the document title is shown
    cy.contains(EXTERNAL_DOC_TITLE, { timeout: 10000 }).should("exist");

    // Verify About section is visible
    ensureAboutSectionIsVisible();

    // Test updating the description
    const testDescription = `Updated description ${Date.now()}`;
    updateDescription(testDescription);

    // Verify the description was updated
    ensureDescriptionContainsText(testDescription);
  });

  it("should show last synced property, inline content banner, and read-only content", () => {
    cy.visit(`/document/${encodeURIComponent(EXTERNAL_DOC_URN)}`);
    cy.wait(2000);

    cy.contains(EXTERNAL_DOC_TITLE, { timeout: 10000 }).should("exist");

    // "Last Synced" property validates the lastObserved fallback fix in DocumentMapper
    cy.contains("Last Synced", { timeout: 10000 }).should("exist");

    // Inline content section renders
    cy.get('[data-testid="external-document-inline-content-section"]', {
      timeout: 10000,
    }).should("exist");

    // Info banner explains the content is extracted text only
    cy.get('[data-testid="external-document-inline-banner"]').should("exist");
    cy.contains("Text extracted from Notion").should("exist");

    // Markdown body renders the document text
    cy.contains(
      "This is an external document created for Cypress testing.",
    ).should("exist");
  });
});
