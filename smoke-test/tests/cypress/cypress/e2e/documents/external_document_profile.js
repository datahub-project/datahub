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
    cy.setIsThemeV2Enabled(true);
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
});
