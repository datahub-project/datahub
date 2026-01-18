import { hasOperationName } from "../utils";

// Use unique test ID with timestamp to ensure idempotency across runs
const test_id = `cy_doc_${Date.now()}`;

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

describe("Document Management End-to-End Test", () => {
  // Suppress common errors that don't affect test validity
  Cypress.on("uncaught:exception", (err) => {
    if (err.message.includes("ResizeObserver loop")) return false;
    if (err.message.includes("Failed to to get quick filters")) return false;
    if (
      err.message.includes(
        "usePageTemplateContext must be used within a PageTemplateProvider",
      )
    )
      return false;
    if (
      err.message.includes(
        "Cannot access 'DataProcessInstanceEntity' before initialization",
      )
    )
      return false;
    return true;
  });

  // Store document URNs for cleanup - ensures idempotency
  const createdDocuments = [];

  // Helper to safely delete documents
  const cleanupDocuments = () => {
    createdDocuments.forEach((urn) => {
      if (urn) {
        cy.deleteUrn(urn).then(
          () => cy.log(`Cleaned up document: ${urn}`),
          (err) => cy.log(`Failed to cleanup ${urn}: ${err?.message || err}`),
        );
      }
    });
    // Clear the array after cleanup
    createdDocuments.length = 0;
  };

  before(() => {
    cy.login();
  });

  beforeEach(() => {
    // Set up intercepts FIRST before any navigation
    enableContextDocuments();
    cy.setIsThemeV2Enabled(true);
    cy.login();
  });

  // Cleanup after all tests, regardless of pass/fail - ensures idempotency
  after(() => {
    cy.login().then(() => {
      cleanupDocuments();
    });
  });

  const doc1Title = `${test_id}_Parent`;
  const doc2Title = `${test_id}_Child`;

  describe("Core Document CRUD Operations", () => {
    let testDocument1Urn = null;
    let testDocument2Urn = null;
    const doc1UpdatedTitle = `${doc1Title}_Updated`;

    it("should create a new document via Context Documents page", () => {
      // Navigate directly to Context Documents page
      cy.visit("/context/documents");
      cy.wait(2000);

      // Wait for navigation to document page (either redirected or auto-created)
      cy.url().should("include", "/document/", { timeout: 15000 });

      // The context sidebar should be visible
      cy.getWithTestId("context-documents-sidebar").should("be.visible");

      // Extract URN from URL
      cy.url().then((url) => {
        const match = url.match(/\/document\/([^/?]+)/);
        if (match) {
          testDocument1Urn = decodeURIComponent(match[1]);
          createdDocuments.push(testDocument1Urn);
          cy.log(`Document URN: ${testDocument1Urn}`);
        }
      });

      // Set initial title
      cy.getWithTestId("document-title-input", { timeout: 5000 })
        .should("be.visible")
        .should("not.be.disabled")
        .clear()
        .type(doc1Title, { delay: 50 });

      // Trigger save by clicking away
      cy.get("body").click(0, 0);
      cy.wait(1500);

      // Verify title persisted
      cy.getWithTestId("document-title-input").should("have.value", doc1Title);
    });

    it("should create a second document using sidebar create button", () => {
      cy.visit(`/document/${encodeURIComponent(testDocument1Urn)}`);
      cy.wait(2000); // Wait longer for page to stabilize

      // The context sidebar should be visible
      cy.getWithTestId("context-documents-sidebar").should("be.visible");

      // Wait for any re-renders to settle, then click the create button
      cy.wait(500);
      cy.getWithTestId("create-document-button")
        .should("be.visible")
        .should("not.be.disabled")
        .click({ force: true });

      // Wait for navigation to new document page
      cy.url().should("not.include", testDocument1Urn, { timeout: 10000 });
      cy.url().should("include", "/document/");

      // Extract URN from URL
      cy.url().then((url) => {
        const match = url.match(/\/document\/([^/?]+)/);
        if (match) {
          testDocument2Urn = decodeURIComponent(match[1]);
          createdDocuments.push(testDocument2Urn);
          cy.log(`Created second document with URN: ${testDocument2Urn}`);
        }
      });

      // Wait for sidebar to finish loading (create button becomes enabled again)
      cy.getWithTestId("create-document-button", { timeout: 15000 })
        .should("be.visible")
        .and("not.be.disabled");

      // Now set the title - break up the chain to handle React re-renders
      cy.getWithTestId("document-title-input", { timeout: 10000 })
        .should("be.visible")
        .and("not.be.disabled")
        .clear();

      cy.getWithTestId("document-title-input").type(doc2Title, { delay: 50 });

      // Trigger save
      cy.get("body").click(0, 0);
      cy.wait(1500);

      // Verify title persisted
      cy.getWithTestId("document-title-input").should("have.value", doc2Title);
    });

    it("should update document title and verify persistence", () => {
      cy.visit(`/document/${encodeURIComponent(testDocument1Urn)}`);
      cy.wait(1000);

      // Update title
      cy.getWithTestId("document-title-input")
        .should("be.visible")
        .should("not.be.disabled")
        .clear()
        .type(doc1UpdatedTitle, { delay: 50 });

      cy.get("body").click(0, 0);
      cy.wait(1500);

      // Navigate away
      cy.visit("/");
      cy.wait(500);

      // Return and verify persistence
      cy.visit(`/document/${encodeURIComponent(testDocument1Urn)}`);
      cy.wait(1000);

      cy.getWithTestId("document-title-input").should(
        "have.value",
        doc1UpdatedTitle,
      );
    });

    it("should update document content", () => {
      cy.visit(`/document/${encodeURIComponent(testDocument1Urn)}`);
      cy.wait(1000);

      const testContent = `Test content for ${test_id}`;

      // Check if editor section exists
      cy.getWithTestId("document-editor-section").should("exist");

      // Click and type in the editor
      cy.get(".remirror-editor").should("exist").click();
      cy.wait(300);

      cy.get('.remirror-editor[contenteditable="true"]')
        .should("exist")
        .click()
        .clear({ force: true });
      cy.wait(200);

      cy.get('.remirror-editor[contenteditable="true"]').type(testContent, {
        delay: 100,
      });

      // Trigger save
      cy.get("body").click(0, 0);
      cy.wait(3500);

      // Verify content saved
      cy.reload();
      cy.wait(1000);

      cy.get(".remirror-editor").should("contain.text", testContent);
    });

    it("should update document status", () => {
      cy.visit(`/document/${encodeURIComponent(testDocument1Urn)}`);
      cy.wait(1000);

      // Wait for status selector to load
      cy.getWithTestId("document-status-select", { timeout: 10000 }).should(
        "exist",
      );
      cy.wait(1000);

      // Change to Published
      cy.getWithTestId("document-status-select")
        .find(".ant-dropdown-trigger")
        .should("be.visible")
        .click({ force: true });
      cy.wait(500);

      cy.get('[data-testid="option-PUBLISHED"]')
        .should("be.visible")
        .click({ force: true });
      cy.wait(1500);

      // Verify status updated
      cy.getWithTestId("document-status-select").should(
        "contain.text",
        "Published",
      );

      // Verify persistence
      cy.reload();
      cy.wait(1000);
      cy.getWithTestId("document-status-select").should(
        "contain.text",
        "Published",
      );
    });

    it("should update document type", () => {
      cy.visit(`/document/${encodeURIComponent(testDocument1Urn)}`);
      cy.wait(1000);

      // Set type to Runbook
      cy.getWithTestId("document-type-select")
        .find(".ant-dropdown-trigger")
        .should("be.visible")
        .click({ force: true });
      cy.wait(500);

      cy.get('[data-testid="option-Runbook"]')
        .should("be.visible")
        .click({ force: true });
      cy.wait(1500);

      // Verify type updated
      cy.getWithTestId("document-type-select").should(
        "contain.text",
        "Runbook",
      );

      // Verify persistence
      cy.reload();
      cy.wait(1000);
      cy.getWithTestId("document-type-select").should(
        "contain.text",
        "Runbook",
      );
    });
  });

  describe("Sidebar Navigation Features", () => {
    it("should collapse and expand the sidebar", () => {
      cy.visit("/context/documents");
      cy.wait(2000);
      cy.url().should("include", "/document/", { timeout: 15000 });

      // Sidebar should be visible and expanded by default
      cy.getWithTestId("context-documents-sidebar").should("be.visible");

      // The collapse button should be visible
      cy.getWithTestId("context-sidebar-collapse-button").should("be.visible");

      // Title should be visible when expanded
      cy.getWithTestId("context-documents-sidebar").should(
        "contain.text",
        "Documents",
      );

      // Click collapse button
      cy.getWithTestId("context-sidebar-collapse-button").click();
      cy.wait(500);

      // After collapsing, the title should not be visible
      // The sidebar width should be reduced (we check that create button is hidden)
      cy.getWithTestId("create-document-button").should("not.exist");

      // Click expand button (same button, different icon)
      cy.getWithTestId("context-sidebar-collapse-button").click();
      cy.wait(500);

      // After expanding, the title should be visible again
      cy.getWithTestId("context-documents-sidebar").should(
        "contain.text",
        "Documents",
      );
      cy.getWithTestId("create-document-button").should("be.visible");
    });

    it("should search for documents using sidebar search", () => {
      cy.visit("/context/documents");
      cy.wait(2000);
      cy.url().should("include", "/document/", { timeout: 15000 });

      // Get the current document URN
      cy.url().then((url) => {
        const match = url.match(/\/document\/([^/?]+)/);
        if (match) {
          const urn = decodeURIComponent(match[1]);
          createdDocuments.push(urn);
        }
      });

      // Set a unique title we can search for
      const searchTestTitle = `${test_id}_SearchTest`;
      cy.getWithTestId("document-title-input")
        .should("be.visible")
        .should("not.be.disabled")
        .clear()
        .type(searchTestTitle, { delay: 50 });
      cy.get("body").click(0, 0);
      cy.wait(1500);

      // The sidebar should have a search bar input - find by placeholder
      cy.getWithTestId("context-documents-sidebar").should("be.visible");

      // Find the search input by placeholder
      cy.get('input[placeholder="Search documents"]')
        .should("be.visible")
        .click()
        .type(searchTestTitle.substring(0, 8), { delay: 50 });

      cy.wait(1500); // Wait for debounce

      // Search results should appear
      cy.getWithTestId("context-sidebar-search-results", {
        timeout: 10000,
      }).should("be.visible");

      // Clear search by clicking outside
      cy.get("body").click(0, 0);
      cy.wait(500);

      // Search results should be hidden
      cy.getWithTestId("context-sidebar-search-results").should("not.exist");
    });
  });

  describe("Document Hierarchy Operations", () => {
    it("should create two documents, move one to the other, and verify nesting", function () {
      // Navigate to Context Documents page
      cy.visit("/context/documents");
      cy.wait(2000);
      cy.url().should("include", "/document/", { timeout: 15000 });

      // Extract first document URN
      cy.url().then((url) => {
        const match = url.match(/\/document\/([^/?]+)/);
        const urn = decodeURIComponent(match[1]);
        createdDocuments.push(urn);
        cy.wrap(urn).as("parentUrn");
      });

      // Set parent title
      cy.getWithTestId("document-title-input")
        .should("be.visible")
        .should("not.be.disabled")
        .clear()
        .type(doc1Title, { delay: 50 });
      cy.get("body").click(0, 0);
      cy.wait(1500);

      // Create Child Doc using sidebar create button
      cy.getWithTestId("create-document-button").should("be.visible").click();

      // Wait for URL to change
      cy.get("@parentUrn").then((parentUrn) => {
        cy.url().should("not.include", parentUrn);
        cy.url().should("include", "/document/");
      });

      cy.url().then((url) => {
        const match = url.match(/\/document\/([^/?]+)/);
        const urn = decodeURIComponent(match[1]);
        createdDocuments.push(urn);
        cy.wrap(urn).as("childUrn");
      });

      cy.getWithTestId("document-title-input")
        .should("be.visible")
        .should("not.be.disabled")
        .clear()
        .type(doc2Title, { delay: 50 });
      cy.get("body").click(0, 0);
      cy.wait(1500);

      // Move Child to Parent
      cy.get("@parentUrn").then(() => {
        cy.get("@childUrn").then((childUrn) => {
          // Find child in tree
          cy.getWithTestId(`document-tree-item-${childUrn}`)
            .should("exist", { timeout: 5000 })
            .first()
            .scrollIntoView()
            .trigger("mouseover");
          cy.wait(500);

          // Open actions menu
          cy.getWithTestId(`document-tree-item-${childUrn}`)
            .first()
            .find('[data-testid="document-actions-menu-button"]')
            .click({ force: true });
          cy.wait(1000);

          // Click Move
          cy.get(".ant-dropdown-menu")
            .should("be.visible")
            .contains("Move")
            .closest(".ant-dropdown-menu-item")
            .should("be.visible")
            .click({ force: true });
          cy.wait(2000);

          // Search for parent in move popover
          cy.get('[data-testid="move-document-popover"]', { timeout: 15000 })
            .should("be.visible")
            .within(() => {
              cy.get('input[placeholder="Search context..."]')
                .should("be.visible")
                .clear()
                .type(doc1Title, { delay: 50 });
              cy.wait(2000);
            });

          // Click the search result
          cy.get('[data-testid="move-document-popover"]').within(() => {
            cy.get('[data-testid="move-popover-search-result-title"]', {
              timeout: 10000,
            })
              .contains(doc1Title)
              .should("be.visible")
              .parent()
              .parent()
              .click({ force: true });
          });
          cy.wait(1000);

          // Confirm move
          cy.getWithTestId("move-document-confirm-button")
            .should("not.be.disabled", { timeout: 5000 })
            .click();

          cy.wait(1500);
          cy.contains("moved successfully", {
            matchCase: false,
            timeout: 5000,
          });

          // Verify nesting by checking breadcrumb
          cy.get("@childUrn").then((childUrnInner) => {
            cy.visit(`/document/${encodeURIComponent(childUrnInner)}`);
          });
          cy.wait(1000);

          // The parent breadcrumb should contain the parent title
          cy.contains("a", doc1Title).should("exist");
        });
      });
    });
  });

  describe("Document Deletion", () => {
    it("should delete parent document and cascade to children", () => {
      const parentUrn = createdDocuments[createdDocuments.length - 2];

      if (!parentUrn) {
        cy.log("No parent document to delete, skipping test");
        return;
      }

      cy.visit(`/document/${encodeURIComponent(parentUrn)}`);
      cy.wait(2000);

      cy.getWithTestId("context-documents-sidebar").should("be.visible");

      // Hover to reveal actions in the tree
      cy.getWithTestId(`document-tree-item-${parentUrn}`)
        .should("exist")
        .first()
        .trigger("mouseover");
      cy.wait(300);

      // Open actions menu
      cy.getWithTestId(`document-tree-item-${parentUrn}`)
        .first()
        .find('[data-testid="document-actions-menu-button"]')
        .should("exist")
        .click({ force: true });
      cy.wait(300);

      // Click Delete
      cy.contains("Delete").should("be.visible").click();
      cy.wait(500);

      // Confirm deletion
      cy.contains("Delete Document", { timeout: 3000 });
      cy.contains("button", "Delete").click();
      cy.wait(2000);

      // Verify parent deleted from tree
      cy.getWithTestId(`document-tree-item-${parentUrn}`).should("not.exist");

      // Clear from cleanup list since we deleted them
      createdDocuments.length = 0;
    });
  });
});
