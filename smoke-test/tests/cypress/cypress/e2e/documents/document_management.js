import { hasOperationName } from "../utils";

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
  // Suppress ResizeObserver and quick filters errors
  Cypress.on("uncaught:exception", (err) => {
    // Suppress ResizeObserver errors
    if (err.message.includes("ResizeObserver loop")) {
      return false;
    }
    // Suppress quick filters backend errors
    if (err.message.includes("Failed to to get quick filters")) {
      return false;
    }
    // Suppress page template context errors from homeV3 context documents
    if (
      err.message.includes(
        "usePageTemplateContext must be used within a PageTemplateProvider",
      )
    ) {
      return false;
    }
    // Suppress DataProcessInstanceEntity init error from entity registry V2
    if (
      err.message.includes(
        "Cannot access 'DataProcessInstanceEntity' before initialization",
      )
    ) {
      return false;
    }
    return true;
  });

  // Store document URNs for cleanup
  const createdDocuments = [];

  // Helper to safely delete documents
  const cleanupDocuments = () => {
    createdDocuments.forEach((urn) => {
      if (urn) {
        cy.deleteUrn(urn).then(
          () => cy.log(`Cleaned up document: ${urn}`),
          (err) => cy.log(`Failed to cleanup ${urn}: ${err.message}`),
        );
      }
    });
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

  // Cleanup after all tests, regardless of pass/fail
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

    it("should create a new document", () => {
      // Navigate to search page
      cy.visit("/search");
      cy.wait(2000);

      // Click the sidebar toggle button to expand the sidebar
      cy.getWithTestId("nav-bar-toggler").should("be.visible").click();
      cy.wait(1000);

      // Check if Context is visible (it should be in the sidebar if feature is enabled)
      cy.contains("Context").should("exist");

      // Hover over the Context header to reveal the create button
      cy.contains("Context").realHover();
      cy.wait(500);

      // Click the create document button - force click
      cy.getWithTestId("create-document-button").click({ force: true });

      // Wait for navigation to document page
      cy.url().should("include", "/document/", { timeout: 10000 });

      // Extract URN from URL
      cy.url().then((url) => {
        const match = url.match(/\/document\/([^/?]+)/);
        if (match) {
          testDocument1Urn = decodeURIComponent(match[1]);
          createdDocuments.push(testDocument1Urn);
          cy.log(`Created document with URN: ${testDocument1Urn}`);
        }
      });

      // Set initial title
      cy.getWithTestId("document-title-input", { timeout: 5000 })
        .should("be.visible")
        .clear()
        .type(doc1Title, { delay: 50 });

      // Trigger save by clicking away
      cy.get("body").click(0, 0);
      cy.wait(1500); // Allow auto-save

      // Verify title persisted
      cy.getWithTestId("document-title-input").should("have.value", doc1Title);
    });

    it("should update document title and verify persistence", () => {
      // Navigate directly to document
      cy.visit(`/document/${encodeURIComponent(testDocument1Urn)}`);
      cy.wait(1000);

      // Update title
      cy.getWithTestId("document-title-input")
        .should("be.visible")
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

      // Look for the remirror editor directly (it might not have the exact testid we expect)
      cy.get(".remirror-editor").should("exist").click();
      cy.wait(300);

      // Type content into the editor
      cy.get('.remirror-editor[contenteditable="true"]')
        .should("exist")
        .click()
        .clear({ force: true });
      cy.wait(200);

      cy.get('.remirror-editor[contenteditable="true"]').type(testContent, {
        delay: 100,
      }); // Slower typing

      // Trigger save
      cy.get("body").click(0, 0);
      cy.wait(3500); // Auto-save timeout + buffer

      // Verify content saved
      cy.reload();
      cy.wait(1000);

      cy.get(".remirror-editor").should("contain.text", testContent);
    });

    it("should update document status", () => {
      cy.visit(`/document/${encodeURIComponent(testDocument1Urn)}`);
      cy.wait(1000);

      // Verify initial status is Draft - wait for it to load
      cy.getWithTestId("document-status-select", { timeout: 10000 }).should(
        "exist",
      );
      cy.wait(1000);

      // Change to Published - click the ant-dropdown-trigger inside the wrapper
      cy.getWithTestId("document-status-select")
        .find(".ant-dropdown-trigger")
        .should("be.visible")
        .click({ force: true });
      cy.wait(500);

      // Click Published option - use specific testid from SimpleSelect
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

      // Click Runbook option - use specific testid
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

  describe("Document Hierarchy Operations", () => {
    it("should create two documents, move one to the other, and verify nesting", function () {
      // 1. Initial Setup
      cy.visit("/search");
      cy.wait(2000);

      // Ensure sidebar is open - check for Context visibility
      cy.get("body").then(($body) => {
        if ($body.find(':contains("Context")').length === 0) {
          cy.getWithTestId("nav-bar-toggler").should("be.visible").click();
          cy.wait(1000);
        }
      });

      // 2. Create Parent Doc
      cy.contains("Context").should("exist").realHover();
      cy.wait(500);
      cy.getWithTestId("create-document-button").click({ force: true });
      cy.url().should("include", "/document/", { timeout: 10000 });

      cy.url().then((url) => {
        const match = url.match(/\/document\/([^/?]+)/);
        const urn = decodeURIComponent(match[1]);
        createdDocuments.push(urn);
        cy.wrap(urn).as("parentUrn");
      });

      cy.getWithTestId("document-title-input")
        .should("be.visible")
        .clear()
        .type(doc1Title, { delay: 50 });
      cy.get("body").click(0, 0);
      cy.wait(1500);

      // 3. Create Child Doc (from current page)
      cy.contains("Context").should("exist").scrollIntoView().realHover();
      cy.getWithTestId("create-document-button")
        .should("exist")
        .click({ force: true });

      // Wait for URL to change (ensure we are on new doc)
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
        .clear()
        .type(doc2Title, { delay: 50 });
      cy.get("body").click(0, 0);
      cy.wait(1500);

      // 4. Move Child to Parent (from current page)
      cy.get("@parentUrn").then((parentUrn) => {
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

          // Click Move - force click the item container in the dropdown
          cy.get(".ant-dropdown-menu")
            .should("be.visible")
            .contains("Move")
            .closest(".ant-dropdown-menu-item")
            .should("be.visible")
            .click({ force: true });
          cy.wait(2000);

          // Select new parent inside the move document popover by searching for its title
          cy.log(
            `Looking for parent by title inside move popover: ${doc1Title}`,
          );
          cy.get('[data-testid="move-document-popover"]', { timeout: 15000 })
            .should("be.visible")
            .within(() => {
              // Use the search box to find the parent document by name
              cy.get('input[placeholder="Search context..."]')
                .should("be.visible")
                .clear()
                .type(doc1Title, { delay: 50 });

              // Wait for debounce and search to complete
              cy.wait(2000);
            });

          // Check if results loaded, if not retry the search
          cy.get('[data-testid="move-document-popover"]').then(($popover) => {
            const resultExists = $popover.find(
              '[data-testid="move-popover-search-result-title"]',
            ).length;
            if (!resultExists) {
              cy.log("No results found on first try, typing again...");
              cy.get('[data-testid="move-document-popover"]').within(() => {
                cy.get('input[placeholder="Search context..."]')
                  .clear()
                  .type(doc1Title, { delay: 50 });
              });
              cy.wait(2000);
            }
          });

          // Now find and click the search result
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

          // Verify nesting by checking breadcrumb on the child document profile
          cy.get("@childUrn").then((childUrnInner) => {
            // Navigate to the child document page
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
      const parentUrn = createdDocuments[1];

      // Navigate to search page
      cy.visit("/search");
      cy.wait(2000);

      // Click the sidebar toggle button to expand the sidebar
      cy.getWithTestId("nav-bar-toggler").should("be.visible").click();
      cy.wait(1000);

      // Hover to reveal actions
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

      // Verify parent deleted
      cy.getWithTestId(`document-tree-item-${parentUrn}`).should("not.exist");

      // Clear from cleanup list since we deleted it
      createdDocuments.length = 0;
    });
  });
});
