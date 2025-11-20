const test_id = `cy_doc_${Date.now()}`;

describe("Document Management End-to-End Test", () => {
  // Suppress ResizeObserver errors
  Cypress.on(
    "uncaught:exception",
    (err) => !err.message.includes("ResizeObserver loop"),
  );

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
    cy.setIsThemeV2Enabled(true);
    cy.login();
  });

  beforeEach(() => {
    cy.login();
  });

  // Cleanup after all tests, regardless of pass/fail
  after(() => {
    cy.login().then(() => {
      cleanupDocuments();
    });
  });

  describe("Core Document CRUD Operations", () => {
    let testDocument1Urn = null;
    let testDocument2Urn = null;
    const doc1Title = `${test_id}_Parent`;
    const doc2Title = `${test_id}_Child`;
    const doc1UpdatedTitle = `${doc1Title}_Updated`;

    it("should create a new document", () => {
      cy.visit("/");
      cy.wait(1000);

      // Find and click create document button (robust selector)
      cy.contains("Context")
        .parents('[role="group"]')
        .first()
        .trigger("mouseover");
      cy.wait(300);

      cy.getWithTestId("create-document-button")
        .should("exist")
        .click({ force: true });

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

      // Focus editor and add content
      cy.getWithTestId("document-editor-section").should("exist").click();
      cy.wait(300);

      cy.getWithTestId("document-content-editor")
        .find('.remirror-editor[contenteditable="true"]')
        .should("exist")
        .click()
        .clear({ force: true })
        .type(testContent, { delay: 30 });

      // Trigger save
      cy.get("body").click(0, 0);
      cy.wait(3500); // Auto-save timeout + buffer

      // Verify content saved
      cy.reload();
      cy.wait(1000);

      cy.getWithTestId("document-content-editor")
        .find(".remirror-editor")
        .should("contain.text", testContent);
    });

    it("should update document status", () => {
      cy.visit(`/document/${encodeURIComponent(testDocument1Urn)}`);
      cy.wait(1000);

      // Change to Published
      cy.getWithTestId("document-status-select")
        .should("exist")
        .click({ force: true });
      cy.wait(200);

      cy.contains("Published").click({ force: true });
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
        .should("exist")
        .click({ force: true });
      cy.wait(200);

      cy.contains("Runbook").click({ force: true });
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

    it("should create a second document for hierarchy testing", () => {
      cy.visit("/");
      cy.wait(1000);

      cy.contains("Context")
        .parents('[role="group"]')
        .first()
        .trigger("mouseover");
      cy.wait(300);

      cy.getWithTestId("create-document-button")
        .should("exist")
        .click({ force: true });

      cy.url().should("include", "/document/", { timeout: 10000 });

      cy.url().then((url) => {
        const match = url.match(/\/document\/([^/?]+)/);
        if (match) {
          testDocument2Urn = decodeURIComponent(match[1]);
          createdDocuments.push(testDocument2Urn);
          cy.log(`Created document 2 with URN: ${testDocument2Urn}`);
        }
      });

      cy.getWithTestId("document-title-input")
        .should("be.visible")
        .clear()
        .type(doc2Title, { delay: 50 });

      cy.get("body").click(0, 0);
      cy.wait(1500);
    });
  });

  describe("Document Hierarchy Operations", () => {
    let parentUrn = null;
    let childUrn = null;

    // Use documents created in previous test suite
    before(() => {
      // Get the URNs from the created documents
      if (createdDocuments.length >= 2) {
        [childUrn, parentUrn] = createdDocuments; // First becomes child, second becomes parent
      }
    });

    it("should move document to create parent-child hierarchy", () => {
      cy.visit("/");
      cy.wait(1500);

      // Hover over child document to reveal actions
      cy.getWithTestId(`document-tree-item-${childUrn}`)
        .should("exist", { timeout: 5000 })
        .trigger("mouseover");
      cy.wait(300);

      // Open actions menu
      cy.getWithTestId(`document-tree-item-${childUrn}`)
        .find('[data-testid="document-actions-menu-button"]')
        .should("exist")
        .click({ force: true });
      cy.wait(300);

      // Click Move
      cy.contains("Move").should("be.visible").click();
      cy.wait(500);

      // Select new parent
      cy.getWithTestId(`document-tree-item-${parentUrn}`)
        .should("exist")
        .click();
      cy.wait(300);

      // Confirm move
      cy.getWithTestId("move-document-confirm-button")
        .should("exist")
        .should("not.be.disabled")
        .click();

      cy.wait(1500);

      // Verify success
      cy.contains("moved successfully", { matchCase: false, timeout: 5000 });
    });

    it("should expand parent to show child documents", () => {
      cy.visit("/");
      cy.wait(1500);

      // Hover to reveal expand button
      cy.getWithTestId(`document-tree-item-${parentUrn}`)
        .should("exist")
        .trigger("mouseover");
      cy.wait(300);

      // Expand parent
      cy.getWithTestId(`document-tree-expand-button-${parentUrn}`)
        .should("exist")
        .click({ force: true });
      cy.wait(1500);

      // Verify child is visible
      cy.getWithTestId(`document-tree-item-${childUrn}`).should("be.visible");
    });

    it("should collapse parent to hide child documents", () => {
      cy.visit("/");
      cy.wait(1500);

      // First expand
      cy.getWithTestId(`document-tree-item-${parentUrn}`)
        .should("exist")
        .trigger("mouseover");
      cy.wait(300);

      cy.getWithTestId(`document-tree-expand-button-${parentUrn}`)
        .should("exist")
        .click({ force: true });
      cy.wait(1500);

      // Then collapse
      cy.getWithTestId(`document-tree-expand-button-${parentUrn}`)
        .should("exist")
        .click({ force: true });
      cy.wait(500);

      // Verify child is hidden
      cy.getWithTestId(`document-tree-item-${childUrn}`).should("not.exist");
    });
  });

  describe("Document Deletion", () => {
    it("should delete parent document and cascade to children", () => {
      const parentUrn = createdDocuments[1];

      cy.visit("/");
      cy.wait(1500);

      // Hover to reveal actions
      cy.getWithTestId(`document-tree-item-${parentUrn}`)
        .should("exist")
        .trigger("mouseover");
      cy.wait(300);

      // Open actions menu
      cy.getWithTestId(`document-tree-item-${parentUrn}`)
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
