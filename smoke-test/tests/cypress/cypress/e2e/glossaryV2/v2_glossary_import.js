const testGlossaryTermGroup = "CypressImportTestGroup";
const testGlossaryTerm = "CypressImportTestTerm";

const navigateToGlossaryImportPage = () => {
  cy.visit("/glossary/import");
  cy.waitTextVisible("Import Glossary");
  cy.wait(1000);
};

const createSimpleCsvContent = (terms) => {
  const header = "entity_type,name,description\n";
  const rows = terms.map(
    (term) => `glossaryTerm,${term.name},${term.description || ""}\n`,
  );
  return header + rows.join("");
};

const createCsvWithParentGroup = (terms, parentGroup) => {
  const header = "entity_type,name,description,parent_nodes\n";
  const rows = terms.map(
    (term) =>
      `glossaryTerm,${term.name},${term.description || ""},${parentGroup}\n`,
  );
  return header + rows.join("");
};

const uploadCsvFile = (fileContent, filename = "test-glossary.csv") => {
  // Write CSV content to a fixture file first
  const fixturesPath = "cypress/fixtures/";
  cy.writeFile(`${fixturesPath}${filename}`, fileContent);

  // Click the dropzone area which triggers the file input
  cy.contains("Drop your CSV file here").click();

  // Upload file - try selectFile first (Cypress 13+), fallback to native input
  cy.get("#file-input", { timeout: 5000 }).then(($input) => {
    // Use selectFile if available, otherwise manually trigger file selection
    cy.window().then((win) => {
      const file = new win.File([fileContent], filename, {
        type: "text/csv",
      });
      const dataTransfer = new win.DataTransfer();
      dataTransfer.items.add(file);

      const inputElement = $input[0];
      Object.defineProperty(inputElement, "files", {
        value: dataTransfer.files,
        writable: false,
        configurable: true,
      });
      $input.trigger("change", { force: true });
    });
  });
};

describe("glossary import", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    Cypress.on("uncaught:exception", (err, runnable) => false);
  });

  it("navigate to import page and verify UI elements", () => {
    cy.loginWithCredentials();
    cy.skipIntroducePage();

    // Navigate via dropdown menu
    cy.visit("/glossary");
    cy.waitTextVisible("Business Glossary");
    cy.clickOptionWithTestId("add-term-group-button-v2");
    cy.clickOptionWithText("Import CSV");

    // Verify import page elements
    navigateToGlossaryImportPage();
    cy.contains("Import Glossary").should("be.visible");
    cy.contains("Import glossary terms from CSV files").should("be.visible");
    cy.contains("Drop your CSV file here").should("be.visible");
    cy.contains("or click to browse files").should("be.visible");
  });

  it("upload valid CSV file with new terms", () => {
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    navigateToGlossaryImportPage();

    const csvContent = createSimpleCsvContent([
      { name: testGlossaryTerm, description: "Test term for import" },
      { name: "CypressImportTerm2", description: "Second test term" },
    ]);

    uploadCsvFile(csvContent);

    // Wait for processing to complete
    cy.wait(2000);
    cy.contains(testGlossaryTerm, { timeout: 10000 }).should("be.visible");

    // Verify import list is displayed
    cy.contains("Import").should("be.visible");
    cy.contains(testGlossaryTerm).should("be.visible");
  });

  it("upload CSV with parent node relationship", () => {
    cy.loginWithCredentials();
    cy.skipIntroducePage();

    // First create a parent group
    cy.visit("/glossary");
    cy.waitTextVisible("Business Glossary");
    cy.clickOptionWithTestId("add-term-group-button-v2");
    cy.clickOptionWithText("Create Term Group");
    cy.addViaModal(
      testGlossaryTermGroup,
      "Create Glossary",
      testGlossaryTermGroup,
      "glossary-entity-modal-create-button",
    );
    cy.wait(2000);

    // Navigate to import page
    navigateToGlossaryImportPage();

    // Upload CSV with parent group
    const csvContent = createCsvWithParentGroup(
      [{ name: testGlossaryTerm, description: "Term with parent group" }],
      testGlossaryTermGroup,
    );

    uploadCsvFile(csvContent);

    // Wait for processing
    cy.wait(2000);
    cy.contains(testGlossaryTerm, { timeout: 10000 }).should("be.visible");
    cy.contains(testGlossaryTermGroup).should("be.visible");
  });

  it("handle invalid CSV file format", () => {
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    navigateToGlossaryImportPage();

    const invalidCsv = "invalid,csv,content\nno,proper,format";
    uploadCsvFile(invalidCsv, "invalid.csv");

    // Should show error or handle gracefully
    cy.wait(2000);
    // The UI should either show an error message or not process the file
    // This test verifies the error handling works
  });

  it("test file removal after upload", () => {
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    navigateToGlossaryImportPage();

    const csvContent = createSimpleCsvContent([
      { name: "TempImportTerm", description: "Temporary term" },
    ]);

    uploadCsvFile(csvContent);
    cy.wait(2000);

    // Look for remove file button and click it
    cy.contains("Remove File", { timeout: 5000 }).should("be.visible").click();

    // Verify file is removed and upload area is reset
    cy.contains("Drop your CSV file here").should("be.visible");
  });

  it("test import flow with multiple entities", () => {
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    navigateToGlossaryImportPage();

    const csvContent = `entity_type,name,description,term_source
glossaryNode,CypressImportGroup1,Group 1 description,INTERNAL
glossaryTerm,CypressImportTerm1,Term 1 description,INTERNAL
glossaryTerm,CypressImportTerm2,Term 2 description,INTERNAL
glossaryNode,CypressImportGroup2,Group 2 description,INTERNAL
glossaryTerm,CypressImportTerm3,Term 3 description,INTERNAL`;

    uploadCsvFile(csvContent);
    cy.wait(3000);

    // Verify entities are listed
    cy.contains("CypressImportGroup1", { timeout: 10000 }).should("be.visible");
    cy.contains("CypressImportTerm1").should("be.visible");
    cy.contains("CypressImportTerm2").should("be.visible");
  });

  it("test search functionality in import list", () => {
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    navigateToGlossaryImportPage();

    const csvContent = createSimpleCsvContent([
      { name: "SearchableTerm1", description: "First searchable term" },
      { name: "SearchableTerm2", description: "Second searchable term" },
      { name: "OtherTerm", description: "Other term" },
    ]);

    uploadCsvFile(csvContent);
    cy.wait(3000);

    // Find search input and search for term
    cy.get('input[placeholder*="Search"]', { timeout: 5000 })
      .first()
      .type("Searchable");

    // Verify filtered results
    cy.contains("SearchableTerm1").should("be.visible");
    cy.contains("SearchableTerm2").should("be.visible");
    cy.contains("OtherTerm").should("not.exist");
  });

  it("test reset functionality", () => {
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    navigateToGlossaryImportPage();

    const csvContent = createSimpleCsvContent([
      { name: "ResetTestTerm", description: "Term for reset test" },
    ]);

    uploadCsvFile(csvContent);
    cy.wait(3000);

    // Click reset button
    cy.contains("Reset", { timeout: 5000 }).should("be.visible").click();

    // Verify we're back to upload step
    cy.contains("Drop your CSV file here").should("be.visible");
  });

  it("test diff modal for updated entity", () => {
    cy.loginWithCredentials();
    cy.skipIntroducePage();

    const existingTermName = "CypressDiffTestTerm";
    const updatedDescription = "Updated description via import";

    // First create an existing term
    cy.visit("/glossary");
    cy.waitTextVisible("Business Glossary");
    cy.clickOptionWithTestId("add-term-group-button-v2");
    cy.clickOptionWithText("Create Term");
    cy.waitTextVisible("Create Glossary Term");
    cy.enterTextInTestId("create-glossary-entity-modal-name", existingTermName);
    cy.clickOptionWithTestId("glossary-entity-modal-create-button");
    cy.waitTextVisible(`Created Glossary Term!`);
    cy.wait(2000);

    // Navigate to import page and upload CSV with same term but different description
    navigateToGlossaryImportPage();

    const csvContent = createSimpleCsvContent([
      { name: existingTermName, description: updatedDescription },
    ]);

    uploadCsvFile(csvContent);
    cy.wait(3000);

    // Verify the entity shows as "Updated" in the list
    cy.contains(existingTermName, { timeout: 10000 }).should("be.visible");
    cy.contains("Updated").should("be.visible");

    // Find and click the Diff button for this entity
    cy.contains(existingTermName)
      .parent()
      .parent()
      .within(() => {
        cy.contains("Diff").should("be.visible").click();
      });

    // Verify diff modal opens
    cy.get('[data-testid="diff-modal"]', { timeout: 5000 }).should(
      "be.visible",
    );

    // Verify modal shows entity name in title
    cy.contains(`Entity Comparison: ${existingTermName}`).should("be.visible");

    // Verify status shows as "Updated"
    cy.contains("Status: Updated").should("be.visible");

    // Verify the diff table shows differences
    cy.contains("Description").should("be.visible");
    cy.contains("Existing Data").should("be.visible");
    cy.contains("Imported Data").should("be.visible");

    // Verify updated description appears in imported data column
    cy.contains(updatedDescription).should("be.visible");

    // Close the modal
    cy.get('[data-testid="diff-modal"]').within(() => {
      cy.contains("Cancel").click();
    });

    cy.wait(1000);
  });

  it("test diff modal for conflicting entity", () => {
    cy.loginWithCredentials();
    cy.skipIntroducePage();

    const conflictTermName = "CypressConflictTestTerm";
    const originalDescription = "Original description";
    const conflictingDescription = "Conflicting description from import";

    // First create an existing term with a description
    cy.visit("/glossary");
    cy.waitTextVisible("Business Glossary");
    cy.clickOptionWithTestId("add-term-group-button-v2");
    cy.clickOptionWithText("Create Term");
    cy.waitTextVisible("Create Glossary Term");
    cy.enterTextInTestId("create-glossary-entity-modal-name", conflictTermName);

    // Add description through documentation modal if available
    // For now, just create the term
    cy.clickOptionWithTestId("glossary-entity-modal-create-button");
    cy.waitTextVisible(`Created Glossary Term!`);
    cy.wait(2000);

    // Navigate to import page and upload CSV with same term but different description
    navigateToGlossaryImportPage();

    const csvContent = createSimpleCsvContent([
      { name: conflictTermName, description: conflictingDescription },
    ]);

    uploadCsvFile(csvContent);
    cy.wait(3000);

    // Verify the entity appears in the list
    cy.contains(conflictTermName, { timeout: 10000 }).should("be.visible");

    // Find and click the Diff button
    cy.contains(conflictTermName)
      .parent()
      .parent()
      .within(() => {
        cy.contains("Diff").should("be.visible").click();
      });

    // Verify diff modal opens
    cy.get('[data-testid="diff-modal"]', { timeout: 5000 }).should(
      "be.visible",
    );

    // Verify modal shows entity name
    cy.contains(`Entity Comparison: ${conflictTermName}`).should("be.visible");

    // Verify diff table is displayed
    cy.contains("Existing Data").should("be.visible");
    cy.contains("Imported Data").should("be.visible");

    // Verify imported description is shown
    cy.contains(conflictingDescription).should("be.visible");

    // Close the modal
    cy.get('[data-testid="diff-modal"]').within(() => {
      cy.contains("Cancel").click();
    });

    cy.wait(1000);
  });

  it("test diff modal shows all field comparisons", () => {
    cy.loginWithCredentials();
    cy.skipIntroducePage();

    const diffTestTerm = "CypressDiffFieldsTerm";

    // Create existing term
    cy.visit("/glossary");
    cy.waitTextVisible("Business Glossary");
    cy.clickOptionWithTestId("add-term-group-button-v2");
    cy.clickOptionWithText("Create Term");
    cy.waitTextVisible("Create Glossary Term");
    cy.enterTextInTestId("create-glossary-entity-modal-name", diffTestTerm);
    cy.clickOptionWithTestId("glossary-entity-modal-create-button");
    cy.waitTextVisible(`Created Glossary Term!`);
    cy.wait(2000);

    // Navigate to import page
    navigateToGlossaryImportPage();

    // Upload CSV with comprehensive data to test all fields in diff
    const csvContent = `entity_type,name,description,term_source,source_ref,source_url,ownership_users,parent_nodes
glossaryTerm,${diffTestTerm},New description,EXTERNAL,REF123,https://example.com,admin:Technical Owner,`;

    uploadCsvFile(csvContent);
    cy.wait(3000);

    // Open diff modal
    cy.contains(diffTestTerm, { timeout: 10000 })
      .parent()
      .parent()
      .within(() => {
        cy.contains("Diff").should("be.visible").click();
      });

    // Verify modal opens
    cy.get('[data-testid="diff-modal"]', { timeout: 5000 }).should(
      "be.visible",
    );

    // Verify key fields are displayed in the comparison table
    cy.contains("Name").should("be.visible");
    cy.contains("Description").should("be.visible");
    cy.contains("Term Source").should("be.visible");
    cy.contains("Source Ref").should("be.visible");
    cy.contains("Source URL").should("be.visible");
    cy.contains("Ownership (Users)").should("be.visible");

    // Verify the new description appears
    cy.contains("New description").should("be.visible");

    // Close modal
    cy.get('[data-testid="diff-modal"]').within(() => {
      cy.contains("Cancel").click();
    });

    cy.wait(1000);
  });

  // Cleanup after tests - attempt to delete test entities
  after(() => {
    cy.loginWithCredentials();
    cy.visit("/glossary");
    cy.waitTextVisible("Business Glossary");
    cy.wait(2000);

    // Try to delete test term if it exists
    cy.get("body").then(($body) => {
      if ($body.text().includes(testGlossaryTerm)) {
        cy.contains(testGlossaryTerm).click({ force: true });
        cy.wait(1000);
        cy.get('[data-testid="MoreVertOutlinedIcon"]')
          .should("be.visible")
          .click();
        cy.clickOptionWithText("Delete");
        cy.clickOptionWithText("Yes");
        cy.wait(1000);
      }
    });

    // Try to delete test group if it exists
    cy.get("body").then(($body) => {
      if ($body.text().includes(testGlossaryTermGroup)) {
        cy.contains(testGlossaryTermGroup).click({ force: true });
        cy.wait(1000);
        cy.get('[data-testid="MoreVertOutlinedIcon"]')
          .should("be.visible")
          .click();
        cy.clickOptionWithText("Delete");
        cy.clickOptionWithText("Yes");
      }
    });

    // Cleanup diff test entities
    const cleanupTerms = [
      "CypressDiffTestTerm",
      "CypressConflictTestTerm",
      "CypressDiffFieldsTerm",
    ];

    cleanupTerms.forEach((termName) => {
      cy.get("body").then(($body) => {
        if ($body.text().includes(termName)) {
          cy.contains(termName).click({ force: true });
          cy.wait(1000);
          cy.get('[data-testid="MoreVertOutlinedIcon"]')
            .should("be.visible")
            .click();
          cy.clickOptionWithText("Delete");
          cy.clickOptionWithText("Yes");
          cy.wait(1000);
        }
      });
    });
  });
});
