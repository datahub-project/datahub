describe("glossary import", () => {
    beforeEach(() => {
      cy.setIsThemeV2Enabled(true);
      Cypress.on("uncaught:exception", (err, runnable) => false);
    });
  
    it("should navigate to the glossary import page and upload CSV", () => {
      cy.loginWithCredentials();
      cy.skipIntroducePage();
  
      // Navigate to import page
      cy.visit("/glossary/import");
      cy.url().should("include", "/glossary/import");
  
      // Wait for the page to fully load in V2 mode
      cy.wait(2000); // Give time for theme to settle
  
      // Verify page loaded
      cy.contains("Import Glossary").should("be.visible");
      cy.get("[data-testid='dropzone-table']").should("be.visible");
      cy.contains("Drop your CSV file here").should("be.visible");
  
      // Alternative approach: Use readFile and manually trigger events
      cy.readFile("cypress/fixtures/glossary-import-test.csv").then((fileContent) => {
        cy.get("#file-input").then(($input) => {
          const blob = new Blob([fileContent], { type: "text/csv" });
          const file = new File([blob], "glossary-import-test.csv", {
            type: "text/csv",
          });
          const dataTransfer = new DataTransfer();
          dataTransfer.items.add(file);
          $input[0].files = dataTransfer.files;
  
          // Manually trigger the change event
          const changeEvent = new Event("change", { bubbles: true });
          $input[0].dispatchEvent(changeEvent);
        });
      });
  
      // Wait for table to appear
      cy.get("[data-testid='glossary-import-list-table']", { 
        timeout: 30000 
      }).should("be.visible");

      cy.contains("button", "Expand All").click();
  
      // Verify all entities are displayed
      cy.contains("CypressImportNode").should("be.visible");
      cy.contains("CypressImportTerm").should("be.visible");
      cy.contains("CypressStandaloneTerm").should("be.visible");
  
      // Verify status badges show "New"
      cy.contains("New").should("be.visible");

      // Click the import button
      cy.contains("button", "Import").click();

      // Wait for import to complete
      cy.contains("Import completed successfully", { timeout: 30000 }).should("be.visible");

      // Navigate to glossary to verify terms were imported
      cy.visit("/glossary");
      cy.wait(2000);

      // Verify imported node exists and expand it by clicking the arrow icon
      cy.contains("CypressImportNode").should("be.visible");
      cy.contains("CypressImportNode")
        .parent()
        .parent()
        .find('[data-testid="KeyboardArrowRightRoundedIcon"]')
        .click();
      cy.wait(500);
      
      // Verify imported terms exist in the glossary
      cy.contains("CypressImportTerm").should("be.visible");
      cy.contains("CypressStandaloneTerm").should("be.visible");
    });

    it("should upload updates to existing glossary entities", () => {
      cy.loginWithCredentials();
      cy.skipIntroducePage();

      // Navigate to import page
      cy.visit("/glossary/import");
      cy.url().should("include", "/glossary/import");
      cy.wait(2000);

      // Upload updates CSV
      cy.readFile("cypress/fixtures/glossary-import-updates.csv").then((fileContent) => {
        cy.get("#file-input").then(($input) => {
          const blob = new Blob([fileContent], { type: "text/csv" });
          const file = new File([blob], "glossary-import-updates.csv", {
            type: "text/csv",
          });
          const dataTransfer = new DataTransfer();
          dataTransfer.items.add(file);
          $input[0].files = dataTransfer.files;

          const changeEvent = new Event("change", { bubbles: true });
          $input[0].dispatchEvent(changeEvent);
        });
      });

      // Wait for table to appear
      cy.get("[data-testid='glossary-import-list-table']", { 
        timeout: 30000 
      }).should("be.visible");

      cy.contains("button", "Expand All").click();

      // Verify entities are displayed
      cy.contains("CypressImportNode").should("be.visible");
      cy.contains("CypressImportTerm").should("be.visible");
      cy.contains("CypressStandaloneTerm").should("be.visible");

      // Verify status badges show "Updated"
      cy.contains("Updated").should("be.visible");

      // Click the import button
      cy.contains("button", "Import").click();

      // Wait for import to complete
      cy.contains("Import completed successfully", { timeout: 30000 }).should("be.visible");

      // Navigate to glossary and verify updates
      cy.visit("/glossary");
      cy.wait(2000);

      // Expand the node and click on a term to verify updated description
      cy.contains("CypressImportNode").should("be.visible");
      cy.contains("CypressImportNode")
        .parent()
        .parent()
        .find('[data-testid="KeyboardArrowRightRoundedIcon"]')
        .click();
      cy.wait(1000);
      cy.contains("CypressImportTerm").click();
      cy.wait(1000);
      
      // Verify updated description is present
      cy.contains("Updated test term with new description").should("be.visible");
    });

    it("should upload CSV with nested nodes hierarchy", () => {
      cy.loginWithCredentials();
      cy.skipIntroducePage();

      // Navigate to import page
      cy.visit("/glossary/import");
      cy.url().should("include", "/glossary/import");
      cy.wait(2000);

      // Upload nested CSV
      cy.readFile("cypress/fixtures/glossary-import-nested.csv").then((fileContent) => {
        cy.get("#file-input").then(($input) => {
          const blob = new Blob([fileContent], { type: "text/csv" });
          const file = new File([blob], "glossary-import-nested.csv", {
            type: "text/csv",
          });
          const dataTransfer = new DataTransfer();
          dataTransfer.items.add(file);
          $input[0].files = dataTransfer.files;

          const changeEvent = new Event("change", { bubbles: true });
          $input[0].dispatchEvent(changeEvent);
        });
      });

      // Wait for table to appear
      cy.get("[data-testid='glossary-import-list-table']", { 
        timeout: 30000 
      }).should("be.visible");

      // Expand all to see full hierarchy
      cy.contains("button", "Expand All").click();
      cy.wait(1000);

      // Verify all levels of hierarchy are displayed
      cy.contains("CypressRootNode").should("be.visible");
      cy.contains("CypressLevel2Node").should("be.visible");
      cy.contains("CypressLevel3Node").should("be.visible");
      cy.contains("CypressLevel4Node").should("be.visible");
      cy.contains("CypressDeepTerm").should("be.visible");
      cy.contains("CypressLevel3Term").should("be.visible");

      // Verify status badges show "New"
      cy.contains("New").should("be.visible");

      // Click the import button
      cy.contains("button", "Import").click();

      // Wait for import to complete
      cy.contains("Import completed successfully", { timeout: 30000 }).should("be.visible");

      // Navigate to glossary to verify nested structure
      cy.visit("/glossary");
      cy.wait(2000);

      // Verify root node exists
      cy.contains("CypressRootNode").should("be.visible");
      
      // Expand through the hierarchy by clicking arrow icons
      cy.contains("CypressRootNode")
        .parent()
        .parent()
        .find('[data-testid="KeyboardArrowRightRoundedIcon"]')
        .click();
      cy.wait(1000);
      cy.contains("CypressLevel2Node").should("be.visible");
      
      cy.contains("CypressLevel2Node")
        .parent()
        .parent()
        .find('[data-testid="KeyboardArrowRightRoundedIcon"]')
        .click();
      cy.wait(1000);
      cy.contains("CypressLevel3Node").should("be.visible");
    });

    it("should upload complex CSV with diverse features", () => {
      cy.loginWithCredentials();
      cy.skipIntroducePage();

      // Navigate to import page
      cy.visit("/glossary/import");
      cy.url().should("include", "/glossary/import");
      cy.wait(2000);

      // Upload complex CSV
      cy.readFile("cypress/fixtures/glossary-import-complex.csv").then((fileContent) => {
        cy.get("#file-input").then(($input) => {
          const blob = new Blob([fileContent], { type: "text/csv" });
          const file = new File([blob], "glossary-import-complex.csv", {
            type: "text/csv",
          });
          const dataTransfer = new DataTransfer();
          dataTransfer.items.add(file);
          $input[0].files = dataTransfer.files;

          const changeEvent = new Event("change", { bubbles: true });
          $input[0].dispatchEvent(changeEvent);
        });
      });

      // Wait for table to appear
      cy.get("[data-testid='glossary-import-list-table']", { 
        timeout: 30000 
      }).should("be.visible");

      cy.contains("button", "Expand All").click();
      cy.wait(1000);

      // Verify diverse entity types are displayed
      cy.contains("CypressComplexRoot").should("be.visible");
      cy.contains("CypressSubCategory").should("be.visible");
      cy.contains("CypressPatientData").should("be.visible");
      cy.contains("CypressDemographics").should("be.visible");
      cy.contains("CypressVitalSigns").should("be.visible");
      cy.contains("CypressLabResults").should("be.visible");
      cy.contains("CypressStandaloneComplex").should("be.visible");
      cy.contains("CypressMetadataNode").should("be.visible");
      cy.contains("CypressDataQuality").should("be.visible");
      cy.contains("CypressCompliance").should("be.visible");
      cy.contains("CypressInheritedTerm").should("be.visible");

      // Verify status badges show "New"
      cy.contains("New").should("be.visible");

      // Click the import button
      cy.contains("button", "Import").click();

      // Wait for import to complete
      cy.contains("Import completed successfully", { timeout: 30000 }).should("be.visible");

      // Navigate to glossary to spot-check key entities
      cy.visit("/glossary");
      cy.wait(2000);

      // Verify key entities exist
      cy.contains("CypressComplexRoot").should("be.visible");
      cy.contains("CypressMetadataNode").should("be.visible");
      cy.contains("CypressStandaloneComplex").should("be.visible");

      // Expand parent node to reveal CypressPatientData and verify details
      // First verify the term is not visible before expanding
      cy.contains("CypressPatientData").should("not.exist");
      
      // Expand CypressComplexRoot to reveal its child terms
      cy.contains("CypressComplexRoot")
        .parent()
        .parent()
        .find('[data-testid="KeyboardArrowRightRoundedIcon"]')
        .click();
      cy.wait(1000);
      
      // Verify CypressPatientData is now visible after expansion
      cy.contains("CypressPatientData").should("be.visible");
      
      // Click on the term to view its details
      cy.contains("CypressPatientData").click();
      cy.wait(1000);
      cy.contains("Patient data term with relationships").should("be.visible");
    });
  });