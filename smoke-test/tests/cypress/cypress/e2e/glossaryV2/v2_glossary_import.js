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

      // Verify imported node exists and expand it
      cy.contains("CypressImportNode").should("be.visible");
      cy.contains("CypressImportNode").click();
      
      // Verify imported terms exist in the glossary
      cy.contains("CypressImportTerm").should("be.visible");
      cy.contains("CypressStandaloneTerm").should("be.visible");
    });
  });