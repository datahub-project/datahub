describe("Glossary Entity Tab Navigation Test", () => {
    const glossaryTermGroup = "CypressGlossaryNavigationTermGroup";
    const glossaryFirstTerm = "CypressGlossaryFirstTerm";
    const glossarySecondTerm = "CypressGlossarySecondTerm";
  
    it("should create terms and term groups, move terms, navigate between term tabs and terms, and delete term groups", () => {
        cy.loginWithCredentials();
        cy.goToGlossaryList();
      // Create a new term group and term, move term to the group
      cy.clickOptionWithTestId("add-term-group-button");
      cy.waitTextVisible("Create Term Group");
      cy.enterTextInTestId("create-glossary-entity-modal-name", glossaryTermGroup);
      cy.clickOptionWithTestId("glossary-entity-modal-create-button");
  
      // Assert the presence of the created term group in the sidebar
      cy.get('[data-testid="glossary-browser-sidebar"]').contains(glossaryTermGroup).should("be.visible");
  
      // Create a term and move it to the term group
      cy.clickOptionWithTestId("add-term-button");
      cy.waitTextVisible("Created Term Group!");
      cy.waitTextVisible("Create Glossary Term");
      cy.enterTextInTestId("create-glossary-entity-modal-name", glossaryFirstTerm);
      cy.clickOptionWithTestId("glossary-entity-modal-create-button").wait(3000);
      cy.get('[data-testid="glossary-browser-sidebar"]').contains(glossaryFirstTerm).click().wait(3000);
      cy.openThreeDotDropdown();
      cy.clickOptionWithTestId("entity-menu-move-button");
      cy.get('[data-testid="move-glossary-entity-modal"]').contains(glossaryTermGroup).click({force: true});
      cy.get('[data-testid="move-glossary-entity-modal"]').contains(glossaryTermGroup).should("be.visible");
      cy.clickOptionWithTestId("glossary-entity-modal-move-button");
      cy.waitTextVisible("Moved Glossary Term!");
  
      // Ensure the new term is under the parent term group in the navigation sidebar
      cy.get('[data-testid="glossary-browser-sidebar"]').contains(glossaryTermGroup).click().wait(3000);
      cy.get('*[class^="GlossaryEntitiesList"]').contains(glossaryFirstTerm).should("be.visible");
  
      // Create another term and move it to the same term group
      cy.clickOptionWithText(glossaryTermGroup);
      cy.openThreeDotDropdown();
      cy.clickOptionWithTestId("entity-menu-add-term-button");
      cy.waitTextVisible("Create Glossary Term");
      cy.enterTextInTestId("create-glossary-entity-modal-name", glossarySecondTerm);
      cy.clickOptionWithTestId("glossary-entity-modal-create-button").wait(3000);
      cy.clickOptionWithText(glossarySecondTerm).wait(3000);
      cy.openThreeDotDropdown();
      cy.clickOptionWithTestId("entity-menu-move-button");
      cy.get('[data-testid="move-glossary-entity-modal"]').contains(glossaryTermGroup).click({force: true});
      cy.get('[data-testid="move-glossary-entity-modal"]').contains(glossaryTermGroup).should("be.visible");
      cy.clickOptionWithTestId("glossary-entity-modal-move-button");
      cy.waitTextVisible("Moved Glossary Term!");
  
      // Ensure the new term is under the parent term group in the navigation sidebar
      cy.get('[data-testid="glossary-browser-sidebar"]').contains(glossaryTermGroup).click().wait(5000);
      cy.get('*[class^="GlossaryEntitiesList"]').contains(glossarySecondTerm).should("be.visible");
  
      // Switch between terms and ensure the "Properties" tab is active
      cy.clickOptionWithText(glossaryFirstTerm).wait(3000);
      cy.get('[data-testid="entity-tab-headers-test-id"]').contains("Properties").click({force: true});
      cy.get('[data-node-key="Properties"]').contains("Properties").should("have.attr", "aria-selected", "true");
  
      cy.clickOptionWithText(glossarySecondTerm).wait(3000);
      cy.get('[data-node-key="Properties"]').contains("Properties").should("have.attr", "aria-selected", "true");
  
      // Delete terms and term group
      cy.goToGlossaryList();
      cy.clickOptionWithText(glossaryTermGroup);
      cy.clickOptionWithText(glossarySecondTerm).wait(3000);
      cy.deleteFromDropdown();
      cy.waitTextVisible("Deleted Glossary Term!");
  
      cy.goToGlossaryList();
      cy.clickOptionWithText(glossaryTermGroup);
      cy.clickOptionWithText(glossaryFirstTerm).wait(3000);
      cy.deleteFromDropdown();
      cy.waitTextVisible("Deleted Glossary Term!");
  
      cy.clickOptionWithText(glossaryTermGroup).wait(3000);
      cy.deleteFromDropdown();
      cy.waitTextVisible("Deleted Term Group!");
  
      // Ensure items are no longer present in the sidebar navigator
      cy.ensureTextNotPresent(glossaryFirstTerm);
      cy.ensureTextNotPresent(glossarySecondTerm);
      cy.ensureTextNotPresent(glossaryTermGroup);
    });
  });
  


  