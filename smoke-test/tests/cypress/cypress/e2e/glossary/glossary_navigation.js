const glossaryTerm = "CypressGlosssaryNavigationTerm";
const glossaryTermGroup = "CypressGlosssaryNavigationGroup";
const glossaryParentGroup = "CypressNode";

describe("glossary sidebar navigation test", () => {
    it("create term and term parent group, move and delete term group", () => {

        // Create a new term group and term, move term to the group
        cy.loginWithCredentials();
        cy.goToGlossaryList();
        cy.clickOptionWithTestId("add-term-group-button");
        cy.waitTextVisible("Create Term Group");
        cy.enterTextInTestId("create-glossary-entity-modal-name", glossaryTermGroup);
        cy.clickOptionWithTestId("glossary-entity-modal-create-button");
        cy.get('[data-testid="glossary-browser-sidebar"]').contains(glossaryTermGroup).should("be.visible");
        cy.clickOptionWithTestId("add-term-button"); 
        cy.waitTextVisible("Created Term Group!");
        cy.waitTextVisible("Create Glossary Term");
        cy.enterTextInTestId("create-glossary-entity-modal-name", glossaryTerm);
        cy.clickOptionWithTestId("glossary-entity-modal-create-button").wait(3000);
        cy.get('[data-testid="glossary-browser-sidebar"]').contains(glossaryTerm).click().wait(3000);
        cy.openThreeDotDropdown();
        cy.clickOptionWithTestId("entity-menu-move-button")
        cy.get('[data-testid="move-glossary-entity-modal"]').contains(glossaryTermGroup).click({force: true});
        cy.get('[data-testid="move-glossary-entity-modal"]').contains(glossaryTermGroup).should("be.visible");
        cy.clickOptionWithTestId("glossary-entity-modal-move-button");
        cy.waitTextVisible("Moved Glossary Term!");

        // Ensure the new term is under the parent term group in the navigation sidebar
        cy.get('[data-testid="glossary-browser-sidebar"]').contains(glossaryTermGroup).click().wait(3000);
        cy.get('*[class^="GlossaryEntitiesList"]').contains(glossaryTerm).should("be.visible");

        // Move a term group from the root level to be under a parent term group
        cy.goToGlossaryList();
        cy.clickOptionWithText(glossaryTermGroup);
        cy.wait(3000)
        cy.openThreeDotDropdown();
        cy.clickOptionWithText("Move");
        cy.get('[data-testid="move-glossary-entity-modal"]').contains(glossaryParentGroup).click({force: true});
        cy.get('[data-testid="move-glossary-entity-modal"]').contains(glossaryParentGroup).should("be.visible");
        cy.clickOptionWithTestId("glossary-entity-modal-move-button");
        cy.waitTextVisible("Moved Term Group!");

        // Ensure it is no longer on the sidebar navigator at the top level but shows up under the new parent
        cy.get('[data-testid="glossary-browser-sidebar"]').contains(glossaryParentGroup).click().wait(3000);
        cy.get('*[class^="GlossaryEntitiesList"]').contains(glossaryTermGroup).should("be.visible");

        // Delete a term group
        cy.goToGlossaryList();
        cy.clickOptionWithText(glossaryParentGroup);
        cy.clickOptionWithText(glossaryTermGroup);
        cy.clickOptionWithText(glossaryTerm).wait(3000);
        cy.deleteFromDropdown();
        cy.waitTextVisible("Deleted Glossary Term!");
        cy.clickOptionWithText(glossaryParentGroup);
        cy.clickOptionWithText(glossaryTermGroup).wait(3000);
        cy.deleteFromDropdown();
        cy.waitTextVisible("Deleted Term Group!");

        // Ensure it is no longer in the sidebar navigator
        cy.ensureTextNotPresent(glossaryTerm);
        cy.ensureTextNotPresent(glossaryTermGroup);
    });
});