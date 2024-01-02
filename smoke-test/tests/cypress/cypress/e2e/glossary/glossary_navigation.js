const glossaryTerm = "CypressGlosssaryNavigationTerm";
const glossarySecondTerm = "CypressGlossarySecondTerm";
const glossaryTermGroup = "CypressGlosssaryNavigationGroup";
const glossaryParentGroup = "CypressNode";

describe("glossary sidebar navigation test", () => {
    it("create term and term parent group, move and delete term group", () => {
        cy.loginWithCredentials();

        // Create term group
        cy.createGlossaryTermGroup(glossaryTermGroup);
        cy.clickOptionWithTestId("add-term-button");
        cy.createTerm(glossaryTerm);
        cy.moveGlossaryEntityToGroup(glossaryTerm, glossaryTermGroup, `Moved Glossary Term!`);

        // Ensure the new term is under the parent term group in the navigation sidebar
        cy.navigateToParentAndCheckTermGroup(glossaryTermGroup, glossaryTerm);

        // Create another term and move it to the same term group
        cy.clickOptionWithText(glossaryTermGroup);
        cy.openThreeDotDropdown();
        cy.clickOptionWithTestId("entity-menu-add-term-button");
        cy.createTerm(glossarySecondTerm)
        cy.moveGlossaryEntityToGroup(glossarySecondTerm, glossaryTermGroup, `Moved Glossary Term!`);

         // Navigate to parent group and check term group visibility
         cy.navigateToParentAndCheckTermGroup(glossaryTermGroup, glossarySecondTerm);

        // Switch between terms and ensure the "Properties" tab is active
        cy.clickOptionWithText(glossaryTerm);
        cy.get('[data-testid="entity-tab-headers-test-id"]').contains("Properties").click({ force: true });
        cy.get('[data-node-key="Properties"]').contains("Properties").should("have.attr", "aria-selected", "true");
        cy.clickOptionWithText(glossarySecondTerm);
        cy.get('[data-node-key="Properties"]').contains("Properties").should("have.attr", "aria-selected", "true");

        // Move a term group from the root level to be under a parent term group
        cy.goToGlossaryList();
        cy.moveGlossaryEntityToGroup(glossaryTermGroup, glossaryParentGroup,'Moved Term Group!');

        // Navigate to parent group and check term group visibility
        cy.navigateToParentAndCheckTermGroup(glossaryParentGroup, glossaryTermGroup);

        // Delete glossary terms and term group
        cy.deleteGlossaryTerm(glossaryParentGroup, glossaryTermGroup, glossaryTerm);
        cy.deleteGlossaryTerm(glossaryParentGroup, glossaryTermGroup, glossarySecondTerm);
        cy.deleteTermGroup(glossaryParentGroup, glossaryTermGroup);

        // Ensure it is no longer in the sidebar navigator
        cy.ensureTextNotPresent(glossaryTerm);
        cy.ensureTextNotPresent(glossaryTermGroup);
    });
});