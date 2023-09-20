const glossaryTerm = "CypressGlosssaryNavigationTerm";
const glossaryTermGroup = "CypressGlosssaryNavigationGroup";
const glossaryParentGroup = "Cypress";

describe("glossary sidebar navigation test", () => {
    it("create term and term parent group, move and delete term group", () => {
        //create a new term group and term, move term to the group
        cy.loginWithCredentials();
        cy.goToGlossaryList();
        cy.clickOptionWithText("Add Term Group");
        cy.waitTextVisible("Create Term Group");
        cy.get(".ant-input-affix-wrapper > input[type='text']").first().type(glossaryTermGroup);
        cy.get(".ant-modal-footer > button:last-child").click();
        cy.get('*[class^="GlossaryBrowser"]').contains(glossaryTermGroup).should("be.visible");
        cy.clickOptionWithText("Add Term");
        cy.waitTextVisible("Create Glossary Term");
        cy.get(".ant-input-affix-wrapper > input[type='text']").first().type(glossaryTerm);
        cy.get(".ant-modal-footer > button:last-child").click();
        cy.get('*[class^="GlossaryBrowser"]').contains(glossaryTerm).click();
        cy.waitTextVisible("No documentation yet");
        cy.openThreeDotDropdown();
        cy.clickOptionWithText("Move");
        cy.get('[role="dialog"]').contains(glossaryTermGroup).click({force: true});
        cy.get('[role="dialog"]').contains(glossaryTermGroup).should("be.visible");
        cy.get("button").contains("Move").click();
        cy.waitTextVisible("Moved Glossary Term!");
        //ensure the new term is under the parent term group in the navigation sidebar
        cy.get('*[class^="GlossaryBrowser"]').contains(glossaryTermGroup).click();
        cy.get('*[class^="GlossaryEntitiesList"]').contains(glossaryTerm).should("be.visible");
        //move a term group from the root level to be under a parent term group
        cy.goToGlossaryList();
        cy.clickOptionWithText(glossaryTermGroup);
        cy.openThreeDotDropdown();
        cy.clickOptionWithText("Move");
        cy.get('[role="dialog"]').contains(glossaryParentGroup).click({force: true});
        cy.get('[role="dialog"]').contains(glossaryParentGroup).should("be.visible");
        cy.get("button").contains("Move").click();
        cy.waitTextVisible("Moved Term Group!");
        //ensure it is no longer on the sidebar navigator at the top level but shows up under the new parent
        cy.get('*[class^="GlossaryBrowser"]').contains(glossaryParentGroup).click();
        cy.get('*[class^="GlossaryEntitiesList"]').contains(glossaryTermGroup).should("be.visible");
        //delete a term group
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
        //ensure it is no longer in the sidebar navigator
        cy.ensureTextNotPresent(glossaryTerm);
        cy.ensureTextNotPresent(glossaryTermGroup);
    });
});