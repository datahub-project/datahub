const glossaryTerm = "CypressGlosssaryNavigationTerm";
const glossarySecondTerm = "CypressGlossarySecondTerm";
const glossaryTermGroup = "CypressGlosssaryNavigationGroup";
const glossaryParentGroup = "CypressNode";

const createTerm = (glossaryTerm) => {
  cy.waitTextVisible("Create Glossary Term");
  cy.enterTextInTestId("create-glossary-entity-modal-name", glossaryTerm);
  cy.clickOptionWithTestId("glossary-entity-modal-create-button");
};

const navigateToParentAndCheckTermGroup = (parentGroup, termGroup) => {
  cy.get('[data-testid="glossary-browser-sidebar"]')
    .contains(parentGroup)
    .click();
  cy.get('*[class^="GlossaryEntitiesList"]')
    .contains(termGroup)
    .should("be.visible");
};

const moveGlossaryEntityToGroup = (
  sourceEntity,
  targetEntity,
  confirmationMsg,
) => {
  cy.clickOptionWithText(sourceEntity);
  cy.get('[data-testid="entity-header-dropdown"]').should("be.visible");
  cy.openThreeDotDropdown();
  cy.clickOptionWithText("Move");
  cy.get('[data-testid="move-glossary-entity-modal"]')
    .contains(targetEntity)
    .click({ force: true });
  cy.get('[data-testid="move-glossary-entity-modal"]')
    .contains(targetEntity)
    .should("be.visible");
  cy.clickOptionWithTestId("glossary-entity-modal-move-button");
  cy.waitTextVisible(confirmationMsg);
};

const deleteGlossaryTerm = (parentGroup, termGroup, term) => {
  cy.goToGlossaryList();
  cy.clickOptionWithText(parentGroup);
  cy.clickOptionWithText(termGroup);
  cy.clickOptionWithText(term);
  cy.deleteFromDropdown();
  cy.waitTextVisible("Deleted Glossary Term!");
};

describe("glossary sidebar navigation test", () => {
  it("create term and term parent group, move and delete term group", () => {
    cy.loginWithCredentials();

    // Create term group and term
    cy.createGlossaryTermGroup(glossaryTermGroup);
    cy.clickOptionWithTestId("add-term-button");
    createTerm(glossaryTerm);
    moveGlossaryEntityToGroup(
      glossaryTerm,
      glossaryTermGroup,
      `Moved Glossary Term!`,
    );
    navigateToParentAndCheckTermGroup(glossaryTermGroup, glossaryTerm);

    // Create another term and move it to the same term group
    cy.clickOptionWithText(glossaryTermGroup);
    cy.openThreeDotDropdown();
    cy.clickOptionWithTestId("entity-menu-add-term-button");
    createTerm(glossarySecondTerm);
    moveGlossaryEntityToGroup(
      glossarySecondTerm,
      glossaryTermGroup,
      `Moved Glossary Term!`,
    );
    navigateToParentAndCheckTermGroup(glossaryTermGroup, glossarySecondTerm);

    // Switch between terms and ensure the "Properties" tab is active
    cy.clickOptionWithText(glossaryTerm);
    cy.get('[data-testid="entity-tab-headers-test-id"]')
      .contains("Properties")
      .click({ force: true });
    cy.get('[data-node-key="Properties"]')
      .contains("Properties")
      .should("have.attr", "aria-selected", "true");
    cy.clickOptionWithText(glossarySecondTerm);
    cy.get('[data-node-key="Properties"]')
      .contains("Properties")
      .should("have.attr", "aria-selected", "true");

    // Move a term group from the root level to be under a parent term group
    cy.goToGlossaryList();
    moveGlossaryEntityToGroup(
      glossaryTermGroup,
      glossaryParentGroup,
      "Moved Term Group!",
    );
    navigateToParentAndCheckTermGroup(glossaryParentGroup, glossaryTermGroup);

    // Delete glossary terms and term group
    deleteGlossaryTerm(glossaryParentGroup, glossaryTermGroup, glossaryTerm);
    deleteGlossaryTerm(
      glossaryParentGroup,
      glossaryTermGroup,
      glossarySecondTerm,
    );

    cy.goToGlossaryList();
    cy.clickOptionWithText(glossaryParentGroup);
    cy.clickOptionWithText(glossaryTermGroup);
    cy.deleteFromDropdown();
    cy.waitTextVisible("Deleted Term Group!");

    // Ensure it is no longer in the sidebar navigator
    cy.ensureTextNotPresent(glossaryTerm);
    cy.ensureTextNotPresent(glossaryTermGroup);
  });
});
