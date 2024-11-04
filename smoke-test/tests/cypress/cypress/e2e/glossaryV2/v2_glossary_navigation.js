const glossaryTerm = "CypressGlosssaryNavigationTerm";
const glossarySecondTerm = "CypressGlossarySecondTerm";
const glossaryTermGroup = "CypressGlosssaryNavigationGroup";
const glossaryParentGroup = "CypressNode";
let myUrl;

const nevigateGlossaryPage = () => {
  cy.visit("/glossary");
  cy.waitTextVisible("Business Glossary");
  cy.wait(1000);
};

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
  cy.contains("Created Glossary Term!").should("not.exist");
  cy.get(".anticon-edit").should("be.visible");
  cy.get('[data-testid="MoreVertOutlinedIcon"]').should("be.visible").click();
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
const deleteGlossary = (message) => {
  cy.get(".anticon-edit").should("be.visible");
  cy.get('[data-testid="MoreVertOutlinedIcon"]').should("be.visible").click();
  cy.clickOptionWithText("Delete");
  cy.clickOptionWithText("Yes");
  cy.waitTextVisible(message);
};

describe("glossary sidebar navigation test", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();
  });

  it("create term and term parent group, move and delete term group", () => {
    nevigateGlossaryPage();
    cy.clickOptionWithTestId("add-term-group-button-v2");
    cy.waitTextVisible("Create Glossary");
    cy.enterTextInTestId(
      "create-glossary-entity-modal-name",
      glossaryTermGroup,
    );
    cy.clickOptionWithTestId("glossary-entity-modal-create-button");
    cy.get('[data-testid="glossary-browser-sidebar"]')
      .contains(glossaryTermGroup)
      .should("be.visible");
    cy.waitTextVisible(`Created Term Group!`);
    cy.clickOptionWithText(glossaryTermGroup);
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
    cy.contains("Moved Glossary Term!").should("not.exist");
    cy.clickOptionWithTestId("add-term-button");
    createTerm(glossarySecondTerm);
    moveGlossaryEntityToGroup(
      glossarySecondTerm,
      glossaryTermGroup,
      `Moved Glossary Term!`,
    );
    navigateToParentAndCheckTermGroup(glossaryTermGroup, glossarySecondTerm);

    // Switch between terms and ensure the "Properties" tab is active
    cy.clickOptionWithText(glossaryTerm);
    cy.clickTextOptionWithClass(".ant-tabs-tab", "Properties");
    cy.get('[data-node-key="Properties"] .ant-tabs-tab-btn').should(
      "have.attr",
      "aria-selected",
      "true",
    );
    cy.clickOptionWithText(glossarySecondTerm);
    cy.get('[data-node-key="Properties"] .ant-tabs-tab-btn').should(
      "have.attr",
      "aria-selected",
      "true",
    );

    // Move a term group from the root level to be under a parent term group
    nevigateGlossaryPage();
    moveGlossaryEntityToGroup(
      glossaryTermGroup,
      glossaryParentGroup,
      "Moved Term Group!",
    );
    navigateToParentAndCheckTermGroup(glossaryParentGroup, glossaryTermGroup);

    // Delete glossary terms and term group
    nevigateGlossaryPage();
    cy.clickOptionWithText(glossaryParentGroup);
    cy.clickOptionWithText(glossaryTermGroup);
    cy.clickOptionWithText(glossaryTerm);
    deleteGlossary("Deleted Glossary Term!");
    cy.clickOptionWithText(glossaryParentGroup);
    cy.clickOptionWithText(glossaryTermGroup);
    cy.ensureTextNotPresent(glossaryTerm);
    nevigateGlossaryPage();
    cy.clickOptionWithText(glossaryParentGroup);
    cy.clickOptionWithText(glossaryTermGroup);
    cy.clickOptionWithText(glossarySecondTerm);
    deleteGlossary("Deleted Glossary Term!");
    cy.clickOptionWithText(glossaryParentGroup);
    cy.clickOptionWithText(glossaryTermGroup);
    cy.ensureTextNotPresent(glossarySecondTerm);
    nevigateGlossaryPage();
    cy.clickOptionWithText(glossaryParentGroup);
    cy.clickOptionWithText(glossaryTermGroup);
    deleteGlossary("Deleted Term Group!");

    // Ensure it is no longer in the sidebar navigator
    cy.ensureTextNotPresent(glossaryTerm);
    cy.ensureTextNotPresent(glossaryTermGroup);
  });
});
