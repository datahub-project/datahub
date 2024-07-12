const nevigateGlossaryPage = () => {
  cy.visit("/glossary");
  cy.waitTextVisible("Business Glossary");
};

const applyAdvancedSearchFilter = (filterType, value) => {
  cy.get('[aria-label="filter"]').first().click();
  cy.clickOptionWithId("#search-results-advanced-search");
  cy.clickOptionWithText("Add Filter");
  cy.clickOptionWithText(filterType);
  cy.get("div.ant-select-selection-overflow").click();
  cy.get(".ant-select-item-option-content").contains(value).click();
  cy.clickOptionWithText("Add Tags");
  cy.clickOptionWithText("Done");
};

const createTerm = (glossaryTerm) => {
  cy.clickOptionWithText("CypressNode");
  cy.clickOptionWithText("Add Glossary Term");
  cy.waitTextVisible("Create Glossary Term");
  cy.enterTextInTestId("create-glossary-entity-modal-name", glossaryTerm);
  cy.clickOptionWithTestId("glossary-entity-modal-create-button");
  cy.waitTextVisible(glossaryTerm);
  cy.clickOptionWithText(glossaryTerm);
};

const invokeTextFromElement = (selector) =>
  cy.get(selector).last().invoke("text");
const deleteGlossary = (message) => {
  cy.get(".anticon-edit").should("be.visible");
  cy.get('[data-testid="MoreVertOutlinedIcon"]')
    .first()
    .should("be.visible")
    .click();
  cy.get('[data-testid="entity-menu-delete-button"]')
    .should("be.visible")
    .click();
  cy.clickOptionWithText("Yes");
  cy.waitTextVisible(message);
};

const elementVisibility = () => {
  cy.get('a[href*="urn:li"] span[class^="ant-typography"]').should(
    "be.visible",
  );
};

const enterKeyInSearchBox = (text) => {
  cy.get('[data-testid="search-input"]')
    .eq(2)
    .should("be.visible")
    .click()
    .type(`${text}{enter}`);
};

describe("glossaryTerm", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.loginWithCredentials();
    cy.wait(1000);
    cy.handleIntroducePage();
    nevigateGlossaryPage();
  });

  it("can search related entities by query", () => {
    createTerm("GlossaryNewTerm");
    cy.clickOptionWithSpecificClass(".anticon-link", 0);
    elementVisibility();
    cy.get(".ant-checkbox-input").first().click();
    cy.clickOptionWithId("#continueButton");
    cy.waitTextVisible("Added Glossary Term to entities!");
    cy.clickOptionWithSpecificClass(".anticon.anticon-appstore", 0);
    invokeTextFromElement(
      'a[href*="urn:li"] span[class^="ant-typography"]',
    ).then((assetText) => {
      enterKeyInSearchBox(assetText);
      cy.wait(2000);
      cy.get('a[href*="urn:li"] span[class^="ant-typography"]')
        .first()
        .should("have.text", assetText);
    });
  });

  it("can apply filters on related entities", () => {
    cy.clickOptionWithText("CypressNode");
    cy.clickOptionWithText("GlossaryNewTerm");
    cy.clickOptionWithSpecificClass(".anticon.anticon-appstore", 0);
    elementVisibility();
    cy.clickOptionWithSpecificClass(".anticon-filter", 0);
    cy.waitTextVisible("Filter");
    cy.get('input.ant-checkbox-input[data-testid^="facet-tags-urn:li:tag:"]')
      .first()
      .click();
    cy.mouseHoverOnFirstElement(".ant-tag");
    invokeTextFromElement(".ant-tooltip-inner").then((assetText) => {
      cy.get('a[href*="urn:li"] span[class^="ant-typography"]')
        .first()
        .should("be.visible")
        .click();
      cy.waitTextVisible(assetText);
    });
  });

  it("can search related entities by a specific tag using advanced search", () => {
    cy.clickOptionWithText("CypressNode");
    cy.clickOptionWithText("GlossaryNewTerm");
    cy.clickOptionWithSpecificClass(".anticon.anticon-appstore", 0);
    elementVisibility();
    applyAdvancedSearchFilter("Tag", "Cypress");
    elementVisibility();
    enterKeyInSearchBox("Baz Chart 1");
    elementVisibility();
    cy.contains("Baz Chart 1").should("exist");
    deleteGlossary("Deleted Glossary Term!");
  });
});
