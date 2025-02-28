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
  cy.clickOptionWithTestId("add-tag-term-from-modal-btn");
};

const createTerm = (glossaryTerm) => {
  cy.clickOptionWithText("CypressNode");
  cy.clickOptionWithTestId("add-term-button");
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
  cy.get('[data-testid^="preview-urn:"]').should("be.visible");
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
    cy.skipIntroducePage();
    nevigateGlossaryPage();
    cy.wait(1000); // adding waits because UI flickers with new UI and causes cypress to miss things
  });

  it("can search related entities by query", () => {
    createTerm("GlossaryNewTerm");
    cy.clickFirstOptionWithTestId("glossary-batch-add");
    elementVisibility();
    cy.get(".ant-checkbox-input").first().click();
    cy.clickOptionWithId("#continueButton");
    cy.waitTextVisible("Added Glossary Term to entities!");
    cy.clickTextOptionWithClass(".ant-tabs-tab", "Related Assets");
    invokeTextFromElement(
      '[data-testid^="preview-urn:"] div[data-testid="entity-title"]',
    ).then((assetText) => {
      enterKeyInSearchBox(assetText);
      cy.wait(2000);
      cy.get('[data-testid^="preview-urn:"] div[data-testid="entity-title"]')
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
      cy.get('[data-testid^="preview-urn:"] div[data-testid="entity-title"]')
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
    enterKeyInSearchBox("Baz Dashboard");
    elementVisibility();
    cy.contains("Baz Dashboard").should("exist");
    deleteGlossary("Deleted Glossary Term!");
  });
});
