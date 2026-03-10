function openViewEditDropDownAndClickId(data_id) {
  cy.get('[data-testid="views-table-dropdown"]').first().click({ force: true });
  cy.clickOptionWithTestId(data_id);
}

describe("view select", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });

  it("click view select, create view, clear view, make defaults, clear view", () => {
    cy.login();
    const randomNumber = Math.floor(Math.random() * 100000);
    const viewName = `Test View ${randomNumber}`;
    const newViewName = `New View Name ${randomNumber}`;

    // Resize Observer Loop warning can be safely ignored - ref. https://github.com/cypress-io/cypress/issues/22113
    cy.visit("/");
    cy.skipIntroducePage();
    cy.goToStarSearchList();
    cy.ensureElementPresent("#browse-v2");
    // Create a View from the select
    cy.clickOptionWithTestId("views-icon");
    cy.ensureElementWithTestIdPresent("views-type-select");
    cy.clickOptionWithTestId("AddOutlinedIcon");
    cy.ensureElementPresent('[data-testid="view-name-input"]');
    cy.enterTextInTestId("view-name-input", viewName);

    // Add Column Glossary Term filter via query builder.
    // The Build Filters tab opens with a blank condition row by default.
    // Step 1: Select the property
    cy.get('[data-testid="condition-select"]', { timeout: 10000 })
      .first()
      .should("be.visible")
      .click();
    cy.get('[data-testid="option-fieldGlossaryTerms"]', {
      timeout: 10000,
    }).click();
    // Step 2: Select the operator — value input only renders after this
    cy.get('[data-testid="condition-operator-select"]', { timeout: 10000 })
      .first()
      .should("be.visible")
      .click();
    cy.get('[data-testid="option-equals"]', { timeout: 10000 }).click();
    // Step 3: Select the glossary term entity value
    cy.get('[data-testid="entity-search-input"]', { timeout: 20000 })
      .should("exist")
      .and("be.visible")
      .click();
    cy.get('[data-testid="dropdown-search-input"]', { timeout: 10000 })
      .last()
      .type("CypressColumnInfoType");
    // The Alchemy BasicSelect renders options as <label> elements in a portal
    // (not as [role="option"] children of entity-search-input).
    // For multi-select, click the checkbox to toggle it, then confirm with Update.
    cy.contains("CypressColumnInfoType", { timeout: 20000 })
      .should("be.visible")
      .closest("label")
      .find(".ant-checkbox-wrapper")
      .click();
    cy.clickOptionWithTestId("footer-button-update");
    cy.clickOptionWithText("Save");
    cy.ensureTextNotPresent("Save");
    cy.waitTextVisible(viewName);

    // Ensure the View filter has been applied correctly.
    cy.waitTextVisible("SampleCypressHdfsDataset");
    cy.waitTextVisible("cypress_logging_events");
    cy.waitTextVisible("of 3 results");
    cy.get("[class*=dataset]").should("have.length", 3);
    cy.get(".ant-btn.ant-btn-default")
      .siblings(".close-container")
      .click({ force: true });
    cy.ensureTextNotPresent("of 3 results");

    // Now edit the view (rename only)
    cy.clickOptionWithTestId("views-icon");
    cy.ensureElementWithTestIdPresent("views-type-select");
    openViewEditDropDownAndClickId("menu-item-edit");
    cy.clickOptionWithTestId("view-name-input").clear().type(newViewName);
    cy.clickOptionWithTestId("view-builder-save");
    cy.waitTextVisible("SampleCypressHdfsDataset");
    cy.waitTextVisible("of 3 results");
    cy.get("[class*=dataset]").should("have.length", 3);
    cy.clickOptionWithId("#v2-search-bar-views");
    openViewEditDropDownAndClickId("menu-item-set-default");
    cy.waitTextVisible(newViewName);
    cy.waitTextVisible("of 3 results");
    cy.get("[class*=dataset]").should("have.length", 3);
    cy.clickOptionWithId("#v2-search-bar-views");
    cy.get('[data-testid="CloseIcon"]').last().click({ force: true });
    cy.clickOptionWithTestId("views-icon");
    cy.ensureElementWithTestIdPresent("views-type-select");
    openViewEditDropDownAndClickId("menu-item-remove-default");

    // Now delete the View
    cy.clickOptionWithId("#v2-search-bar-views");
    cy.clickOptionWithTestId("views-icon");
    cy.ensureElementWithTestIdPresent("views-type-select");
    openViewEditDropDownAndClickId("menu-item-delete");
    cy.clickOptionWithText("Yes");

    // Ensure that the view was deleted
    cy.goToViewsSettings();
    cy.waitTextVisible("Permissions");
    cy.ensureTextNotPresent(newViewName);
    cy.ensureTextNotPresent("of 3 results");
  });
});
