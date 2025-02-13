function openViewEditDropDownAndClickId(data_id) {
  cy.openMultiSelect("view-select");
  cy.get('[data-testid="view-select-item"]').first().trigger("mouseover");
  cy.get('[data-testid="views-table-dropdown"]').first().trigger("mouseover");
  cy.clickOptionWithTestId(data_id);
}

describe("view select", () => {
  it("click view select, create view, clear view, make defaults, clear view", () => {
    cy.login();
    const randomNumber = Math.floor(Math.random() * 100000);
    const viewName = `Test View ${randomNumber}`;
    const newViewName = `New View Name ${randomNumber}`;

    // Resize Observer Loop warning can be safely ignored - ref. https://github.com/cypress-io/cypress/issues/22113
    const resizeObserverLoopErrRe = "ResizeObserver loop limit exceeded";
    cy.on(
      "uncaught:exception",
      (err) => !err.message.includes(resizeObserverLoopErrRe),
    );

    cy.goToStarSearchList();

    cy.log("Create a View from the select");
    cy.multiSelect("view-select", "Create new view");
    cy.waitTextVisible("Create new view");

    cy.enterTextInTestId("view-name-input", viewName);

    cy.log("Add Column Glossary Term Filter");
    cy.clickOptionWithText("Add Filter");
    cy.clickOptionWithText("Column Glossary Term");

    cy.waitTextVisible("Add Glossary Terms");
    cy.selectOptionInTagTermModal("CypressColumnInfoType");

    cy.clickOptionWithText("Save");
    cy.ensureTextNotPresent("Save");
    cy.waitTextVisible(viewName);

    cy.log("Ensure the View filter has been applied correctly.");
    cy.contains("SampleCypressHdfsDataset");
    cy.contains("cypress_logging_events");
    cy.contains(/Showing 1 - [2-4] of [2-4]/);

    cy.log("Clear the selected view");
    cy.clearView(viewName);
    cy.ensureTextNotPresent(/Showing 1 - [2-4] of [2-4]/);

    cy.log("Now edit the view");
    openViewEditDropDownAndClickId("view-dropdown-edit");
    cy.get(".ant-input-affix-wrapper > input[type='text']")
      .first()
      .clear()
      .type(newViewName);
    cy.log("Update the actual filters by adding another filter");
    cy.contains("Add Filter").click();
    cy.clickOptionWithTestId("adv-search-add-filter-description");
    cy.enterTextInTestId("edit-text-input", "log event");
    cy.clickOptionWithTestId("edit-text-done-btn");

    cy.log("Save View");
    cy.clickOptionWithTestId("view-builder-save");

    cy.contains("cypress_logging_events");
    cy.contains("of 1 result");

    cy.log("Now set the View as the personal Default");
    cy.clearView(viewName);
    openViewEditDropDownAndClickId("view-dropdown-set-user-default");

    cy.contains("of 1 result");
    cy.clearView(newViewName);
    cy.log("Now unset as the personal default");
    openViewEditDropDownAndClickId("view-dropdown-remove-user-default");

    cy.log("Now delete the View");
    cy.clearView(newViewName);
    openViewEditDropDownAndClickId("view-dropdown-delete");
    cy.clickOptionWithText("Yes");

    cy.log("Ensure that the view was deleted.");
    cy.goToViewsSettings();
    cy.ensureTextNotPresent(newViewName);
    cy.ensureTextNotPresent("of 1 result");
  });
});
