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
    const resizeObserverLoopErrRe = "ResizeObserver loop limit exceeded";
    cy.on("uncaught:exception", (err) => {
      if (err.message.includes(resizeObserverLoopErrRe)) {
        return false;
      }
    });

    cy.visit("/");
    cy.skipIntroducePage();
    cy.goToStarSearchList();
    cy.ensureElementPresent("#browse-v2");
    // Create a View from the select
    cy.clickOptionWithTestId("views-icon");
    cy.ensureElementPresent(".select-view-icon");
    cy.clickOptionWithTestId("AddOutlinedIcon");
    cy.ensureElementPresent('[data-testid="view-name-input"]');
    cy.enterTextInTestId("view-name-input", viewName);

    // Add Column Glossary Term Filter
    cy.clickOptionWithText("Add filter");
    cy.contains("Column Term").scrollIntoView();
    cy.clickOptionWithText("Column Term");
    cy.ensureElementPresent('[placeholder="Search for Column Term"]');
    cy.get('[data-testid="search-input"]').last().type("CypressColumnInfoType");
    cy.get(".ant-checkbox").should("be.visible").click();
    cy.clickOptionWithTestId("update-filters");
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

    // Now edit the view
    cy.clickOptionWithTestId("views-icon");
    cy.ensureElementPresent(".select-view-icon");
    openViewEditDropDownAndClickId("view-dropdown-edit");
    cy.clickOptionWithTestId("view-name-input").clear().type(newViewName);

    // Update the actual filters by adding another filter
    cy.clickOptionWithText("Add filter");
    cy.get(".anticon-file-text").eq(0).scrollIntoView().click();
    cy.enterTextInTestId("edit-text-input", "log event");
    cy.clickOptionWithText("Apply");
    cy.clickOptionWithTestId("view-builder-save");
    cy.waitTextVisible("cypress_logging_events");
    cy.waitTextVisible("of 1 result");
    cy.get("[class*=dataset]").should("have.length", 1);
    cy.clickOptionWithId("#v2-search-bar-views");
    openViewEditDropDownAndClickId("view-dropdown-set-user-default");
    cy.waitTextVisible("of 1 result");
    cy.get("[class*=dataset]").should("have.length", 1);
    cy.clickOptionWithId("#v2-search-bar-views");
    cy.get('[data-testid="CloseIcon"]').last().click({ force: true });
    cy.clickOptionWithTestId("views-icon");
    cy.ensureElementPresent(".select-view-icon");
    openViewEditDropDownAndClickId("view-dropdown-remove-user-default");

    // Now delete the View
    cy.clickOptionWithId("#v2-search-bar-views");
    cy.clickOptionWithTestId("views-icon");
    cy.ensureElementPresent(".select-view-icon");
    openViewEditDropDownAndClickId("view-dropdown-delete");
    cy.clickOptionWithText("Yes");

    // Ensure that the view was deleted
    cy.goToViewsSettings();
    cy.waitTextVisible("Permissions");
    cy.ensureTextNotPresent(newViewName);
    cy.ensureTextNotPresent("of 1 result");
  });
});
