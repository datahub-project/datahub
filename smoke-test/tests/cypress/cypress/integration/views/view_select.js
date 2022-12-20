function openViewEditDropDownAndClickId(data_id) {
  cy.openMultiSelect('view-select');
  cy.get('[data-testid="view-select-item"]').first().trigger('mouseover')
  cy.get('[data-testid="views-table-dropdown"]').first().trigger('mouseover');
  cy.clickOptionWithTestId(data_id);
}

function unfocus() {
  cy.get("input[data-testid='search-input']").click()
}

describe("view select", () => {
    it("click view select, create view, clear view, make defaults, clear view", () => {
        cy.login();
        let randomNumber = Math.floor(Math.random() * 100000);
        const viewName = `Test View ${randomNumber}`
        const newViewName = `New View Name ${randomNumber}`

        // Resize Observer Loop warning can be safely ignored - ref. https://github.com/cypress-io/cypress/issues/22113
        const resizeObserverLoopErrRe = "ResizeObserver loop limit exceeded";
        cy.on('uncaught:exception', (err) => {
          if (err.message.includes(resizeObserverLoopErrRe)) {
            return false;
          }
        });

        cy.goToStarSearchList();

        cy.log("Create a View from the select")
        cy.multiSelect('view-select', 'Create View');
        cy.waitTextVisible("Create new View")

        cy.enterTextInTestId("view-name-input", viewName);

        cy.log("Add Column Glossary Term Filter")
        cy.clickOptionWithText("Add Filter");
        cy.clickOptionWithText("Column Glossary Term");

        cy.waitTextVisible("Add Glossary Terms");
        cy.selectOptionInTagTermModal("CypressColumnInfoType");

        cy.clickOptionWithText("Save");
        cy.ensureTextNotPresent("Save");
        cy.waitTextVisible(viewName);

        cy.log("Ensure the View filter has been applied correctly.")
        cy.contains("SampleCypressHdfsDataset");
        cy.contains("cypress_logging_events");
        cy.contains("of 2 results");

        cy.log("Clear the selected view")
        cy.clickOptionWithTestId("view-select");
        cy.clickOptionWithTestId("view-select-clear");
        unfocus();
        cy.contains(viewName).should("not.be.visible");
        cy.ensureTextNotPresent("of 2 results");

        cy.log("Now edit the view")
        openViewEditDropDownAndClickId('view-dropdown-edit');
        cy.get(".ant-form-item-control-input-content > input[type='text']").first().clear().type(newViewName);
        cy.log("Update the actual filters by adding another filter")
        cy.contains("Add Filter").click();
        cy.get('[data-testid="adv-search-add-filter-description"]').click({
          force: true,
        });
        cy.get('[data-testid="edit-text-input"]').type("log event");
        cy.get('[data-testid="edit-text-done-btn"]').click();

        cy.log("Save View")
        cy.clickOptionWithTestId("view-builder-save");

        cy.contains("cypress_logging_events");
        cy.contains("of 1 result");
        unfocus();

        cy.log("Now set the View as the personal Default")
        openViewEditDropDownAndClickId('view-dropdown-set-user-default');
        unfocus();


        cy.log("Now unset as the personal default")
        openViewEditDropDownAndClickId('view-dropdown-remove-user-default');
        unfocus();

        cy.log("Now delete the View")
        openViewEditDropDownAndClickId('view-dropdown-delete');
        cy.clickOptionWithText("Yes");
        unfocus();

        cy.log("Ensure that the view was deleted.")
        cy.ensureTextNotPresent(viewName);
        cy.ensureTextNotPresent("of 1 result");

    });
});
