describe("view select", () => {
    it("click view select, create view, clear view, make defaults, clear view", () => {
        const viewName = "Test View"
        const viewDescription = "View Description"
        const newViewName = "New View Name"

        // Resize Observer Loop warning can be safely ignored - ref. https://github.com/cypress-io/cypress/issues/22113
        const resizeObserverLoopErrRe = "ResizeObserver loop limit exceeded";
        cy.on('uncaught:exception', (err) => {
          if (err.message.includes(resizeObserverLoopErrRe)) {
            return false;
          }
        });

        cy.login();
        cy.visit("/search?page=1&query=%2A&unionType=0");

        // Create a View from the select
        cy.get('[data-testid="view-select"]').click();
        cy.clickOptionWithTestId("view-select-create");

        cy.get(".ant-form-item-control-input-content > input[type='text']").first().type(viewName);

        // Add Column Glossary Term Filter
        cy.contains("Add Filter").click();
        cy.contains("Column Glossary Term").click({ force: true });
        cy.get('[data-testid="tag-term-modal-input"]').type("CypressColumnInfo");
        cy.wait(2000);
        cy.get('[data-testid="tag-term-option"]').click({ force: true });
        cy.get('[data-testid="add-tag-term-from-modal-btn"]').click({
          force: true,
        });

        cy.clickOptionWithTestId("view-builder-save");
        cy.waitTextVisible(viewName);

        cy.wait(2000); // Allow search to reload

        // Ensure the View filter has been applied correctly.
        cy.contains("SampleCypressHdfsDataset");
        cy.contains("cypress_logging_events");
        cy.contains("of 2 results");

        // Clear the selected view view
        cy.get('[data-testid="view-select"]').click();
        cy.clickOptionWithTestId("view-select-clear");
        cy.get("input[data-testid=search-input]").click(); // unfocus
        cy.contains(viewName).should("not.be.visible");

        cy.wait(2000); // Allow search to reload
        cy.ensureTextNotPresent("of 2 results");

        // Now edit the view
        cy.get('[data-testid="view-select"]').click();
        cy.get('[data-testid="view-select-item"]').first().trigger('mouseover')
        cy.get('[data-testid="views-table-dropdown"]').first().trigger('mouseover');
        cy.get('[data-testid="view-dropdown-edit"]').click();
        cy.get(".ant-form-item-control-input-content > input[type='text']").first().clear().type(newViewName);
        // Update the actual filters by adding another filter
        cy.contains("Add Filter").click();
        cy.get('[data-testid="adv-search-add-filter-description"]').click({
          force: true,
        });
        cy.get('[data-testid="edit-text-input"]').type("log event");
        cy.get('[data-testid="edit-text-done-btn"]').click();

        // Save View
        cy.clickOptionWithTestId("view-builder-save");

        cy.wait(2000); // Allow search to reload

        cy.contains("cypress_logging_events");
        cy.contains("of 1 result");

        // Now set the View as the personal Default
        cy.get('[data-testid="view-select"]').click();
        cy.get('[data-testid="view-select-item"]').first().trigger('mouseover')
        cy.get('[data-testid="views-table-dropdown"]').first().trigger('mouseover');
        cy.get('[data-testid="view-dropdown-set-user-default"]').click();
        cy.get("input[data-testid=search-input]").click(); // unfocus

        // Now unset as the personal default
        cy.get('[data-testid="view-select"]').click();
        cy.get('[data-testid="view-select-item"]').first().trigger('mouseover')
        cy.get('[data-testid="views-table-dropdown"]').first().trigger('mouseover');
        cy.get('[data-testid="view-dropdown-remove-user-default"]').click();
        cy.get("input[data-testid=search-input]").click(); // unfocus

        // Now delete the View
        cy.get('[data-testid="view-select"]').click();
        cy.get('[data-testid="view-select-item"]').first().trigger('mouseover')
        cy.get('[data-testid="views-table-dropdown"]').first().trigger('mouseover');
        cy.get('[data-testid="view-dropdown-delete"]').click();
        cy.clickOptionWithText("Yes");

        // Ensure that the view was deleted.
        cy.ensureTextNotPresent(viewName);
        cy.wait(2000); // Allow search to reload
        cy.ensureTextNotPresent("of 1 result");

    });
});
