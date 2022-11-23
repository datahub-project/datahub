describe("manage views", () => {
    it("go to views settings page, create, edit, delete a view", () => {

        const viewName = "Test View"

        cy.login();
        cy.goToViewsSettings();

        cy.clickOptionWithText("Create new View");
        cy.waitTextVisible("New View");
        cy.get(".ant-form-item-control-input-content > input[type='text']").first().type(viewName);
        cy.clickOptionWithTestId("view-builder-save");

        // Confirm that the test has been created.
        cy.waitTextVisible("Created View!");
        cy.waitTextVisible("Test View");

        // Wait for list to refresh
        cy.wait(3000);

        // Now edit the View
        cy.clickOptionWithText("EDIT");
        cy.get(".ant-form-item-control-input-content > input[type='text']").first().clear().type("New View Name");
        cy.clickOptionWithTestId("view-builder-save");
        cy.waitTextVisible("Edited View!");
        cy.waitTextVisible("New View Name");

        // Now delete the View
        cy.clickFirstOptionWithTestId("views-table-dropdown");
        cy.clickOptionWithText("Delete");
        cy.clickOptionWithText("Yes");

        // Ensure that the entity is deleted.
        cy.ensureTextNotPresent("Test View");
    });
});
