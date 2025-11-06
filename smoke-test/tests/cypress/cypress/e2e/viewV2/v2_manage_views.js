describe("manage views", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });
  it("go to views settings page, create, edit, make default, delete a view", () => {
    const viewName = "Test View";

    cy.login();
    cy.goToViewsSettings();
    cy.waitTextVisible("Settings");
    cy.wait(1000);
    cy.clickOptionWithText("Create View");
    cy.get('[data-testid="view-name-input"]').click().type(viewName);
    cy.clickOptionWithTestId("view-builder-save");

    // Confirm that the test has been created.
    cy.waitTextVisible("Test View");

    // Now edit the View
    cy.clickFirstOptionWithTestId("views-table-dropdown");
    cy.get('[data-testid="view-dropdown-edit"]').click({ force: true });
    cy.get('[data-testid="view-name-input"]')
      .click()
      .clear()
      .type("New View Name");
    cy.clickOptionWithTestId("view-builder-save");
    cy.waitTextVisible("New View Name");

    // Now make the view the default
    cy.clickFirstOptionWithTestId("views-table-dropdown");
    cy.get('[data-testid="view-dropdown-set-user-default"]').click({
      force: true,
    });

    // Now unset as the default
    cy.clickFirstOptionWithTestId("views-table-dropdown");
    cy.get('[data-testid="view-dropdown-remove-user-default"]').click({
      force: true,
    });

    // Now delete the View
    cy.clickFirstOptionWithTestId("views-table-dropdown");
    cy.get('[data-testid="view-dropdown-delete"]').click({ force: true });
    cy.clickOptionWithText("Yes");
  });
});
