describe("manage ownership", () => {
  it("go to ownership types settings page, create, edit, make default, delete a ownership type", () => {
    const testOwnershipType = `Test Ownership Type ${Date.now()}`;

    cy.login();
    cy.goToOwnershipTypesSettings();
    cy.waitTextVisible("Manage Ownership");
    cy.clickOptionWithTestId("create-owner-type-v2");
    cy.get('[data-testid="ownership-type-name-input"]').clear();
    cy.get('[data-testid="ownership-type-name-input"]').type(testOwnershipType);
    cy.get('[data-testid="ownership-type-description-input"]').clear();
    cy.get('[data-testid="ownership-type-description-input"]').type(
      "This is a test ownership type description.",
    );
    cy.get('[data-testid="ownership-builder-save"]').click();
    cy.wait(3000);
    cy.waitTextVisible(testOwnershipType);

    // Edit the ownership type
    cy.contains("tr", testOwnershipType)
      .find('[data-testid="ownership-table-dropdown"]')
      .click();
    cy.get('[data-testid="menu-item-edit"]').click();

    cy.get('[data-testid="ownership-type-description-input"]').clear();
    cy.get('[data-testid="ownership-type-description-input"]').type(
      "This is an edited test ownership type description.",
    );
    cy.get('[data-testid="ownership-builder-save"]').click();
    cy.wait(3000);
    cy.waitTextVisible("This is an edited test ownership type description.");

    // Delete the ownership type
    cy.contains("tr", testOwnershipType)
      .find('[data-testid="ownership-table-dropdown"]')
      .click();
    cy.get('[data-testid="menu-item-delete"]').click();

    cy.wait(3000);
    cy.ensureTextNotPresent(testOwnershipType);
  });
});
