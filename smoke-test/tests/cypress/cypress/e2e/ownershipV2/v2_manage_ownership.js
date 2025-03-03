describe("manage ownership", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });

  it("go to ownership types settings page, create, edit, make default, delete a ownership type", () => {
    const testOwnershipType = `Test Ownership Type ${Date.now()}`;

    cy.login();
    cy.goToOwnershipTypesSettings();
    cy.waitTextVisible("Manage Ownership");
    cy.clickOptionWithTestId("create-owner-type");
    cy.get('[data-testid="ownership-type-name-input"]').clear("T");
    cy.get('[data-testid="ownership-type-name-input"]').type(testOwnershipType);
    cy.get('[data-testid="ownership-type-description-input"]').clear("T");
    cy.get('[data-testid="ownership-type-description-input"]').type(
      "This is a test ownership type description.",
    );
    cy.get('[data-testid="ownership-builder-save"]').click();
    cy.wait(3000);
    cy.waitTextVisible(testOwnershipType);

    cy.get(`[data-row-key="${testOwnershipType}"]`)
      .first()
      .within(() => {
        cy.get('[data-testid="ownership-table-dropdown"]').click();
      });
    cy.clickOptionWithText("Edit");

    cy.get('[data-testid="ownership-type-description-input"]').clear(
      "This is an test ownership type description.",
    );
    cy.get('[data-testid="ownership-type-description-input"]').type(
      "This is an edited test ownership type description.",
    );
    cy.get('[data-testid="ownership-builder-save"]').click();
    cy.wait(3000);
    cy.waitTextVisible("This is an edited test ownership type description.");
    cy.get(`[data-row-key="${testOwnershipType}"]`)
      .first()
      .within(() => {
        cy.get('[data-testid="ownership-table-dropdown"]').click();
      });
    cy.clickOptionWithText("Delete");

    // Complete the deletion
    cy.get(".ant-popover-buttons > .ant-btn-primary").click();
    cy.wait(3000);
    cy.ensureTextNotPresent(testOwnershipType);
  });
});
