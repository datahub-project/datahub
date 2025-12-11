/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe("manage views", () => {
  it("go to views settings page, create, edit, make default, delete a view", () => {
    const viewName = "Test View";

    cy.login();
    cy.goToViewsSettings();

    cy.clickOptionWithText("Create new View");
    cy.get(".ant-input-affix-wrapper > input[type='text']")
      .first()
      .type(viewName);
    cy.clickOptionWithTestId("view-builder-save");

    // Confirm that the test has been created.
    cy.waitTextVisible("Test View");

    // Now edit the View
    cy.clickFirstOptionWithTestId("views-table-dropdown");
    cy.get('[data-testid="view-dropdown-edit"]').click({ force: true });
    cy.get(".ant-input-affix-wrapper > input[type='text']")
      .first()
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
