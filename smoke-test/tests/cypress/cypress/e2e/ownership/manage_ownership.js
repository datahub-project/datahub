/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe("manage ownership", () => {
  it("go to ownership types settings page, create, edit, make default, delete a ownership type", () => {
    const viewName = "Test View";

    cy.login();
    cy.goToOwnershipTypesSettings();

    cy.clickOptionWithTestId("create-owner-type-v1");
    cy.get('[data-testid="ownership-type-name-input"]').clear("T");
    cy.get('[data-testid="ownership-type-name-input"]').type(
      "Test Ownership Type",
    );
    cy.get('[data-testid="ownership-type-description-input"]').clear("T");
    cy.get('[data-testid="ownership-type-description-input"]').type(
      "This is a test ownership type description.",
    );
    cy.get('[data-testid="ownership-builder-save"]').click();
    cy.wait(3000);
    cy.waitTextVisible("Test Ownership Type");

    cy.get(
      '[data-row-key="Test Ownership Type"] > :nth-child(3) > .anticon > svg',
    )
      .first()
      .click();
    cy.clickOptionWithText("Edit");
    cy.get('[data-testid="ownership-type-description-input"]').clear(
      "This is an test ownership type description.",
    );
    cy.get('[data-testid="ownership-type-description-input"]').type(
      "This is an edited test ownership type description.",
    );
    cy.get('[data-testid="ownership-builder-save"] > span').click();
    cy.wait(3000);
    cy.waitTextVisible("This is an edited test ownership type description.");

    cy.get(
      '[data-row-key="Test Ownership Type"] > :nth-child(3) > .anticon > svg',
    )
      .first()
      .click();
    cy.clickOptionWithText("Delete");
    cy.get(".ant-popover-buttons > .ant-btn-primary").click();
    cy.wait(3000);
    cy.ensureTextNotPresent("Test Ownership Type");
  });
});
