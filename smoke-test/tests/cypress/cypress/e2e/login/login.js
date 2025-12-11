/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe("login", () => {
  it("logs in", () => {
    cy.visit("/");
    cy.get("input[data-testid=username]").type("datahub");
    cy.get("input[data-testid=password]").type("datahub");
    cy.contains("Sign In").click();
    cy.contains(`Welcome back, ${Cypress.env("ADMIN_DISPLAYNAME")}`);
  });
});
