/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe("login", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });

  it("logs in", () => {
    cy.visit("/");
    cy.get("input[data-testid=username]").type(Cypress.env("ADMIN_USERNAME"));
    cy.get("input[data-testid=password]").type(Cypress.env("ADMIN_PASSWORD"));
    cy.contains("Sign In").click();
    cy.skipIntroducePage();
    cy.contains("Discover");
  });
});
