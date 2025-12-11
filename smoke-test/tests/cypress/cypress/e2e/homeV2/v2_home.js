/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe("home", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.skipIntroducePage();
    cy.on("uncaught:exception", (err, runnable) => false);
  });
  it("home page shows ", () => {
    cy.login();
    cy.visit("/");
    cy.get('[xmlns="http://www.w3.org/2000/svg"]').should("exist");
    cy.get('[data-testid="home-page-content-container"').should("exist");
    cy.get('[data-testid="nav-menu-links"').should("exist");
  });
});
