/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe("domains", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    Cypress.on("uncaught:exception", (err, runnable) => false);
  });

  it("can see elements inside the domain", () => {
    cy.login();
    cy.goToDomain("urn:li:domain:testing/Entities");
    cy.contains("Testing");
    cy.get('[data-node-key="Assets"]').click();
    cy.contains("Baz Dashboard");
    cy.contains("CypressTerm");
    cy.contains("1 - 2 of 2");
  });
});
