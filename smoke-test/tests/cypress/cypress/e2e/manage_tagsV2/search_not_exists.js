/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe("tags - search not exists", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.login();
  });

  it("verify search not exists", () => {
    cy.visit("/tags");
    cy.get('[data-testid="page-title"]').should("contain.text", "Manage Tags");
    cy.get('[data-testid="urn:li:tag:Cypress-name"]').should(
      "contain.text",
      "Cypress",
    );
    cy.get('[data-testid="tag-search-input"]').type("invalidvalue");
    cy.get('[data-testid="tags-not-found"]').should(
      "contain.text",
      "No tags found for your search query",
    );
  });
});
