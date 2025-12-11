/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe("manage tags", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(false);
    cy.login();
  });

  it("Manage Tags Page - Verify search bar placeholder", () => {
    cy.visit("/tags");
    cy.get('[data-testid="tag-search-input"]').should(
      "have.attr",
      "placeholder",
      "Search tags...",
    );
  });

  it("Manage Tags Page - Verify Title, Search, and Results", () => {
    cy.visit("/tags");
    cy.get('[data-testid="page-title"]').should("contain.text", "Manage Tags");
    cy.get('[data-testid="urn:li:tag:Cypress-name"]').should(
      "contain.text",
      "Cypress",
    );
    cy.get('[data-testid="tag-search-input"]').type("Cypress");
    cy.get('[data-testid="urn:li:tag:Cypress-name"]').should(
      "contain.text",
      "Cypress",
    );
  });

  it("Manage Tags Page - Verify search not exists", () => {
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
