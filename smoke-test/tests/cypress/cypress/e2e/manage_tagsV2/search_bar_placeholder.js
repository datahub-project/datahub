/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe("tags - search bar placeholder", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.login();
  });

  it("verify search bar placeholder", () => {
    cy.visit("/tags");
    cy.get('[data-testid="tag-search-input"]').should(
      "have.attr",
      "placeholder",
      "Search tags...",
    );
  });
});
