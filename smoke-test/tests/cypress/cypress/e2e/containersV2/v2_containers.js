/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe("containers", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });

  it("can see elements inside the container", () => {
    cy.login();
    cy.goToContainer("urn:li:container:348c96555971d3f5c1ffd7dd2e7446cb");
    cy.contains("jaffle_shop");
    cy.contains("customers");
    cy.contains("customers_source");
    cy.contains("orders");
    cy.contains("raw_orders");
    cy.contains("1 - 9 of 9");
  });
});
