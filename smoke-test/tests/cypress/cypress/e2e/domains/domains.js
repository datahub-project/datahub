/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe("domains", () => {
  it("can see elements inside the domain", () => {
    cy.login();
    cy.goToDomain("urn:li:domain:marketing/Entities");

    cy.contains("Marketing");
    cy.contains("SampleCypressKafkaDataset");
    cy.contains("1 - 1 of 1");
  });
});
