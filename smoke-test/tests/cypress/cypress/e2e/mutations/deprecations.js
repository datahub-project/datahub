/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe("dataset deprecation", () => {
  it("go to dataset and check deprecation works", () => {
    const urn =
      "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)";
    const datasetName = "cypress_logging_events";
    cy.login();
    cy.goToDataset(urn, datasetName);
    cy.openThreeDotDropdown();
    cy.clickOptionWithText("Mark as deprecated");
    cy.addViaFormModal("test deprecation", "Add Deprecation Details");
    cy.waitTextVisible("Deprecation Updated");
    cy.waitTextVisible("DEPRECATED");
    cy.openThreeDotDropdown();
    cy.clickOptionWithText("Mark as un-deprecated");
    cy.waitTextVisible("Deprecation Updated");
    cy.ensureTextNotPresent("DEPRECATED");
    cy.openThreeDotDropdown();
    cy.clickOptionWithText("Mark as deprecated");
    cy.addViaFormModal("test deprecation", "Add Deprecation Details");
    cy.waitTextVisible("Deprecation Updated");
    cy.waitTextVisible("DEPRECATED");
    cy.contains("DEPRECATED").trigger("mouseover", { force: true });
    cy.waitTextVisible("Deprecation note");
    cy.get("[role='tooltip']").contains("Mark as un-deprecated").click();
    cy.waitTextVisible("Confirm Mark as un-deprecated");
    cy.get("button").contains("Yes").click();
    cy.waitTextVisible("Marked assets as un-deprecated!");
    cy.ensureTextNotPresent("DEPRECATED");
  });
});
