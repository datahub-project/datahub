/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

describe("auto-complete", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.skipIntroducePage();
    cy.hideOnboardingTour();
    cy.login();
    // look for a dataset
    cy.visit("/");
    cy.wait(2000);
  });

  it("should see auto-complete results after typing in a search query", () => {
    cy.get("input[data-testid=search-input]")
      .should("be.visible")
      .type("SampleCypressHive");
    cy.contains("Datasets");
    cy.contains("SampleCypressHiveDataset");
    cy.focused().clear();

    // look for a dashboard
    cy.get("input[data-testid=search-input]").type("baz");
    cy.contains("Dashboards");
    cy.contains("Baz Dashboard");
    cy.focused().clear();

    // look for a dataflow
    cy.get("input[data-testid=search-input]").type("dataflow user");
    cy.contains("Pipelines").scrollIntoView();
    cy.contains("Users");
    cy.focused().clear();
  });

  it("should send you to the entity profile after clicking on an auto-complete option", () => {
    cy.get("input[data-testid=search-input]")
      .should("be.visible")
      .type("SampleCypressHiveDataset", { delay: 0 });
    cy.get('[data-testid^="auto-complete-option"]').first().click();
    cy.url().should(
      "include",
      "dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)",
    );
  });
});
