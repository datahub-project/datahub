/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

import {
  addYourAssetsModule,
  createAssetCollectionModule,
  finishEditingDefaultTemplate,
  removeFirstModuleWithTestId,
  resetToOrgDefault,
  setThemeV2AndHomePageRedesignFlags,
  shouldBeOnPersonalTemplate,
  shouldShowDefaultTemplate,
  startEditingDefaultTemplate,
  waitUntilTemplateIsLoaded,
} from "./utils";

describe("home page templates", () => {
  beforeEach(() => {
    setThemeV2AndHomePageRedesignFlags(true);
    cy.login();
    cy.visit("/");
    cy.skipIntroducePage();
    waitUntilTemplateIsLoaded();
  });

  Cypress.on("uncaught:exception", (err, runnable) => false);

  it.skip("view default homepage template", () => {
    cy.getWithTestId("page-title").should("exist");
    cy.getWithTestId("search-bar").should("exist");
    cy.getWithTestId("edit-home-page-settings").should("exist");
    shouldShowDefaultTemplate();
  });

  it.skip("fork the homepage and create personal template", () => {
    addYourAssetsModule();
    cy.getWithTestId("edited-home-page-toast"); // wait for confirmation before continuing to prevent flakiness
    cy.getWithTestId("your-assets-module").should("have.length", 2);
    shouldBeOnPersonalTemplate();
    resetToOrgDefault();
  });

  it.skip("create personal template, then log back in to check the updated template", () => {
    addYourAssetsModule();
    cy.getWithTestId("edited-home-page-toast");
    createAssetCollectionModule("Collection Module");
    cy.wait(2000);
    shouldBeOnPersonalTemplate();
    cy.logoutV2();
    cy.login();
    cy.visit("/");
    cy.getWithTestId("your-assets-module").should("have.length", 2);
    cy.getWithTestId("asset-collection-module").should("be.visible");

    // Clean-up
    resetToOrgDefault();
  });

  it.skip("reset the homepage to organization default", () => {
    addYourAssetsModule();
    cy.getWithTestId("edited-home-page-toast");
    cy.wait(1000);
    resetToOrgDefault();
    shouldShowDefaultTemplate();
  });

  it.skip("edit the default homepage", () => {
    startEditingDefaultTemplate();
    addYourAssetsModule();
    finishEditingDefaultTemplate();
    cy.getWithTestId("your-assets-module").should("have.length", 2);

    // Clean-up
    startEditingDefaultTemplate();
    removeFirstModuleWithTestId("your-assets-module");
    finishEditingDefaultTemplate();
  });
});
