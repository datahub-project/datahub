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
} from "./utils";

describe("home page templates", () => {
  beforeEach(() => {
    setThemeV2AndHomePageRedesignFlags(true);
    cy.login();
    cy.visit("/");
    cy.skipIntroducePage();
  });

  Cypress.on("uncaught:exception", (err, runnable) => false);

  it("view default homepage template", () => {
    cy.getWithTestId("page-title").should("exist");
    cy.getWithTestId("search-bar").should("exist");
    cy.getWithTestId("edit-home-page-settings").should("exist");
    shouldShowDefaultTemplate();
  });

  it("fork the homepage and create personal template", () => {
    addYourAssetsModule();
    cy.getWithTestId("your-assets-module").should("have.length", 2);
    shouldBeOnPersonalTemplate();
    resetToOrgDefault();
  });

  it("create personal template, then log back in to check the updated template", () => {
    addYourAssetsModule();
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

  it("reset the homepage to organization default", () => {
    addYourAssetsModule();
    cy.wait(1000);
    resetToOrgDefault();
    shouldShowDefaultTemplate();
  });

  it("edit the default homepage", () => {
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
