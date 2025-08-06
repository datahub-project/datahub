import {
  addYourAssetsModule,
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
    cy.loginWithCredentials();
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

  it("reset the homepage to organization default", () => {
    addYourAssetsModule();
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
