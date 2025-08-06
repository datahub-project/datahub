import { hasOperationName } from "../utils";

export const setThemeV2AndHomePageRedesignFlags = (isOn) => {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.reply((res) => {
        res.body.data.appConfig.featureFlags.themeV2Enabled = isOn;
        res.body.data.appConfig.featureFlags.themeV2Default = isOn;
        res.body.data.appConfig.featureFlags.showNavBarRedesign = isOn;
        res.body.data.appConfig.featureFlags.showHomePageRedesign = isOn;
      });
    }
  });
};

export const clickFirstAddModuleButton = () => {
  cy.getWithTestId("add-button-container").first().realHover();
  cy.getWithTestId("add-module-button").first().should("be.visible").click();
};

export const clickLastAddModuleButton = () => {
  cy.getWithTestId("add-button-container").last().realHover();
  cy.getWithTestId("add-module-button").last().should("be.visible").click();
};

export const shouldShowDefaultTemplate = () => {
  cy.getWithTestId("your-assets-module").should("exist");
  cy.getWithTestId("domains-module").should("exist");
  cy.getWithTestId("edit-home-page-settings").click();
  cy.getWithTestId("reset-to-organization-default").should("not.exist");
};

export const shouldBeOnPersonalTemplate = () => {
  cy.getWithTestId("edit-home-page-settings").click();
  cy.getWithTestId("reset-to-organization-default").should("exist");
};

export const resetToOrgDefault = () => {
  cy.getWithTestId("edit-home-page-settings").click();
  cy.getWithTestId("reset-to-organization-default")
    .should("be.visible")
    .click();
  cy.getWithTestId("modal-confirm-button").should("be.visible").click();
};

export const startEditingDefaultTemplate = () => {
  cy.getWithTestId("edit-home-page-settings").click();
  cy.getWithTestId("edit-organization-default").should("be.visible").click();
  cy.getWithTestId("editing-default-template-bar").should("be.visible");
};

export const finishEditingDefaultTemplate = () => {
  cy.getWithTestId("finish-editing-default-template").click();
  cy.getWithTestId("editing-default-template-bar").should("not.exist");
};

export const addYourAssetsModule = () => {
  clickFirstAddModuleButton();
  cy.getWithTestId("add-your-assets-module").click();
};

export const addDomainsModule = () => {
  clickFirstAddModuleButton();
  cy.getWithTestId("add-domains-module").click();
};

export const removeFirstModuleWithTestId = (testId) => {
  cy.getWithTestId(testId)
    .first()
    .within(() => {
      cy.getWithTestId("module-options").click();
    });
  cy.getWithTestId("remove-module").click();
};

export const createAssetCollectionModule = (name) => {
  clickFirstAddModuleButton();
  cy.getWithTestId("add-asset-collection-module").click();
  cy.getWithTestId("module-name").should("be.visible").type(name);
  cy.getWithTestId("select-assets-search-results").should("exist");
  // Select first 2 assets
  cy.getWithTestId("asset-selection-checkbox").eq(0).click({ force: true });
  cy.getWithTestId("asset-selection-checkbox").eq(1).click({ force: true });
  cy.getWithTestId("selected-assets-list").children().should("have.length", 2);
  cy.getWithTestId("create-update-module-button").click();
};

export const createHierarchyModule = (name) => {
  clickFirstAddModuleButton();
  cy.getWithTestId("add-hierarchy-module").click();
  cy.getWithTestId("hierarchy-module-name").should("be.visible").type(name);
  cy.getWithTestId("hierarchy-module-nodes").should("exist");
  cy.getWithTestId("hierarchy-selection-checkbox").eq(0).click({ force: true });
  cy.getWithTestId("create-update-module-button").click();
};

export const createLinkModule = (name, url) => {
  clickLastAddModuleButton();
  cy.getWithTestId("add-link-module").click();
  cy.getWithTestId("module-name").should("be.visible").type(name);
  cy.getWithTestId("link-url").should("be.visible").type(url);
  cy.getWithTestId("create-update-module-button").click();
};

export const createDocumentationModule = (name, text) => {
  clickFirstAddModuleButton();
  cy.getWithTestId("add-documentation-module").click();
  cy.getWithTestId("module-name").should("be.visible").type(name);
  cy.getWithTestId("rich-text-documentation").should("be.visible").type(text);
  cy.getWithTestId("create-update-module-button").click();
};

export const editAssetCollectionModule = (updatedName) => {
  cy.getWithTestId("asset-collection-module")
    .should("be.visible")
    .within(() => {
      cy.getWithTestId("module-options").click();
    });
  cy.getWithTestId("edit-module").click();

  // edit name
  cy.getWithTestId("module-name")
    .should("be.visible")
    .clear()
    .type(updatedName);

  // edit selected assets, select third one
  cy.getWithTestId("asset-selection-checkbox").eq(2).click({ force: true });

  cy.getWithTestId("selected-assets-list").children().should("have.length", 3);
  cy.getWithTestId("create-update-module-button").click();
};

export const addFirstHomeDefaultModule = () => {
  clickFirstAddModuleButton();
  cy.getWithTestId("home-default-modules").trigger("mouseover");
  cy.getWithTestId("home-default-submenu-option").first().click();
};
