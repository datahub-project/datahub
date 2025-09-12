import { hasOperationName } from "../utils";

export function setThemeV2AndHomePageRedesignFlags(isOn) {
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
}

export function clickFirstAddModuleButton() {
  cy.getWithTestId("add-button-container")
    .first()
    .should("be.visible")
    .realHover();

  cy.getWithTestId("add-module-button")
    .first()
    .then(($el) => {
      if (!$el.is(":visible")) {
        cy.wait(500); // Brief wait for retry
        cy.getWithTestId("add-button-container")
          .first()
          .should("be.visible")
          .realHover();
      }
    });
  cy.getWithTestId("add-module-button").first().should("be.visible").click();
}

export function clickLastAddModuleButton() {
  cy.getWithTestId("add-button-container")
    .last()
    .should("be.visible")
    .realHover();

  cy.getWithTestId("add-module-button")
    .last()
    .then(($el) => {
      if (!$el.is(":visible")) {
        cy.wait(500); // Brief wait for retry
        cy.getWithTestId("add-button-container")
          .last()
          .should("be.visible")
          .realHover();
      }
    });
  cy.getWithTestId("add-module-button").last().should("be.visible").click();
}

export function closeForkedHomepageToast() {
  cy.wait(200);
  cy.get("body").then(($body) => {
    if (
      $body.find('[data-testid="toast-notification-close-icon"]').length > 0
    ) {
      cy.getWithTestId("toast-notification-close-icon")
        .should("be.visible")
        .click();
    }
  });
}

export function shouldShowDefaultTemplate() {
  cy.getWithTestId("your-assets-module").should("exist");
  cy.getWithTestId("domains-module").should("exist");
  cy.getWithTestId("edit-home-page-settings").click();
  cy.getWithTestId("reset-to-organization-default").should("not.exist");
}

export function shouldBeOnPersonalTemplate() {
  closeForkedHomepageToast();
  cy.getWithTestId("edit-home-page-settings").click({ force: true });
  cy.getWithTestId("reset-to-organization-default").should("exist");
  cy.getWithTestId("edit-home-page-settings").click({ force: true });
}

export function resetToOrgDefault() {
  closeForkedHomepageToast();
  cy.getWithTestId("edit-home-page-settings").click({ force: true });
  cy.getWithTestId("reset-to-organization-default").click();
  cy.getWithTestId("modal-confirm-button").filter(":visible").click();
  cy.getWithTestId("modal-confirm-button").should("not.be.visible");
}

export function startEditingDefaultTemplate() {
  closeForkedHomepageToast();
  cy.getWithTestId("edit-home-page-settings").click({ force: true });
  cy.getWithTestId("edit-organization-default").should("be.visible").click();
  cy.getWithTestId("editing-default-template-bar").should("be.visible");
}

export function finishEditingDefaultTemplate() {
  cy.getWithTestId("finish-editing-default-template")
    .should("be.visible")
    .click();
  cy.getWithTestId("editing-default-template-bar").should("not.exist");
}

export function addYourAssetsModule() {
  clickFirstAddModuleButton();
  cy.getWithTestId("add-your-assets-module").click();
}

export function addDomainsModule() {
  clickFirstAddModuleButton();
  cy.getWithTestId("add-domains-module").click();
}

export function removeFirstModuleWithTestId(testId) {
  cy.getWithTestId(testId)
    .first()
    .within(() => {
      cy.getWithTestId("module-options").click();
    });
  cy.getWithTestId("remove-module").click();
  cy.getWithTestId("modal-confirm-button").filter(":visible").click();
  cy.wait(100);
}

export function createAssetCollectionModule(name) {
  clickFirstAddModuleButton();
  cy.getWithTestId("add-asset-collection-module").click();
  cy.getWithTestId("module-name").should("be.visible").type(name);
  cy.getWithTestId("select-assets-search-results").should("exist");
  // Select first 2 assets
  cy.getWithTestId("asset-selection-checkbox").eq(0).click({ force: true });
  cy.getWithTestId("asset-selection-checkbox").eq(1).click({ force: true });
  cy.getWithTestId("selected-assets-list").children().should("have.length", 2);
  cy.getWithTestId("create-update-module-button").click();
}

export function createHierarchyModule(name) {
  clickFirstAddModuleButton();
  cy.getWithTestId("add-hierarchy-module").click();
  cy.getWithTestId("hierarchy-module-name").should("be.visible").type(name);
  cy.getWithTestId("hierarchy-module-nodes").should("exist");
  cy.getWithTestId("hierarchy-selection-checkbox")
    .first()
    .click({ force: true });
  cy.getWithTestId("create-update-module-button").click();
}

export function createLinkModule(name, url) {
  clickLastAddModuleButton();
  cy.getWithTestId("add-link-module").click();
  cy.getWithTestId("module-name").should("be.visible").type(name);
  cy.getWithTestId("link-url").should("be.visible").type(url);
  cy.getWithTestId("create-update-module-button").click();
}

export function createDocumentationModule(name, text) {
  clickFirstAddModuleButton();
  cy.getWithTestId("add-documentation-module").click();
  cy.getWithTestId("module-name").should("be.visible").type(name);
  cy.getWithTestId("rich-text-documentation").should("be.visible").type(text);
  cy.getWithTestId("create-update-module-button").click();
}

export function editAssetCollectionModule(updatedName) {
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
}

export function addFirstHomeDefaultModule() {
  clickFirstAddModuleButton();
  cy.getWithTestId("home-default-modules").trigger("mouseover");
  cy.getWithTestId("home-default-submenu-option")
    .first()
    .click({ force: true });
}

export function expectModulesOrder(firstId, secondId) {
  cy.get(`[data-testid="${firstId}"], [data-testid="${secondId}"]`).then(
    ($els) => {
      expect($els[0].dataset.testid).to.eq(firstId);
    },
  );
}

export function dragAndDropModuleToNewRow(moduleId) {
  cy.get(`[data-testid="${moduleId}"] [data-testid="large-module-drag-handle"]`)
    .realMouseDown({ button: "left", position: "center" })
    .realMouseMove(0, 10, { position: "center" })
    .wait(200);
  cy.getWithTestId("new-row-drop-zone")
    .last()
    .realMouseMove(0, 0, { position: "center" })
    .realMouseUp();
}

export function waitUntilTemplateIsLoaded() {
  cy.getWithTestId("home-template-wrapper");
}
