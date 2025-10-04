import {
  addDomainsModule,
  addFirstHomeDefaultModule,
  addYourAssetsModule,
  createAssetCollectionModule,
  createDocumentationModule,
  createHierarchyModule,
  createLinkModule,
  dragAndDropModuleToNewRow,
  editAssetCollectionModule,
  expectModulesOrder,
  finishEditingDefaultTemplate,
  removeFirstModuleWithTestId,
  resetToOrgDefault,
  setThemeV2AndHomePageRedesignFlags,
  startEditingDefaultTemplate,
  waitUntilTemplateIsLoaded,
} from "./utils";

describe("home page modules", () => {
  beforeEach(() => {
    setThemeV2AndHomePageRedesignFlags(true);
    cy.login();
    cy.visit("/");
    cy.skipIntroducePage();
    waitUntilTemplateIsLoaded();
    resetToOrgDefault();
  });

  afterEach(() => {
    resetToOrgDefault();
  });

  Cypress.on("uncaught:exception", (err, runnable) => false);

  it.skip("add default modules", () => {
    addYourAssetsModule();
    cy.getWithTestId("edited-home-page-toast"); // wait for confirmation before continuing to prevent flakiness
    cy.getWithTestId("your-assets-module").should("have.length", 2);
    cy.getWithTestId("user-owned-entities")
      .should("be.visible")
      .children()
      .should("have.length.gte", 1);

    addDomainsModule();
    cy.getWithTestId("domains-module").should("have.length", 2);
    cy.getWithTestId("domain-entities")
      .should("be.visible")
      .children()
      .should("have.length.gte", 1);
  });

  it.skip("create custom asset collection module", () => {
    createAssetCollectionModule("Collection Module");
    cy.getWithTestId("edited-home-page-toast");
    cy.getWithTestId("asset-collection-module").should("be.visible");
    cy.getWithTestId("asset-collection-entities")
      .should("be.visible")
      .children()
      .should("have.length", 2);
  });

  it.skip("create custom hierarchy module", () => {
    createHierarchyModule("Hierarchy Module");
    cy.getWithTestId("edited-home-page-toast");
    cy.getWithTestId("hierarchy-module").should("be.visible");
    cy.getWithTestId("hierarchy-module-nodes")
      .should("be.visible")
      .children()
      .should("have.length", 1);
  });

  it.skip("create custom link module", () => {
    const linkName = "Link 1";
    createLinkModule(linkName, "www.google.com");
    cy.getWithTestId("edited-home-page-toast");
    cy.getWithTestId("link-module")
      .last()
      .scrollIntoView()
      .should("be.visible");
    cy.waitTextVisible(linkName);
  });

  it.skip("create custom documentation module", () => {
    const moduleName = "Rich Text module";
    const text = "Rich text description";
    createDocumentationModule(moduleName, text);
    cy.getWithTestId("edited-home-page-toast");
    cy.getWithTestId("documentation-module").should("be.visible");
    cy.waitTextVisible(moduleName);
    cy.waitTextVisible(text);
  });

  it.skip("remove default module", () => {
    addYourAssetsModule();
    cy.getWithTestId("edited-home-page-toast");
    cy.ensureElementWithTestIdPresent("edited-home-page-toast");
    removeFirstModuleWithTestId("your-assets-module");
    cy.getWithTestId("your-assets-module").should("have.length.lessThan", 2);
    cy.getWithTestId("your-assets-module").should("have.length", 1);
  });

  it.skip("remove custom module", () => {
    const moduleName = "Rich Text module";
    const text = "Rich text description";
    createDocumentationModule(moduleName, text);
    cy.getWithTestId("edited-home-page-toast");
    removeFirstModuleWithTestId("documentation-module");
    cy.getWithTestId("documentation-module").should("not.exist");
    cy.contains(moduleName).should("not.exist");
  });

  it.skip("should not be able to edit default module", () => {
    cy.getWithTestId("your-assets-module")
      .scrollIntoView()
      .should("be.visible")
      .within(() => {
        cy.getWithTestId("module-options").click();
      });
    cy.getWithTestId("edit-module").should(
      "have.attr",
      "aria-disabled",
      "true",
    );
  });

  it.skip("edit custom module", () => {
    const name = "Collection Module";
    const updatedName = "Collection Module Updated";
    createAssetCollectionModule(name);
    cy.getWithTestId("edited-home-page-toast");
    editAssetCollectionModule(updatedName);
    cy.waitTextVisible(updatedName);
    cy.wait(2000);
    cy.getWithTestId("asset-collection-entities")
      .children()
      .should("have.length", 3);
  });

  it.skip("add home default module", () => {
    const name = "Global Collection Module";
    addYourAssetsModule();
    cy.ensureElementWithTestIdPresent("edited-home-page-toast");
    startEditingDefaultTemplate();
    createAssetCollectionModule(name);
    finishEditingDefaultTemplate();
    addFirstHomeDefaultModule();

    cy.waitTextVisible(name);
    cy.getWithTestId("asset-collection-module").should("be.visible");
    cy.getWithTestId("asset-collection-entities")
      .should("be.visible")
      .children()
      .should("have.length", 2);

    // Clean-up
    startEditingDefaultTemplate();
    removeFirstModuleWithTestId("asset-collection-module");
    finishEditingDefaultTemplate();
  });

  it.skip("reorder module with drag-and-drop", () => {
    expectModulesOrder("your-assets-module", "domains-module");
    dragAndDropModuleToNewRow("your-assets-module");
    cy.getWithTestId("edited-home-page-toast");
    expectModulesOrder("domains-module", "your-assets-module");
  });
});
