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
} from "./utils";

describe("home page modules", () => {
  beforeEach(() => {
    setThemeV2AndHomePageRedesignFlags(true);
    cy.login();
    cy.visit("/");
    cy.skipIntroducePage();
  });

  Cypress.on("uncaught:exception", (err, runnable) => false);

  it("add default modules", () => {
    addYourAssetsModule();
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

    // Clean-up
    resetToOrgDefault();
  });

  it("create custom asset collection module", () => {
    createAssetCollectionModule("Collection Module");
    cy.getWithTestId("asset-collection-module").should("be.visible");
    cy.getWithTestId("asset-collection-entities")
      .should("be.visible")
      .children()
      .should("have.length", 2);

    // Clean-up
    resetToOrgDefault();
  });

  it("create custom hierarchy module", () => {
    createHierarchyModule("Hierarchy Module");
    cy.getWithTestId("hierarchy-module").should("be.visible");
    cy.getWithTestId("hierarchy-module-nodes")
      .should("be.visible")
      .children()
      .should("have.length", 1);

    // Clean-up
    resetToOrgDefault();
  });

  it("create custom link module", () => {
    const linkName = "Link 1";
    createLinkModule(linkName, "www.google.com");
    cy.getWithTestId("link-module").should("be.visible");
    cy.waitTextVisible(linkName);

    // Clean-up
    resetToOrgDefault();
  });

  it("create custom documentation module", () => {
    const moduleName = "Rich Text module";
    const text = "Rich text description";
    createDocumentationModule(moduleName, text);
    cy.getWithTestId("documentation-module").should("be.visible");
    cy.waitTextVisible(moduleName);
    cy.waitTextVisible(text);

    // Clean-up
    resetToOrgDefault();
  });

  it("remove default module", () => {
    addYourAssetsModule();
    cy.ensureElementWithTestIdPresent("edited-home-page-toast");
    removeFirstModuleWithTestId("your-assets-module");
    cy.getWithTestId("your-assets-module").should("have.length.lessThan", 2);
    cy.getWithTestId("your-assets-module").should("have.length", 1);

    // Clean-up
    resetToOrgDefault();
  });

  it("remove custom module", () => {
    const moduleName = "Rich Text module";
    const text = "Rich text description";
    createDocumentationModule(moduleName, text);
    removeFirstModuleWithTestId("documentation-module");
    cy.getWithTestId("documentation-module").should("not.exist");
    cy.contains(moduleName).should("not.exist");

    // Clean-up
    resetToOrgDefault();
  });

  it("should not be able to edit default module", () => {
    cy.getWithTestId("your-assets-module")
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

  it("edit custom module", () => {
    const name = "Collection Module";
    const updatedName = "Collection Module Updated";
    createAssetCollectionModule(name);
    editAssetCollectionModule(updatedName);
    cy.waitTextVisible(updatedName);
    cy.wait(2000);
    cy.getWithTestId("asset-collection-entities")
      .children()
      .should("have.length", 3);

    // Clean-up
    resetToOrgDefault();
  });

  it("add home default module", () => {
    const name = "Global Collection Module";
    addYourAssetsModule();
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
    resetToOrgDefault();
  });

  it("reorder module with drag-and-drop", () => {
    expectModulesOrder("your-assets-module", "domains-module");
    dragAndDropModuleToNewRow("your-assets-module");
    expectModulesOrder("domains-module", "your-assets-module");

    // Clean-up
    resetToOrgDefault();
  });
});
