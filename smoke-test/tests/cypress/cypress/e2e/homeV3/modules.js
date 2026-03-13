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
  ensureModuleHasContent,
  clickViewAll,
  ensureUrlContains,
} from "./utils";

describe("home page modules", () => {
  beforeEach(() => {
    setThemeV2AndHomePageRedesignFlags(true);
    cy.login();
    cy.visit("/");
    cy.skipIntroducePage();
    waitUntilTemplateIsLoaded();
  });

  it("should show content in Your Assets module", () => {
    ensureModuleHasContent("your-assets", "incidents-sample-dataset");
    clickViewAll("your-assets");
    ensureUrlContains("search?filter_owners");
  });

  it("should show content in Domains module", () => {
    ensureModuleHasContent("domains", "Marketing");
    clickViewAll("domains");
    ensureUrlContains("domains");
  });
});
