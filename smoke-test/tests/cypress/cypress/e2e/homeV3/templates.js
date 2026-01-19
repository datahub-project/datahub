import {
  setThemeV2AndHomePageRedesignFlags,
  waitUntilTemplateIsLoaded,
  ensureThatModuleIsAvailable,
} from "./utils";

describe("home page templates", () => {
  beforeEach(() => {
    setThemeV2AndHomePageRedesignFlags(true);
    cy.login();
    cy.visit("/");
    cy.skipIntroducePage();
    waitUntilTemplateIsLoaded();
  });

  it("should show default modules", () => {
    ensureThatModuleIsAvailable("Your Assets", "your-assets");
    ensureThatModuleIsAvailable("Domains", "domains");
  });
});
