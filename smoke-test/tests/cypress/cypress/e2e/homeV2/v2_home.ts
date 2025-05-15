import "../../support/commands"; // This import makes the commands available and clickable
import { homePage } from "../../support/pages/HomePage";

describe("Home Page V2", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.skipIntroducePage();
    cy.on("uncaught:exception", (err) => {
      // Prevent Cypress from failing the test on uncaught exceptions
      return false;
    });
  });

  it("should display all required elements on the home page", () => {
    cy.login();

    homePage
      .visit()
      .shouldHaveSvgContent()
      .shouldHaveContentContainer()
      .shouldHaveNavigationMenu();
  });
});
