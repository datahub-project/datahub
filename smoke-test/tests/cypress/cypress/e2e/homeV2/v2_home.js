import { hasOperationName } from "../utils";

function setFeatureFlags() {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.alias = "gqlappConfigQuery";

      req.on("response", (res) => {
        res.body.data.appConfig.featureFlags.themeV2Enabled = true;
        res.body.data.appConfig.featureFlags.themeV2Default = true;
        res.body.data.appConfig.featureFlags.showNavBarRedesign = true;
        res.body.data.appConfig.featureFlags.showHomePageRedesign = false;
      });
    }
  });
}

describe("home", () => {
  beforeEach(() => {
    setFeatureFlags();
    cy.skipIntroducePage();
    cy.on("uncaught:exception", (err, runnable) => false);
  });
  it("home page shows ", () => {
    cy.login();
    cy.visit("/");
    cy.get('[xmlns="http://www.w3.org/2000/svg"]').should("exist");
    cy.get('[data-testid="home-page-content-container"').should("exist");
    cy.get('[data-testid="nav-menu-links"').should("exist");
  });
});
