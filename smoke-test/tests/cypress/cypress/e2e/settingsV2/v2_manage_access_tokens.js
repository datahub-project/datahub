import { aliasQuery, hasOperationName } from "../utils";

const test_id = Math.floor(Math.random() * 100000);

describe("manage access tokens", () => {
  before(() => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  const setTokenAuthEnabledFlag = (isOn) => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.reply((res) => {
          res.body.data.appConfig.authConfig.tokenAuthEnabled = isOn;
          res.body.data.appConfig.featureFlags.themeV2Enabled = true;
          res.body.data.appConfig.featureFlags.themeV2Default = true;
        });
      }
    });
  };

  it("create and revoke access token", () => {
    // create access token, verify token on ui
    setTokenAuthEnabledFlag(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.goToAccessTokenSettings();
    cy.clickOptionWithTestId("add-token-button");
    cy.clickOptionWithSpecificClass(".personal-token-dropdown-option", 0);
    cy.enterTextInTestId("create-access-token-name", `Token Name${test_id}`);
    cy.enterTextInTestId(
      "create-access-token-description",
      `Token Description${test_id}`,
    );
    cy.clickOptionWithTestId("create-access-token-button");
    cy.waitTextVisible("New Personal Access Token");
    cy.get('[data-testid="access-token-value"]').should("be.visible");
    cy.get('[data-testid="access-token-value"]')
      .invoke("text")
      .should("match", /^[a-zA-Z0-9-_]+\.[a-zA-Z0-9-_]+\.[a-zA-Z0-9-_]+$/);
    cy.clickOptionWithTestId("access-token-modal-close-button");
    // revoke access token, verify token removed from ui
    cy.waitTextVisible(`Token Name${test_id}`);
    cy.waitTextVisible(`Token Description${test_id}`);
    cy.clickOptionWithTestId("revoke-token-button");
    cy.waitTextVisible("Are you sure you want to revoke this token?");
    cy.clickOptionWithText("Yes");
    cy.ensureTextNotPresent(`Token Name${test_id}`);
    cy.ensureTextNotPresent(`Token Description${test_id}`);
  });
});
