function clearMonacoEditor() {
  const selectAllKey = Cypress.platform === "darwin" ? "{cmd}a" : "{ctrl}a";
  return cy
    .get(".monaco-scrollable-element")
    .first()
    .click()
    .focused()
    .type(selectAllKey)
    .type("{backspace}");
}

function typeInMonacoEditor(text) {
  return cy
    .get(".monaco-scrollable-element")
    .first()
    .click()
    .focused()
    .type(text);
}

describe("run managed ingestion", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });

  it("cannot access ingestion page when disabled", () => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.alias = "gqlappConfigQuery";
        req.on("response", (res) => {
          res.body.data.appConfig.managedIngestionConfig.enabled = false;
        });
      }
    });
    cy.loginWithCredentials();
    cy.visit("/ingestion");
    cy.waitTextVisible("404");
  });

  it("cannot access ingestion page without permissions", () => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.alias = "gqlappConfigQuery";
        req.on("response", (res) => {
          res.body.data.appConfig.managedIngestionConfig.enabled = true;
        });
      } else if (hasOperationName(req, "getMe")) {
        req.alias = "gqlgetMeQuery";
        req.on("response", (res) => {
          res.body.data.me.platformPrivileges.manageIngestion = false;
          res.body.data.me.platformPrivileges.manageSecrets = false;
        });
      }
    });
    cy.loginWithCredentials();
    cy.visit("/ingestion");
    cy.waitTextVisible("404");
  });

  it("can access ingestion but not secrets", () => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.alias = "gqlappConfigQuery";
        req.on("response", (res) => {
          res.body.data.appConfig.managedIngestionConfig.enabled = true;
        });
      } else if (hasOperationName(req, "getMe")) {
        req.alias = "gqlgetMeQuery";
        req.on("response", (res) => {
          res.body.data.me.platformPrivileges.manageIngestion = true;
          res.body.data.me.platformPrivileges.manageSecrets = false;
        });
      }
    });
    cy.loginWithCredentials();
    cy.visit("/ingestion");
    cy.waitTextVisible("Manage Data Sources");
    cy.get('div[role="tab"]').contains("Sources").should("exist");
    cy.get('div[role="tab"]').contains("Secrets").should("not.exist");
  });

  it("can access secrets but not ingestion", () => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.alias = "gqlappConfigQuery";
        req.on("response", (res) => {
          res.body.data.appConfig.managedIngestionConfig.enabled = true;
        });
      } else if (hasOperationName(req, "getMe")) {
        req.alias = "gqlgetMeQuery";
        req.on("response", (res) => {
          res.body.data.me.platformPrivileges.manageIngestion = false;
          res.body.data.me.platformPrivileges.manageSecrets = true;
        });
      }
    });
    cy.loginWithCredentials();
    cy.visit("/ingestion");
    cy.waitTextVisible("Manage Data Sources");
    cy.get('div[role="tab"]').contains("Secrets").should("exist");
    cy.get('div[role="tab"]').contains("Sources").should("not.exist");
  });

  it.skip("create run managed ingestion source", () => {
    const number = Math.floor(Math.random() * 100000);
    const testName = `cypress test source ${number}`;
    const cli_version = "0.15.0.5";
    cy.login();
    cy.goToIngestionPage();
    cy.contains("Loading ingestion sources...").should("not.exist");
    cy.clickOptionWithTestId("create-ingestion-source-button");
    cy.get('[placeholder="Search data sources..."]').type("other");
    cy.clickOptionWithTextToScrollintoView("Other");

    cy.waitTextVisible("source-type");

    // Clear the editor first
    clearMonacoEditor();

    // Type your content
    typeInMonacoEditor("source:{enter}");
    typeInMonacoEditor("    type: demo-data{enter}");
    typeInMonacoEditor("config: {}");

    cy.clickOptionWithText("Next");
    cy.clickOptionWithText("Next");

    cy.enterTextInTestId("source-name-input", testName);
    cy.clickOptionWithText("Advanced");
    cy.enterTextInTestId("cli-version-input", cli_version);
    cy.clickOptionWithTextToScrollintoView("Save & Run");
    cy.waitTextVisible(testName);

    cy.contains(testName)
      .parent()
      .within(() => {
        cy.contains("Succeeded", { timeout: 180000 });
        cy.clickOptionWithTestId(`delete-ingestion-source-${testName}`);
      });
    cy.get(`[data-testid="confirm-delete-ingestion-source"]`).click();
    cy.ensureTextNotPresent(testName);
  });
});
