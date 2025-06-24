import { aliasQuery, hasOperationName } from "../utils";

describe("applications", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    Cypress.on("uncaught:exception", (err, runnable) => false);
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  const setApplicationFeatureFlag = (isOn) => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.alias = "gqlappConfigQuery";
        req.on("response", (res) => {
          res.body.data.appConfig.visualConfig.application.showSidebarSectionWhenEmpty =
            isOn;
        });
      }
    });
  };

  it("can see elements inside the application", () => {
    cy.login();
    cy.goToApplication(
      "urn:li:application:d63587c6-cacc-4590-851c-4f51ca429b51/Assets",
    );
    cy.contains("cypress_logging_events");
    cy.contains("1 - 1 of 1");
  });

  it("can see application sidebar section always when filled", () => {
    cy.login();
    setApplicationFeatureFlag(false);
    cy.goToDataset(
      "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)",
      "cypress_logging_events",
    );

    cy.contains("Cypress Accounts Application");
  });

  it("can see application sidebar section when empty if feature flag is on", () => {
    cy.login();
    setApplicationFeatureFlag(true);
    cy.goToDataset(
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)",
      "customers",
    );

    cy.contains("No application yet");
  });

  it("cannot see application sidebar section when empty if feature flag is off", () => {
    cy.login();
    setApplicationFeatureFlag(false);
    cy.goToDataset(
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)",
      "customers",
    );

    cy.ensureTextNotPresent("No application yet");
  });

  it("can add and remove application to dataset", () => {
    cy.login();
    setApplicationFeatureFlag(true);
    cy.goToDataset(
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)",
      "customers",
    );
    cy.clickOptionWithTestId("add-applications-button");
    cy.contains("Select an application").click({ force: true });
    cy.focused().type("Cypress Accounts Application");
    cy.get(".ant-select-item").contains("Cypress Accounts Application").click();
    cy.clickOptionWithText("OK");
    cy.waitTextVisible("Application set");
    cy.contains("Cypress Accounts Application");

    cy.removeApplicationFromDataset(
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)",
      "customers",
      "urn:li:application:d63587c6-cacc-4590-851c-4f51ca429b51/Assets",
    );

    cy.ensureTextNotPresent("Cypress Accounts Application");
  });
});
