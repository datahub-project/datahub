import { aliasQuery, hasOperationName } from "../utils";

const test_domain_id = Math.floor(Math.random() * 100000);
const test_domain = `CypressDomainTest ${test_domain_id}`;
const test_domain_urn = `urn:li:domain:${test_domain_id}`;

describe("add remove domain", () => {
  beforeEach(() => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  const setDomainsFeatureFlag = (isOn) => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.reply((res) => {
          res.body.data.appConfig.featureFlags.nestedDomainsEnabled = isOn;
        });
      }
    });
  };

  it("create domain", () => {
    cy.loginWithCredentials();
    cy.goToDomainList();
    cy.clickOptionWithText("New Domain");
    cy.waitTextVisible("Create New Domain");
    cy.get('[data-testid="create-domain-name"]').click().type(test_domain);
    cy.clickOptionWithText("Advanced");
    cy.get('[data-testid="create-domain-id"]').click().type(test_domain_id);
    cy.get('[data-testid="create-domain-button"]').click();
    cy.waitTextVisible(test_domain);
  });

  it("add entities to domain", () => {
    setDomainsFeatureFlag(false);
    cy.loginWithCredentials();
    cy.goToDomainList();
    cy.clickOptionWithText(test_domain);
    cy.waitTextVisible("Add assets");
    cy.clickOptionWithText("Add assets");
    cy.get(".ant-modal-content").within(() => {
      cy.get('[data-testid="search-input"]')
        .click()
        .invoke("val", "cypress_project.jaffle_shop.")
        .type("customer");
      cy.contains("BigQuery", { timeout: 30000 });
      cy.get(".ant-checkbox-input").first().click();
      cy.get("#continueButton").click();
    });
    cy.waitTextVisible("Added assets to Domain!");
  });

  it("remove entity from domain", () => {
    setDomainsFeatureFlag(false);
    cy.loginWithCredentials();
    cy.goToDomainList();
    cy.removeDomainFromDataset(
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)",
      "customers",
      test_domain_urn,
    );
  });

  it("delete a domain and ensure dangling reference is deleted on entities", () => {
    setDomainsFeatureFlag(false);
    cy.loginWithCredentials();
    cy.goToDomainList();
    cy.get(`[data-testid="dropdown-menu-${test_domain_urn}"]`).click();
    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    cy.ensureTextNotPresent(test_domain);
    cy.goToContainer("urn:li:container:348c96555971d3f5c1ffd7dd2e7446cb");
    cy.waitTextVisible("customers");
    cy.ensureTextNotPresent(test_domain);
  });
});
