import { aliasQuery, hasOperationName } from "../utils";

describe("home", () => {
  let businessAttributeEntityEnabled;

  beforeEach(() => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  const setBusinessAttributeFeatureFlag = () => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.reply((res) => {
          businessAttributeEntityEnabled =
            res.body.data.appConfig.featureFlags.businessAttributeEntityEnabled;
          return res;
        });
      }
    }).as("apiCall");
  };
  it("home page shows ", () => {
    setBusinessAttributeFeatureFlag();
    cy.login();
    cy.visit("/");
    // cy.get('img[src="/assets/platforms/datahublogo.png"]').should('exist');
    cy.get('[data-testid="entity-type-browse-card-DATASET"]').should("exist");
    cy.get('[data-testid="entity-type-browse-card-DASHBOARD"]').should("exist");
    cy.get('[data-testid="entity-type-browse-card-CHART"]').should("exist");
    cy.get('[data-testid="entity-type-browse-card-DATA_FLOW"]').should("exist");
    cy.get('[data-testid="entity-type-browse-card-GLOSSARY_TERM"]').should(
      "exist",
    );
    cy.wait("@apiCall").then(() => {
      if (!businessAttributeEntityEnabled) {
        return;
      }
      cy.get(
        '[data-testid="entity-type-browse-card-BUSINESS_ATTRIBUTE"]',
      ).should("exist");
    });
  });
});
