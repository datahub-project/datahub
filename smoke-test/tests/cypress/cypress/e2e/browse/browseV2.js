import { aliasQuery, hasOperationName } from "../utils";

describe("search", () => {
  beforeEach(() => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  const setBrowseFeatureFlag = (isOn) => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.reply((res) => {
          // Modify the response body directly
          res.body.data.appConfig.featureFlags.showBrowseV2 = isOn;
        });
      }
    });
  };

  it("should show new browse if browse v2 flag is on", () => {
    setBrowseFeatureFlag(true);
    cy.login();
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("*{enter}");
    cy.wait(2000);

    cy.get("[data-testid=browse-v2").should("exist");
  });

  it("should not show new browse if browse v2 flag is off", () => {
    setBrowseFeatureFlag(false);
    cy.login();
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("*{enter}");
    cy.wait(2000);

    cy.get("[data-testid=browse-v2").should("not.exist");
  });

  it("should be able to walk through a browse path, select it, and close the browse path", () => {
    setBrowseFeatureFlag(true);
    cy.login();
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("*{enter}");
    cy.wait(2000);

    // walk through a full browse path and select it
    cy.get("[data-testid=browse-entity-Datasets]").click({ force: true });
    cy.get("[data-testid=browse-platform-BigQuery]").click({ force: true });
    cy.get("[data-testid=browse-node-expand-cypress_project]").click({
      force: true,
    });
    cy.get("[data-testid=browse-node-jaffle_shop]").click({ force: true });

    // ensure expected dataset is there with expected filters applied
    cy.contains("customers");
    cy.url().should(
      "include",
      "filter__entityType%E2%90%9EtypeNames___false___EQUAL___0=DATASET"
    );
    cy.url().should(
      "include",
      "filter_platform___false___EQUAL___1=urn%3Ali%3AdataPlatform%3Abigquery"
    );
    cy.url().should(
      "include",
      "filter_browsePathV2___false___EQUAL___2=%E2%90%9Furn%3Ali%3Acontainer%3Ab5e95fce839e7d78151ed7e0a7420d84%E2%90%9Furn%3Ali%3Acontainer%3A348c96555971d3f5c1ffd7dd2e7446cb"
    );

    // close each of the levels, ensuring its children aren't visible anymore
    cy.get("[data-testid=browse-node-expand-cypress_project]").click({
      force: true,
    });
    cy.get("[data-testid=browse-node-jaffle_shop]").should("not.be.visible");

    cy.get("[data-testid=browse-platform-BigQuery]").click({ force: true });
    cy.get("[data-testid=browse-node-expand-cypress_project]").should(
      "not.be.visible"
    );

    cy.get("[data-testid=browse-entity-Datasets]").click({ force: true });
    cy.get("[data-testid=browse-platform-BigQuery]").should("not.be.visible");
  });
});
