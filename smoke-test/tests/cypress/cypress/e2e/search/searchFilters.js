import { aliasQuery, hasOperationName } from "../utils";

describe("search", () => {
  beforeEach(() => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  const setSearchFiltersFeatureFlag = (isOn) => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.reply((res) => {
          // Modify the response body directly
          res.body.data.appConfig.featureFlags.showSearchFiltersV2 = isOn;
        });
      }
    });
  };

  it("should show old search filters if search filters v2 flag is off", () => {
    setSearchFiltersFeatureFlag(false);
    cy.login();
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("*{enter}");

    cy.get("[data-testid=search-filters-v1").should("exist");
    cy.get("[data-testid=search-filters-v2").should("not.exist");
  });

  it("should show new search filters if search filters v2 flag is on", () => {
    setSearchFiltersFeatureFlag(true);
    cy.login();
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("*{enter}");

    cy.get("[data-testid=search-filters-v1").should("not.exist");
    cy.get("[data-testid=search-filters-v2").should("exist");
  });

  it("should add and remove multiple filters with no issues", () => {
    setSearchFiltersFeatureFlag(true);
    cy.login();
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("*{enter}");

    // click tag filter dropdown inside of "More Filters"
    cy.get("[data-testid=more-filters-dropdown").click({ force: true });
    cy.get("[data-testid=more-filter-Tag").click({ force: true });

    // click and search for tag, save that tag
    cy.get("[data-testid=search-bar").eq(1).type("cypress");
    cy.get("[data-testid=filter-option-Cypress").click({ force: true });
    cy.get("[data-testid=update-filters").click({ force: true });
    cy.url().should(
      "include",
      "filter_tags___false___EQUAL___0=urn%3Ali%3Atag%3ACypress",
    );
    cy.get("[data-testid=update-filters").should("not.be.visible");

    // select datasets filter
    cy.get("[data-testid=filter-dropdown-Type").click({ force: true });
    cy.get("[data-testid=filter-option-Datasets").click({ force: true });
    cy.get("[data-testid=update-filters").eq(1).click({ force: true });
    cy.url().should(
      "include",
      "filter__entityType%E2%90%9EtypeNames___false___EQUAL___1=DATASET",
    );

    // ensure expected entity is in search results
    cy.contains("SampleCypressHdfsDataset");

    // ensure active filters are visible under filter dropdowns
    cy.get("[data-testid=active-filter-Datasets");
    cy.get("[data-testid=active-filter-Cypress");

    // remove datasets filter by clicking "x" on active filter
    cy.get("[data-testid=remove-filter-Datasets").click({ force: true });
    cy.url().should(
      "not.include",
      "filter__entityType%E2%90%9EtypeNames___false___EQUAL___1=DATASET",
    );
    cy.get("[data-testid=active-filter-Datasets").should("not.exist");

    // remove other filter using "clear all" button
    cy.get("[data-testid=clear-all-filters").click({ force: true });
    cy.url().should(
      "not.include",
      "filter_tags___false___EQUAL___0=urn%3Ali%3Atag%3ACypress",
    );
    cy.get("[data-testid=active-filter-Cypress").should("not.exist");
  });
});
