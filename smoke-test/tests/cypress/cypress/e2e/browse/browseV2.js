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
          // search and browse both need to be on for browse to show
          res.body.data.appConfig.featureFlags.showSearchFiltersV2 = isOn;
        });
      }
    });
  };

  it("should show new browse if browse v2 flag is on", () => {
    setBrowseFeatureFlag(true);
    cy.login();
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("*{enter}");

    cy.get("[data-testid=browse-v2").should("exist");
  });

  it("should not show new browse if browse v2 flag is off", () => {
    setBrowseFeatureFlag(false);
    cy.login();
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("*{enter}");

    cy.get("[data-testid=browse-v2").should("not.exist");
  });

  it("should hide and show the sidebar when the toggle button is clicked", () => {
    setBrowseFeatureFlag(true);
    cy.login();
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("*{enter}");

    cy.get("[data-testid=browse-v2")
      .invoke("css", "width")
      .should("match", /\d\d\dpx$/);

    cy.get("[data-testid=browse-v2-toggle").click();

    cy.get("[data-testid=browse-v2")
      .invoke("css", "width")
      .should("match", /\dpx$/);

    cy.reload();

    cy.get("[data-testid=browse-v2")
      .invoke("css", "width")
      .should("match", /\dpx$/);

    cy.get("[data-testid=browse-v2-toggle").click();

    cy.get("[data-testid=browse-v2")
      .invoke("css", "width")
      .should("match", /\d\d\dpx$/);

    cy.reload();

    cy.get("[data-testid=browse-v2")
      .invoke("css", "width")
      .should("match", /\d\d\dpx$/);
  });

  it("should take you to the old browse experience when clicking entity type on home page with the browse flag off", () => {
    setBrowseFeatureFlag(false);
    cy.login();
    cy.visit("/");
    cy.get('[data-testid="entity-type-browse-card-DATASET"]').click({
      force: true,
    });
    cy.url().should("include", "/browse/dataset");
    cy.url().should(
      "not.include",
      "search?filter__entityType%E2%90%9EtypeNames___false___EQUAL___0=DATASET",
    );
  });

  it("should take you to the new browse experience when clicking on browse path from entity profile page when browse flag is on", () => {
    setBrowseFeatureFlag(true);
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)",
    );
    cy.get('[data-testid="browse-path-cypress_project"]').click({
      force: true,
    });
    cy.url().should("not.include", "/browse/dataset");
    cy.url().should(
      "include",
      "filter__entityType%E2%90%9EtypeNames___false___EQUAL___0=DATASET",
    );
    cy.url().should(
      "include",
      "filter_platform___false___EQUAL___1=urn%3Ali%3AdataPlatform%3Abigquery",
    );
    cy.url().should(
      "include",
      "filter_browsePathV2___false___EQUAL___2=%E2%90%9Fcypress_project",
    );
  });

  /* Legacy Browse Path Disabled when showBrowseV2 = `true`
  it("should take you to the old browse experience when clicking on browse path from entity profile page when browse flag is off", () => {
    setBrowseFeatureFlag(false);
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)"
    );
    cy.get('[data-testid="legacy-browse-path-cypress_project"]').click({
      force: true,
    });
    cy.url().should("include", "/browse/dataset/prod/bigquery/cypress_project");
  });
*/

  it("should take you to the unified search and browse experience when clicking entity type on home page with the browse flag on", () => {
    setBrowseFeatureFlag(true);
    cy.login();
    cy.visit("/");
    cy.get('[data-testid="entity-type-browse-card-DATASET"]').click({
      force: true,
    });
    cy.url().should("not.include", "/browse/dataset");
    cy.url().should(
      "include",
      "search?filter__entityType%E2%90%9EtypeNames___false___EQUAL___0=DATASET",
    );
    cy.get("[data-testid=browse-platform-BigQuery]");
  });

  it("should be able to walk through a browse path, select it, and close the browse path", () => {
    setBrowseFeatureFlag(true);
    cy.login();
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("*{enter}");

    // walk through a full browse path and select it
    cy.get("[data-testid=browse-entity-Datasets]").click({ force: true });
    cy.get("[data-testid=browse-platform-BigQuery]").click({ force: true });
    cy.get("[data-testid=browse-node-expand-cypress_project]").click({
      force: true,
    });
    cy.get("[data-testid=browse-node-jaffle_shop]").click({ force: true });

    cy.url().should(
      "include",
      "filter__entityType%E2%90%9EtypeNames___false___EQUAL___0=DATASET",
    );
    cy.url().should(
      "include",
      "filter_platform___false___EQUAL___1=urn%3Ali%3AdataPlatform%3Abigquery",
    );
    cy.url().should(
      "include",
      "filter_browsePathV2___false___EQUAL___2=%E2%90%9Fcypress_project%E2%90%9Fjaffle_shop",
    );

    // close each of the levels, ensuring its children aren't visible anymore
    cy.get("[data-testid=browse-node-expand-cypress_project]").click({
      force: true,
    });
    cy.get("[data-testid=browse-node-jaffle_shop]").should("not.be.visible");

    cy.get("[data-testid=browse-platform-BigQuery]").click({ force: true });
    cy.get("[data-testid=browse-node-expand-cypress_project]").should(
      "not.be.visible",
    );

    cy.get("[data-testid=browse-entity-Datasets]").click({ force: true });
    cy.get("[data-testid=browse-platform-BigQuery]").should("not.be.visible");
  });

  it("should be able to select and then deselect a browse path", () => {
    setBrowseFeatureFlag(true);
    cy.login();
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("*{enter}");

    cy.get("[data-testid=browse-entity-Datasets]").click({ force: true });
    cy.get("[data-testid=browse-platform-BigQuery]").click({ force: true });
    cy.get("[data-testid=browse-node-expand-cypress_project]").click({
      force: true,
    });
    cy.get("[data-testid=browse-node-jaffle_shop]").click({ force: true });

    cy.url().should(
      "include",
      "filter__entityType%E2%90%9EtypeNames___false___EQUAL___0=DATASET",
    );
    cy.url().should(
      "include",
      "filter_platform___false___EQUAL___1=urn%3Ali%3AdataPlatform%3Abigquery",
    );
    cy.url().should(
      "include",
      "filter_browsePathV2___false___EQUAL___2=%E2%90%9Fcypress_project%E2%90%9Fjaffle_shop",
    );

    cy.get("[data-testid=browse-node-jaffle_shop]").click({ force: true });

    cy.url().should(
      "not.include",
      "filter__entityType%E2%90%9EtypeNames___false___EQUAL___0=DATASET",
    );
    cy.url().should(
      "not.include",
      "filter_platform___false___EQUAL___1=urn%3Ali%3AdataPlatform%3Abigquery",
    );
    cy.url().should(
      "not.include",
      "filter_browsePathV2___false___EQUAL___2=%E2%90%9Fcypress_project%E2%90%9Fjaffle_shop",
    );
  });
});
