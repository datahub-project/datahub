const BIGQUERY_URN =
  "urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)";
const DBT_URN =
  "urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.customers,PROD)";

describe("siblings", () => {
  it("will merge metadata to non-primary sibling", () => {
    cy.visitWithLogin(`/dataset/${BIGQUERY_URN}/?is_lineage_mode=false`);

    // check merged platforms
    cy.contains("dbt & BigQuery");

    // check merged schema (from dbt)
    cy.contains("This is a unique identifier for a customer");

    // check merged profile (from bigquery)
    cy.contains("Stats").click({ force: true });
    cy.get('[data-testid="table-stats-rowcount"]').contains("100");
  });

  it("will merge metadata to primary sibling", () => {
    cy.visitWithLogin(`/dataset/${DBT_URN}/?is_lineage_mode=false`);

    // check merged platforms
    cy.contains("dbt & BigQuery");

    // check merged schema (from dbt)
    cy.contains("This is a unique identifier for a customer");

    // check merged profile (from bigquery)
    cy.contains("Stats").click({ force: true });
    cy.get('[data-testid="table-stats-rowcount"]').contains("100");
  });

  it("can view individual nodes", () => {
    const resizeObserverLoopErrRe = /^[^(ResizeObserver loop limit exceeded)]/;
    cy.on("uncaught:exception", (err) => {
      /* returning false here prevents Cypress from failing the test */
      if (resizeObserverLoopErrRe.test(err.message)) {
        return false;
      }
    });

    cy.visitWithLogin(`/dataset/${DBT_URN}/?is_lineage_mode=false`);
    cy.get(".ant-table-row").should("be.visible");
    // navigate to the bq entity
    cy.get(`[data-testid="compact-entity-link-${BIGQUERY_URN}"`).click();
    cy.get(".ant-table-row").should("be.visible");
    // check merged platforms is not shown
    cy.get('[data-testid="entity-header-test-id"]')
      .contains("dbt & BigQuery")
      .should("not.exist");
    cy.get('[data-testid="entity-header-test-id"]').contains("BigQuery");

    // check dbt schema descriptions not shown
    cy.contains("This is a unique identifier for a customer").should(
      "not.exist",
    );

    // check merged profile still there (from bigquery)
    cy.contains("Stats").click({ force: true });
    cy.get('[data-testid="table-stats-rowcount"]').contains("100");
  });

  it("can mutate at individual node or combined node level", () => {
    cy.visitWithLogin(`/dataset/${DBT_URN}/?is_lineage_mode=false`);
    cy.get(".ant-table-row").should("be.visible");
    // navigate to the bq entity
    cy.get(`[data-testid="compact-entity-link-${BIGQUERY_URN}"`).click();
    cy.get(".ant-table-row").should("be.visible");
    cy.clickOptionWithText("Add Term");

    cy.selectOptionInTagTermModal("CypressTerm");

    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.customers,PROD)/?is_lineage_mode=false",
    );

    cy.get(
      'a[href="/glossaryTerm/urn:li:glossaryTerm:CypressNode.CypressTerm"]',
    ).within(() => cy.get("span[aria-label=close]").click());
    cy.clickOptionWithText("Yes");

    cy.contains("CypressTerm").should("not.exist");
  });

  it("will combine results in search", () => {
    cy.visitWithLogin("/search?page=1&query=%22raw_orders%22");

    cy.contains("Showing 1 - 2 of ");

    cy.get(".test-search-result").should("have.length", 1);
    cy.get(".test-search-result-sibling-section").should("have.length", 1);

    cy.get(".test-search-result-sibling-section")
      .get(".test-mini-preview-class:contains(raw_orders)")
      .should("have.length", 2);
  });

  it("will combine results in lineage", () => {
    cy.login();
    cy.visit(
      "dataset/urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.stg_orders,PROD)/?is_lineage_mode=true",
    );

    // check the subtypes
    cy.get("text:contains(Table)").should("have.length", 2);
    cy.get("text:contains(Seed)").should("have.length", 1);

    // check the names
    cy.get("text:contains(raw_orders)").should("have.length", 1);
    cy.get("text:contains(customers)").should("have.length", 1);
    // center counts twice since we secretely render two center nodes
    cy.get("text:contains(stg_orders)").should("have.length", 2);

    // check the platform
    cy.get("svg").get("text:contains(dbt & BigQuery)").should("have.length", 5);
  });

  it("can separate results in lineage if flag is set", () => {
    cy.login();
    cy.visit(
      "dataset/urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.stg_orders,PROD)/?is_lineage_mode=true",
    );

    cy.clickOptionWithTestId("compress-lineage-toggle");

    // check the subtypes
    cy.get('[data-testid="Seed"]').should("have.length", 1);
    // center counts twice since we secretely render two center nodes, plus the downstream bigquery
    cy.get('[data-testid="View"]').should("have.length", 3);
    cy.get('[data-testid="Table"]').should("have.length", 0);

    // check the names
    cy.get("text:contains(raw_orders)").should("have.length", 1);
    // center counts twice since we secretely render two center nodes, plus the downstream bigquery
    cy.get("text:contains(stg_orders)").should("have.length", 3);

    // check the platform
    cy.get("svg").get("text:contains(dbt & BigQuery)").should("have.length", 0);
    cy.get("svg").get("text:contains(dbt)").should("have.length", 3);
    cy.get("svg").get("text:contains(BigQuery)").should("have.length", 1);
  });
});
