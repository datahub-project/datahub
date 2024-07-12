describe("siblings", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });

  it("will merge metadata to non-primary sibling", () => {
    cy.login();
    cy.visit("/");
    cy.handleIntroducePage();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)/?is_lineage_mode=false",
    );
    // check merged platforms
    cy.get('[src*="dbtlogo"]').should("exist");
    cy.get('[src*="bigquerylogo"]').should("exist");

    // check merged schema (from dbt)
    cy.contains("This is a unique identifier for a customer");

    // check merged profile (from bigquery)
    cy.contains("Stats").click({ force: true });
    cy.get('[data-testid="table-stats-rowcount"]').contains("100");
  });

  it("will merge metadata to primary sibling", () => {
    cy.login();
    cy.visit("/");
    cy.handleIntroducePage();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.customers,PROD)/?is_lineage_mode=false",
    );

    // check merged platforms
    cy.get('[src*="dbtlogo"]').should("exist");
    cy.get('[src*="bigquerylogo"]').should("exist");

    // check merged schema (from dbt)
    cy.contains("This is a unique identifier for a customer");

    // check merged profile (from bigquery)
    cy.contains("Stats").click({ force: true });
    cy.get('[data-testid="table-stats-rowcount"]').contains("100");
  });

  it("can view individual nodes", () => {
    cy.login();
    cy.visit("/");
    cy.handleIntroducePage();
    const resizeObserverLoopErrRe = /^[^(ResizeObserver loop limit exceeded)]/;
    cy.on("uncaught:exception", (err) => {
      /* returning false here prevents Cypress from failing the test */
      if (resizeObserverLoopErrRe.test(err.message)) {
        return false;
      }
    });

    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.customers,PROD)/?is_lineage_mode=false",
    );

    // navigate to the bq entity
    cy.clickOptionWithTestId(
      "compact-entity-link-urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)",
    );

    // check merged platforms is not shown
    cy.get('[data-testid="entity-header-test-id"]')
      .contains("dbt & BigQuery")
      .should("not.exist");
    cy.get('[src*="bigquerylogo"]').should("exist");
    cy.get('[src*="bigquerylogo"]').should("exist");
    // check dbt schema descriptions not shown
    cy.contains("This is a unique identifier for a customer").should(
      "not.exist",
    );

    // check merged profile still there (from bigquery)
    cy.contains("Stats").click({ force: true });
    cy.get('[data-testid="table-stats-rowcount"]').contains("100");
  });

  it("can mutate at individual node or combined node level", () => {
    cy.login();
    cy.visit("/");
    cy.handleIntroducePage();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.customers,PROD)/?is_lineage_mode=false",
    );

    // navigate to the bq entity
    cy.clickOptionWithTestId(
      "compact-entity-link-urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)",
    );
    cy.contains(".ant-collapse-header-text", "Terms")
      .parent()
      .find('[data-testid="AddRoundedIcon"]')
      .scrollIntoView()
      .click();
    cy.selectOptionInTagTermModal("CypressTerm");
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.customers,PROD)/?is_lineage_mode=false",
    );
    cy.get("#column-first_name").should("be.visible");
  });

  it("will combine results in search", () => {
    cy.login();
    cy.visit("/");
    cy.handleIntroducePage();
    cy.visit("/search?page=1&query=raw_orders");
    cy.contains("Showing 1 - 2 of ");

    cy.get(".test-search-result").should("have.length", 1);
    cy.get('[data-testid="browse-platform-BigQuery"]').should("exist");
    cy.get('[data-testid="browse-platform-dbt"]').should("exist");
    cy.get('[data-testid^="browse-platform').should("have.length", 2);
  });

  it("will combine results in lineage", () => {
    cy.login();
    cy.visit("/");
    cy.handleIntroducePage();
    cy.visit(
      "dataset/urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.stg_orders,PROD)/?is_lineage_mode=true",
    );
    cy.wait(2000);
    cy.get('[data-testid="KeyboardArrowRightIcon"]').click();
    cy.get(".react-flow__node-lineage-entity").eq(1).click();
    // check the subtypes
    cy.get('div[title="Table"]').should("be.visible");
    cy.get("body").click();
    cy.get(".react-flow__node-lineage-entity").eq(2).click();
    cy.get('div[title="Table"]').should("be.visible");
    cy.get("body").click();

    // check the names
    cy.get(".react-flow__node-lineage-entity")
      .eq(0)
      .contains("stg_orders")
      .should("exist");
    cy.get(".react-flow__node-lineage-entity")
      .eq(1)
      .contains("customer")
      .should("exist");
    cy.get(".react-flow__node-lineage-entity")
      .eq(2)
      .contains("orders")
      .should("exist");

    // check the platform
    cy.get('[data-testid*="dataPlatform:dbt"').should("exist");
    cy.get('[data-testid*="dataPlatform:bigquery"').should("exist");
  });

  it("can separate results in lineage if flag is set", () => {
    cy.login();
    cy.visit("/");
    cy.handleIntroducePage();
    cy.visit(
      "dataset/urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.stg_orders,PROD)/?is_lineage_mode=true",
    );
    cy.wait(2000);
    cy.get(".react-flow__node-lineage-entity").eq(0).click();

    // Verify downstream bigquery
    cy.get("#entity-sidebar-tabs-tab-Lineage").click();
    cy.clickOptionWithTestId(
      "compact-lineage-tab-direction-select-option-downstream",
    );
    cy.get(".ant-spin-container .ant-list-items")
      .contains("customers")
      .should("exist");
    cy.get(".ant-spin-container .ant-list-items")
      .contains("orders")
      .should("exist");
    cy.contains("1 - 2 of 2");

    // Verify Upstream bigquery
    cy.clickOptionWithTestId(
      "compact-lineage-tab-direction-select-option-upstream",
    );
    cy.get(".ant-spin-container .ant-list-items")
      .contains("stg_orders")
      .should("exist");
    cy.contains("1 - 1 of 1");
  });
});
