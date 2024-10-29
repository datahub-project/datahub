describe("auto-complete", () => {
  it("should see auto-complete results after typing in a search query", () => {
    cy.login();
    cy.visit("/");

    // look for a dataset
    cy.get("input[data-testid=search-input]").type("SampleCypressHive");
    cy.contains("Datasets");
    cy.contains("SampleCypressHiveDataset");
    cy.focused().clear();

    // look for a dashboard
    cy.get("input[data-testid=search-input]").type("baz");
    cy.contains("Dashboards");
    cy.contains("Baz Dashboard");
    cy.focused().clear();

    // look for a dataflow
    cy.get("input[data-testid=search-input]").type("dataflow user");
    cy.contains("Pipelines");
    cy.contains("Users");
    cy.focused().clear();
  });

  it("should send you to the entity profile after clicking on an auto-complete option", () => {
    cy.login();
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("SampleCypressHiveDataset");
    cy.get('[data-testid^="auto-complete-option"]')
      .contains("SampleCypressHiveDataset")
      .click();
    cy.get(".ant-table-row").should("be.visible");
    cy.url().should(
      "include",
      "dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)",
    );
  });

  it("should filter auto-complete results when clicking on a quick filter", () => {
    cy.login();
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("baz");

    cy.get('[data-testid="auto-complete-entity-name-Baz Chart 2');
    cy.get('[data-testid="quick-filter-DASHBOARD"]').click();
    cy.wait(2000);
    cy.get('[data-testid="auto-complete-entity-name-Baz Chart 2').should(
      "not.exist",
    );
    cy.contains("Baz Dashboard");
    cy.wait(1000);
  });

  it("should filter search results when when searching with a quick filter selected", () => {
    cy.login();
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("raw_pay");
    cy.contains("raw_payments");
    cy.get('[data-testid="quick-filter-urn:li:dataPlatform:bigquery"]').click();
    cy.focused().type("{enter}");
    cy.url().should(
      "include",
      "?filter_platform___false___EQUAL___0=urn%3Ali%3AdataPlatform%3Abigquery",
    );
  });
});
