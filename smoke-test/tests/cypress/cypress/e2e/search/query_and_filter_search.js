describe("auto-complete dropdown, filter plus query search test", () => {

  const platformQuerySearch = (query,test_id,active_filter) => {
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type(query);
    cy.get(`[data-testid="quick-filter-urn:li:dataPlatform:${test_id}"]`).click();
    cy.focused().type("{enter}").wait(3000);
    cy.url().should(
      "include",
      `?filter_platform___false___EQUAL___0=urn%3Ali%3AdataPlatform%3A${test_id}`
    );
    cy.get('[data-testid="search-input"]').should("have.value", query);
    cy.get(`[data-testid="active-filter-${active_filter}"]`).should("be.visible");
    cy.contains("of 0 results").should("not.exist");
    cy.contains(/of [0-9]+ results/);
  }

  const entityQuerySearch = (query,test_id,active_filter) => {
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type(query);
    cy.get(`[data-testid="quick-filter-${test_id}"]`).click();
    cy.focused().type("{enter}").wait(3000);
    cy.url().should(
      "include",
      `?filter__entityType___false___EQUAL___0=${test_id}`
    );
    cy.get('[data-testid="search-input"]').should("have.value", query);
    cy.get(`[data-testid="active-filter-${active_filter}"]`).should("be.visible");
    cy.contains("of 0 results").should("not.exist");
    cy.contains(/of [0-9]+ results/);
  }

  it("verify the 'filter by' section + query (result in search page with query applied + filter applied)", () => {
    // Platform query plus filter test
    cy.loginWithCredentials();
    // Airflow
    platformQuerySearch ("cypress","airflow","Airflow");
    // BigQuery
    platformQuerySearch ("cypress","bigquery","BigQuery");
    // dbt
    platformQuerySearch ("cypress","dbt","dbt");
    // Hive 
    platformQuerySearch ("cypress","hive","Hive");

    // Entity type query plus filter test
    // Datasets
    entityQuerySearch ("cypress","DATASET","Datasets");
    // Dashboards
    entityQuerySearch ("cypress","DASHBOARD","Dashboards");
    // Pipelines
    entityQuerySearch ("cypress","DATA_FLOW","Pipelines");
    // Domains
    entityQuerySearch ("Marketing","DOMAIN","Domains");
    // Glossary Terms
    entityQuerySearch ("cypress","GLOSSARY_TERM","Glossary Terms");
  });
});