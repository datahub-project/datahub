
describe("glossaryTerm", () => {
  it("can visit related entities", () => {
      cy.loginAndVisitRelatedEntities();
      cy.contains("of 0").should("not.exist");
      cy.contains(/of [0-9]+/);
  });

  it("can search related entities by query", () => {
      cy.loginAndVisitRelatedEntities();
      cy.get('[placeholder="Filter entities..."]').click().type("logging{enter}");
      cy.contains("of 0").should("not.exist");
      cy.contains(/of 1/);
      cy.contains("cypress_logging_events");
      cy.contains("SampleCypressHdfsDataset").should("not.exist");
  });

  it("can apply filters on related entities", () => {
      cy.loginAndVisitRelatedEntities();
      cy.applyTagFilter("urn:li:tag:Cypress2");
      cy.contains("cypress_logging_events").should("not.exist");
      cy.contains("SampleCypressHdfsDataset");
  });

  it("can search related entities by a specific tag using advanced search", () => {
      cy.loginAndVisitRelatedEntities();
      cy.applyAdvancedSearchFilter("Tag", "Cypress2");
      cy.contains("SampleCypressHdfsDataset");
      cy.contains("of 1");
  });

  it("can search related entities by AND-ing two concepts using search", () => {
    cy.loginAndVisitRelatedEntities();
    cy.wait(3000);
    // Apply tags filter using advanced search
    cy.applyAdvancedSearchFilter();

    cy.contains("Add Filter").click()
    cy.get('[data-testid="adv-search-add-filter-description"]').click({
      force: true,
    });
    cy.get('[data-testid="edit-text-input"]').type("my hdfs");
    cy.get('[data-testid="edit-text-done-btn"]').click({ force: true });
    cy.contains('SampleCypressHdfsDataset');
    cy.contains("of 1");  
  });

  it("can search related entities by OR-ing two concepts using search", () => {
    cy.loginAndVisitRelatedEntities();
      cy.applyAdvancedSearchFilter("Description", "single log event");
      cy.applyBasicSearchFilter("Tag", "Cypress2");
      cy.searchByConceptsWithLogicalOperator("Cypress", "Tag", "any filter");
      cy.contains("SampleCypressHdfsDataset");
      cy.contains("cypress_logging_events");
  });
});