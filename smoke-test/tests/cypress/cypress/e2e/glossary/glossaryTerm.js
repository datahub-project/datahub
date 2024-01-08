const glossaryTerms = {
  glossaryTermUrl:"/glossaryTerm/urn:li:glossaryTerm:CypressNode.CypressColumnInfoType/Related%20Entities",
  SampleCypressHdfsDataset:"SampleCypressHdfsDataset"
};

const applyTagFilter = (tag) => {
  cy.get('[data-icon="filter"]').click()
  cy.waitTextVisible("Filter");
  cy.get(`[data-testid="facet-tags-${tag}"]`).click({ force: true });
};

const applyAdvancedSearchFilter = (filterType, value) => {
  cy.get('[aria-label="filter"]').click();
  cy.get('[id="search-results-advanced-search"]').click();
  cy.clickOptionWithText('Add Filter');

  if (filterType === "Tag") {
    applyTagFilterInSearch(value);
  } else if (filterType === "Description") {
    applyDescriptionFilterInAdvancedSearch(value);
  }
};

const applyBasicSearchFilter = () => {
  cy.waitTextVisible("Basic");
  cy.clickOptionWithText('Add Filter');
};

const searchByConceptsWithLogicalOperator = (concept1, concept2, operator) => {
  cy.waitTextVisible("Filters");
  applyBasicSearchFilter();
  applyTagFilterInSearch(concept1);
  cy.clickOptionWithText('Add Filter');
  applyDescriptionFilterInAdvancedSearch(concept2);
  cy.get('[title="all filters"]').click();
  cy.clickOptionWithText(operator)
};

// Helper function to apply tag filter in basic search
const applyTagFilterInSearch = (tag) => {
  cy.contains(/^Tag$/).click({ force: true });
  cy.selectOptionInTagTermModal(tag);
};

// Helper function to apply description filter in advanced search
const applyDescriptionFilterInAdvancedSearch = (value) => {
  cy.get('[data-testid="adv-search-add-filter-description"]').click({ force: true });
  cy.get('[data-testid="edit-text-input"]').type(value);
  cy.get('[data-testid="edit-text-done-btn"]').click({ force: true });
};

describe("glossaryTerm", () => {
  beforeEach(() => {
    cy.loginWithCredentials();
    cy.visit(glossaryTerms.glossaryTermUrl);
  });

  it("can visit related entities", () => {
    cy.contains("of 0").should("not.exist");
    cy.waitTextVisible(/of [0-9]+/);
  });

  it("can search related entities by query", () => {
    cy.get('[placeholder="Filter entities..."]').click().type("logging{enter}");
    cy.contains("of 0").should("not.exist");
    cy.waitTextVisible(/of 1/);
    cy.waitTextVisible("cypress_logging_events");
    cy.contains(glossaryTerms.SampleCypressHdfsDataset).should("not.exist");
  });

  it("can apply filters on related entities", () => {
    applyTagFilter("urn:li:tag:Cypress2");
    cy.contains("cypress_logging_events").should("not.exist");
    cy.waitTextVisible(glossaryTerms.SampleCypressHdfsDataset);
  });

  it("can search related entities by a specific tag using advanced search", () => {
    applyAdvancedSearchFilter("Tag", "Cypress2");
    cy.waitTextVisible(glossaryTerms.SampleCypressHdfsDataset);
    cy.waitTextVisible("of 1");
  });

  it("can search related entities by AND-ing two concepts using search", () => {
    applyAdvancedSearchFilter();
    cy.clickOptionWithText('Add Filter');
    cy.get('[data-testid="adv-search-add-filter-description"]').click({
      force: true,
    });
    cy.get('[data-testid="edit-text-input"]').type("my hdfs");
    cy.get('[data-testid="edit-text-done-btn"]').click({ force: true });
    cy.waitTextVisible(glossaryTerms.SampleCypressHdfsDataset);
    cy.waitTextVisible("of 1");
  });

  it("can search related entities by OR-ing two concepts using search", () => {
    applyAdvancedSearchFilter("Description", "single log event");
    applyBasicSearchFilter("Tag", "Cypress2");
    searchByConceptsWithLogicalOperator("Cypress", "Tag", "any filter");
    cy.waitTextVisible(glossaryTerms.SampleCypressHdfsDataset);
    cy.waitTextVisible("cypress_logging_events");
  });
});