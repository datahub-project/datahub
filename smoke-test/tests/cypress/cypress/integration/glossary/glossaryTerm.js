describe("glossaryTerm", () => {
  it("can visit related entities", () => {
    cy.login();
    cy.visit("/glossaryTerm/urn:li:glossaryTerm:CypressNode.CypressColumnInfoType/Related%20Entities");
    cy.wait(5000);
    cy.contains("of 0").should("not.exist");
    cy.contains(/of [0-9]+/);
  });

  it("can search related entities by query", () => {
    cy.login();
    cy.visit("/glossaryTerm/urn:li:glossaryTerm:CypressNode.CypressColumnInfoType/Related%20Entities");
    cy.get('[placeholder="Filter entities..."]').click().type(
      "logging{enter}"
    );
    cy.wait(5000);
    cy.contains("of 0").should("not.exist");
    cy.contains(/of 1/);
    cy.contains("cypress_logging_events");
    cy.contains("SampleCypressHdfsDataset").should("not.exist");
  });

  it("can apply filters on related entities", () => {
    cy.login();
    cy.visit("/glossaryTerm/urn:li:glossaryTerm:CypressNode.CypressColumnInfoType/Related%20Entities");
    cy.wait(5000);
    // Select tags filter
    cy.contains("Filters").click();

    cy.get('[data-testid="facet-tags-urn:li:tag:Cypress2"]').click({force: true})
    cy.wait(3000);

    // Unselect tags filter
    cy.contains("cypress_logging_events").should("not.exist");
    cy.contains("SampleCypressHdfsDataset");
    cy.get('[data-testid="facet-tags-urn:li:tag:Cypress2"]').click({force: true})

    cy.wait(3000);
    cy.contains("cypress_logging_events");
    cy.contains("SampleCypressHdfsDataset");
  });

  it("can search related entities by a specific tag using advanced search", () => {
    cy.login();

    cy.visit("/glossaryTerm/urn:li:glossaryTerm:CypressNode.CypressColumnInfoType/Related%20Entities");
    cy.wait(3000);
    cy.contains("Filters").click();
    cy.contains("Advanced").click();
    cy.contains("Add Filter").click();
    cy.contains(/^Tag$/).click({ force: true });

    cy.get('[data-testid="tag-term-modal-input"]').type("Cypress2");
    cy.wait(2000);
    cy.get('[data-testid="tag-term-option"]').click({ force: true });
    cy.get('[data-testid="add-tag-term-from-modal-btn"]').click({
      force: true,
    });

    cy.wait(2000);
    cy.contains("SampleCypressHdfsDataset");
    // Only 1 result.
    cy.contains("of 1");
  });

  it("can search related entities by AND-ing two concepts using advanced search", () => {
    cy.login();

    cy.visit("/glossaryTerm/urn:li:glossaryTerm:CypressNode.CypressColumnInfoType/Related%20Entities");
    cy.wait(3000);
    cy.contains("Filters").click();
    cy.contains("Advanced").click();
    cy.contains("Add Filter").click();
    cy.contains(/^Tag$/).click({ force: true });
    cy.get('[data-testid="tag-term-modal-input"]').type("Cypress2");
    cy.wait(2000);
    cy.get('[data-testid="tag-term-option"]').click({ force: true });
    cy.get('[data-testid="add-tag-term-from-modal-btn"]').click({
      force: true,
    });
    cy.wait(2000);
    cy.contains("Add Filter").click();
    cy.get('[data-testid="adv-search-add-filter-description"]').click({
      force: true,
    });
    cy.get('[data-testid="edit-text-input"]').type("my hdfs");
    cy.get('[data-testid="edit-text-done-btn"]').click({ force: true });
    cy.contains("SampleCypressHdfsDataset");
    cy.contains("of 1");
  });

  it("can search related entities by OR-ing two concepts using advanced search", () => {
    cy.login();

    cy.visit("/glossaryTerm/urn:li:glossaryTerm:CypressNode.CypressColumnInfoType/Related%20Entities");
    cy.wait(3000);
    cy.contains("Filters").click();
    cy.contains("Advanced").click();
    cy.contains("Add Filter").click();
    cy.get('[data-testid="adv-search-add-filter-description"]').click({
      force: true,
    });
    cy.get('[data-testid="edit-text-input"]').type("single log event");
    cy.get('[data-testid="edit-text-done-btn"]').click({ force: true });
    cy.wait(2000);
    cy.contains("Add Filter").click();
    cy.contains(/^Tag$/).click({ force: true });
    cy.get('[data-testid="tag-term-modal-input"]').type("Cypress2");
    cy.wait(2000);
    cy.get('[data-testid="tag-term-option"]').click({ force: true });
    cy.get('[data-testid="add-tag-term-from-modal-btn"]').click({
      force: true,
    });
    cy.wait(2000);

    cy.contains("all filters").click();
    cy.contains("any filter").click({ force: true });

    cy.contains("SampleCypressHdfsDataset");
    cy.contains("cypress_logging_events");
  });
});
