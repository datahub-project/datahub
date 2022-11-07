describe("search", () => {
  it("can hit all entities search, see some results (testing this any more is tricky because it is cached for now)", () => {
    cy.login();
    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("*{enter}");
    cy.wait(5000);
    cy.contains("of 0 results").should("not.exist");
    cy.contains(/of [0-9]+ results/);
  });

  it("can hit all entities search with an impossible query and find 0 results", () => {
    cy.login();
    cy.visit("/");
    // random string that is unlikely to accidentally have a match
    cy.get("input[data-testid=search-input]").type(
      "zzzzzzzzzzzzzqqqqqqqqqqqqqzzzzzzqzqzqzqzq{enter}"
    );
    cy.wait(5000);
    cy.contains("of 0 results");
  });

  it("can search, find a result, and visit the dataset page", () => {
    cy.login();
    cy.visit(
      "/search?filter_entity=DATASET&filter_tags=urn%3Ali%3Atag%3ACypress&page=1&query=users_created"
    );
    cy.contains("of 1 result");

    cy.contains("Cypress");

    cy.contains("fct_cypress_users_created").click();

    // platform
    cy.contains("Hive");

    // entity type
    cy.contains("Dataset");

    // entity name
    cy.contains("fct_cypress_users_created");

    // column name
    cy.contains("user_id");
    // column description
    cy.contains("Id of the user");

    // table description
    cy.contains("table containing all the users created on a single day");
  });

  it("can search and get glossary term facets with proper labels", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)"
    );
    cy.contains("cypress_logging_events");

    cy.contains("Add Term").click();

    cy.focused().type("CypressTerm");

    cy.get(".ant-select-item-option-content").within(() =>
      cy.contains("CypressTerm").click({ force: true })
    );

    cy.get('[data-testid="add-tag-term-from-modal-btn"]').click({
      force: true,
    });
    cy.get('[data-testid="add-tag-term-from-modal-btn"]').should("not.exist");

    cy.contains("CypressTerm");
    cy.visit("/search?query=cypress");
    cy.contains("CypressTerm");
  });

  it("can search by a specific term using advanced search", () => {
    cy.login();

    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("*{enter}");
    cy.wait(2000);

    cy.contains("Advanced").click();

    cy.contains("Add Filter").click();

    cy.contains("Column Glossary Term").click({ force: true });

    cy.get('[data-testid="tag-term-modal-input"]').type("CypressColumnInfo");

    cy.wait(2000);

    cy.get('[data-testid="tag-term-option"]').click({ force: true });

    cy.get('[data-testid="add-tag-term-from-modal-btn"]').click({
      force: true,
    });

    cy.wait(2000);

    // has the term in editable metadata
    cy.contains("SampleCypressHdfsDataset");

    // has the term in non-editable metadata
    cy.contains("cypress_logging_events");

    cy.contains("of 2 results");
  });

  it("can search by AND-ing two concepts using advanced search", () => {
    cy.login();

    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("*{enter}");
    cy.wait(2000);

    cy.contains("Advanced").click();

    cy.contains("Add Filter").click();

    cy.contains("Column Glossary Term").click({ force: true });

    cy.get('[data-testid="tag-term-modal-input"]').type("CypressColumnInfo");

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

    cy.get('[data-testid="edit-text-input"]').type("log event");

    cy.get('[data-testid="edit-text-done-btn"]').click({ force: true });

    // has the term in non-editable metadata
    cy.contains("cypress_logging_events");
  });

  it("can search by OR-ing two concepts using advanced search", () => {
    cy.login();

    cy.visit("/");
    cy.get("input[data-testid=search-input]").type("*{enter}");
    cy.wait(2000);

    cy.contains("Advanced").click();

    cy.contains("Add Filter").click();

    cy.contains("Column Glossary Term").click({ force: true });

    cy.get('[data-testid="tag-term-modal-input"]').type("CypressColumnInfo");

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

    cy.get('[data-testid="edit-text-input"]').type("log event");

    cy.get('[data-testid="edit-text-done-btn"]').click({ force: true });

    // has the term in non-editable metadata
    cy.contains("all filters").click();
    cy.contains("any filter").click({ force: true });

    cy.contains("cypress_logging_events");
    cy.contains("fct_cypress_users_created_no_tag");
    cy.contains("SampleCypressHdfsDataset");
  });
});
