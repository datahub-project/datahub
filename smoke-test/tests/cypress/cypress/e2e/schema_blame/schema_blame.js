describe("schema blame", () => {
  Cypress.on("uncaught:exception", (err, runnable) => false);

  // TODO: (v1_ui_removing) migrate this test
  it.skip("can activate the blame view and verify for an older version of a dataset", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)/Schema?semantic_version=0.0.0",
    );
    cy.wait(10000);

    // Verify which fields are present, along with checking descriptions and tags
    cy.contains("field_foo");
    cy.contains("field_bar");
    cy.contains("field_baz").should("not.exist");
    cy.contains("Foo field description");
    cy.contains("Bar field description");
    cy.clickOptionWithText("field_foo");
    cy.get('[data-testid="schema-field-field_foo-tags"]')
      .contains("Legacy")
      .should("not.exist");

    // Make sure the schema blame is accurate
    cy.get('[data-testid="schema-blame-button"]').click({ force: true });
    cy.wait(3000);

    cy.get('[data-testid="field_foo-schema-blame-description"]').contains(
      "Added in v0.0.0",
    );
    cy.get('[data-testid="field_bar-schema-blame-description"]').contains(
      "Added in v0.0.0",
    );
  });
});
