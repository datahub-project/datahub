describe('schema blame', () => {
    Cypress.on('uncaught:exception', (err, runnable) => {
        return false;
      });

    it('can activate the blame view and verify for the latest version of a dataset', () => {
      cy.login();
      cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)/Schema');
      cy.wait(10000);

      // Verify which fields are present, along with checking descriptions and tags
      cy.contains('field_foo');
      cy.contains('field_baz');
      cy.contains('field_bar').should('not.exist');
      cy.contains('Foo field description has changed');
      cy.contains('Baz field description');
      cy.get('[data-testid="schema-field-field_foo-tags"]').contains('Legacy');

      // Make sure the schema blame is accurate
      cy.get('[data-testid="schema-blame-button"]').click({ force: true });
      cy.wait(3000);
      
      cy.get('[data-testid="field_foo-schema-blame-description"]').contains("Modified in v1.0.0");
      cy.get('[data-testid="field_baz-schema-blame-description"]').contains("Added in v1.0.0");

      // Verify the "view blame prior to" button changes state by modifying the URL
      cy.get('[data-testid="field_foo-view-prior-blame-button"]').click({force: true});
      cy.wait(3000);

      cy.url().should('include', 'semantic_version=1.0.0');
  });

  it('can activate the blame view and verify for an older version of a dataset', () => {
    cy.login();
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)/Schema?semantic_version=0.0.0');
    cy.wait(10000);

      // Verify which fields are present, along with checking descriptions and tags
    cy.contains('field_foo');
    cy.contains('field_bar');
    cy.contains('field_baz').should('not.exist');
    cy.contains('Foo field description');
    cy.contains('Bar field description');
    cy.get('[data-testid="schema-field-field_foo-tags"]').contains('Legacy').should('not.exist');

    // Make sure the schema blame is accurate
    cy.get('[data-testid="schema-blame-button"]').click({ force: true });
    cy.wait(3000);

    cy.get('[data-testid="field_foo-schema-blame-description"]').contains("Added in v0.0.0");
    cy.get('[data-testid="field_bar-schema-blame-description"]').contains("Added in v0.0.0");
});
})