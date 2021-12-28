describe('mutations', () => {
  it('can create and add a tag to dataset and visit new tag page', () => {
    cy.deleteUrn('urn:li:tag:CypressTestAddTag')
    cy.login();
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)');
    cy.contains('cypress_logging_events');

    cy.contains('Add Tag').should('be.visible').click();

    cy.focused().type('CypressTestAddTag');

    cy.contains('Create CypressTestAddTag').should('be.visible').click();

    cy.get('textarea').type('CypressTestAddTag Test Description');

    cy.contains(/Create$/).should('be.visible').click();

    // go to tag page
    cy.get('a[href="/tag/urn:li:tag:CypressTestAddTag"]').should('be.visible').click();

    // title of tag page
    cy.contains('CypressTestAddTag');

    // description of tag page
    cy.contains('CypressTestAddTag Test Description');

    // used by panel - click to search
    cy.contains('1 Datasets').should('be.visible').click();

    // verify dataset shows up in search now
    cy.contains('of 1 result').should('be.visible').click();
    cy.contains('cypress_logging_events').should('be.visible').click();
    cy.get('a[href="/tag/urn:li:tag:CypressTestAddTag"]').within(() => cy.get('span[aria-label=close]').click());
    cy.contains('Yes').should('be.visible').click();

    cy.get('a[href="/tag/urn:li:tag:CypressTestAddTag"]').should('not.exist');

    cy.deleteUrn('urn:li:tag:CypressTestAddTag')
  });
})
