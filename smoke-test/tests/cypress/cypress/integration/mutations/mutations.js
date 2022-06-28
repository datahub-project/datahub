describe('mutations', () => {
  before(() => {
    // warm up elastic by issuing a `*` search
    cy.visit('http://localhost:9002/search?query=%2A');
    cy.wait(5000);
  });

  it('can create and add a tag to dataset and visit new tag page', () => {
    cy.deleteUrn('urn:li:tag:CypressTestAddTag')
    cy.login();
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)');
    cy.contains('cypress_logging_events');

    cy.contains('Add Tag').click({force:true});

    cy.focused().type('CypressTestAddTag');

    cy.contains('Create CypressTestAddTag').click({force:true});

    cy.get('textarea').type('CypressTestAddTag Test Description');

    cy.contains(/Create$/).click({force:true});

    // wait a breath for elasticsearch to index the tag being applied to the dataset- if we navigate too quick ES
    // wont know and we'll see applied to 0 entities
    cy.wait(2000);

    // go to tag drawer
    cy.contains('CypressTestAddTag').click();
    
    cy.wait(1000);
    
    // Click the Tag Details to launch full profile
    cy.contains('Tag Details').click();
    
    cy.wait(1000);

    // title of tag page
    cy.contains('CypressTestAddTag');

    // description of tag page
    cy.contains('CypressTestAddTag Test Description');

    // used by panel - click to search
    cy.contains('1 Datasets').click();

    // verify dataset shows up in search now
    cy.contains('of 1 result').click();
    cy.contains('cypress_logging_events').click();
    cy.contains('CypressTestAddTag').within(() => cy.get('span[aria-label=close]').click());
    cy.contains('Yes').click();

    cy.contains('CypressTestAddTag').should('not.exist');

    cy.deleteUrn('urn:li:tag:CypressTestAddTag')
  });

  it('can add and remove terms from a dataset', () => {
    cy.login();
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)');
    cy.contains('cypress_logging_events');

    cy.contains('Add Term').click();

    cy.focused().type('CypressTerm');

    cy.get('.ant-select-item-option-content').within(() => cy.contains('CypressTerm').click({force: true}));

    cy.get('[data-testid="add-tag-term-from-modal-btn"]').click({force: true});
    cy.get('[data-testid="add-tag-term-from-modal-btn"]').should('not.exist');

    cy.contains('CypressTerm');

    cy.get('a[href="/glossaryTerm/urn:li:glossaryTerm:CypressNode.CypressTerm"]').within(() => cy.get('span[aria-label=close]').click());
    cy.contains('Yes').click();

    cy.contains('CypressTerm').should('not.exist');
  });

  it('can add and remove terms from a dataset field', () => {
    cy.login();
    // make space for the glossary term column
    cy.viewport(2000, 800)
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)');
    cy.get('[data-testid="schema-field-event_name-terms"]').trigger('mouseover', {force: true});
    cy.get('[data-testid="schema-field-event_name-terms"]').within(() => cy.contains('Add Term').click())

    cy.focused().type('CypressTerm');

    cy.get('.ant-select-item-option-content').within(() => cy.contains('CypressTerm').click({force: true}));

    cy.get('[data-testid="add-tag-term-from-modal-btn"]').click({force: true});
    cy.get('[data-testid="add-tag-term-from-modal-btn"]').should('not.exist');

    cy.contains('CypressTerm');

    cy.get('a[href="/glossaryTerm/urn:li:glossaryTerm:CypressNode.CypressTerm"]').within(() => cy.get('span[aria-label=close]').click());
    cy.contains('Yes').click();

    cy.contains('CypressTerm').should('not.exist');
  });
})
