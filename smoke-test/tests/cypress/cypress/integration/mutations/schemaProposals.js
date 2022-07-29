describe('schemaProposals', () => {
  Cypress.on('uncaught:exception', (err, runnable) => {
    return false;
  });

  /*
  it('can propose a schema-level tag and then decline tag proposal from the dataset page', () => {
    cy.login();

    // Proposing a tag
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
    cy.get('[data-testid="schema-field-field_foo-tags"]').trigger('mouseover', {force: true});
    cy.get('[data-testid="schema-field-field_foo-tags"]').within(() => cy.contains('Add Tags').click());

    cy.focused().type('TagToPropose');
    cy.wait(3000);

    cy.get('.ant-select-item-option-content').within(() => cy.contains('TagToPropose').click({force: true}));
    cy.wait(3000);

    cy.get('[data-testid="create-proposal-btn"]').click({force: true});
    cy.wait(5000);   
    cy.reload();

    // Rejecting tag proposal
    cy.get('[data-testid="proposed-tag-TagToPropose"]').click({ force: true });
    cy.get('[data-testid="proposal-reject-button-TagToPropose"]').click({ force: true });
    cy.wait(3000);

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');
  });

  it('can propose a schema-level term and then decline term proposal from the dataset page', () => {
    cy.login();

    // Proposing a term
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
    cy.get('[data-testid="schema-field-field_foo-terms"]').trigger('mouseover', {force: true});
    cy.get('[data-testid="schema-field-field_foo-terms"]').within(() => cy.contains('Add Terms').click());

    cy.focused().type('TermToPropose');
    cy.wait(3000);

    cy.get('.ant-select-item-option-content').within(() => {
      cy.wait(1000);
      cy.contains('TermToPropose').click({force: true});
    });
    cy.get('[data-testid="create-proposal-btn"]').click({force: true});
    cy.wait(5000);
    cy.reload();

    // Rejecting term proposal
    cy.get('[data-testid="proposed-term-TermToPropose"]').click({ force: true });
    cy.get('[data-testid="proposal-reject-button-TermToPropose"]').click({ force: true });
    cy.wait(3000);

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');
  });

  it('can propose a schema-level tag and then accept tag proposal from the dataset page', () => {
    cy.login();

    // Proposing a tag
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
    cy.get('[data-testid="schema-field-field_foo-tags"]').trigger('mouseover', {force: true});
    cy.get('[data-testid="schema-field-field_foo-tags"]').within(() => cy.contains('Add Tags').click());

    cy.focused().type('TagToPropose');
    cy.wait(3000);

    cy.get('.ant-select-item-option-content').within(() => cy.contains('TagToPropose').click({force: true}));

    cy.get('[data-testid="create-proposal-btn"]').click({force: true});
    cy.wait(5000);
    cy.reload();   

    // Accepting tag proposal
    cy.get('[data-testid="proposed-tag-TagToPropose"]').click({ force: true });
    cy.get('[data-testid="proposal-accept-button-TagToPropose"]').click({ force: true });
    cy.wait(3000);

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');

    // Data cleanup
    cy.contains('TagToPropose').within(() => cy.get('span[aria-label=close]').click());
    cy.contains('Yes').click();
  });

  it('can propose a schema-level term and then accept term proposal from the dataset page', () => {
    cy.login();

    // Proposing a term
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
    cy.get('[data-testid="schema-field-field_foo-terms"]').trigger('mouseover', {force: true});
    cy.get('[data-testid="schema-field-field_foo-terms"]').within(() => cy.contains('Add Terms').click());

    cy.focused().type('TermToPropose');
    cy.wait(3000);

    cy.get('.ant-select-item-option-content').within(() => {
      cy.wait(1000);
      cy.contains('TermToPropose').click({force: true});
    });
    cy.get('[data-testid="create-proposal-btn"]').click({force: true});
    cy.wait(5000);
    cy.reload();

    // Accepting term proposal
    cy.get('[data-testid="proposed-term-TermToPropose"]').click({ force: true });
    cy.get('[data-testid="proposal-accept-button-TermToPropose"]').click({ force: true });
    cy.wait(3000);

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');

    // Data cleanup
    cy.contains('TermToPropose').within(() => cy.get('span[aria-label=close]').click());
    cy.contains('Yes').click();
  });
  
    it('can propose a schema-level tag and then decline the proposal from the my requests tab', () => {
      cy.login();

      // Proposing a tag
      cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
      cy.get('[data-testid="schema-field-field_foo-tags"]').trigger('mouseover', {force: true});
      cy.get('[data-testid="schema-field-field_foo-tags"]').within(() => cy.contains('Add Tags').click());

      cy.focused().type('TagToPropose');
      cy.wait(3000);

      cy.get('.ant-select-item-option-content').within(() => cy.contains('TagToPropose').click({force: true}));

      cy.get('[data-testid="create-proposal-btn"]').click({force: true});
      cy.wait(5000);
      cy.reload(); 
  
      // Checking search result after proposing
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TagToPropose');
      cy.contains('DatasetToProposeOn');
      cy.contains('TagToPropose');

      // Rejecting proposal
      cy.contains('My Requests').click({force: true});
      cy.get('.action-request-test-id').should('have.length', 1)
      cy.contains('Decline').first().click({force: true});
      cy.contains('Yes').click({force: true});
      cy.get('.action-request-test-id').should('have.length', 0)
  
      // Verifying the proposal doesn't show up in search results
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TagToPropose');
      cy.contains('DatasetToProposeOn').should('not.exist');
    });

    it('can propose a schema-level glossary term and then decline the proposal from the my requests tab', () => {
      cy.login();

      // Proposing a term
      cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
      cy.get('[data-testid="schema-field-field_foo-terms"]').trigger('mouseover', {force: true});
      cy.get('[data-testid="schema-field-field_foo-terms"]').within(() => cy.contains('Add Terms').click());

      cy.focused().type('TermToPropose');
      cy.wait(3000);

      cy.get('.ant-select-item-option-content').within(() => {
        cy.wait(1000);
        cy.contains('TermToPropose').click({force: true});
      });
      cy.get('[data-testid="create-proposal-btn"]').click({force: true});
      cy.wait(5000);
      cy.reload();

      // Checking search results after proposing
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TermToPropose');
      cy.contains('DatasetToProposeOn');
      cy.contains('TermToPropose');

      // Rejecting proposals
      cy.contains('My Requests').click({force: true});
      cy.get('.action-request-test-id').should('have.length', 1)
      cy.contains('Decline').first().click({force: true});
      cy.contains('Yes').click({force: true});
      cy.get('.action-request-test-id').should('have.length', 0)
  
      // Verifying the proposal doesn't show up in search results
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TermToPropose');
      cy.contains('DatasetToProposeOn').should('not.exist');
    });
    */
    it('can propose a schema-level tag to a dataset and then accept the proposal from the my requests tab', () => {
      cy.login();

      // Proposing a tag
      cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
      cy.get('[data-testid="schema-field-field_foo-tags"]').trigger('mouseover', {force: true});
      cy.get('[data-testid="schema-field-field_foo-tags"]').within(() => cy.contains('Add Tags').click());

      cy.focused().type('TagToPropose');
      cy.wait(1000);

      cy.get('.ant-select-item-option-content').within(() => cy.contains('TagToPropose').click({force: true}));

      cy.get('[data-testid="create-proposal-btn"]').click({force: true});   
      cy.wait(5000);
      cy.reload();

      // Checking search results after proposing
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TagToPropose');
      cy.contains('DatasetToProposeOn');
      cy.contains('TagToPropose');

      // Accepting proposal
      cy.contains('Inbox').click({force: true});
      cy.get('.action-request-test-id').should('have.length', 1)
      cy.contains('Approve').first().click({force: true});
      cy.contains('Yes').click({force: true});
      cy.get('.action-request-test-id').should('have.length', 0)
  
      // Verifying the accepted proposals show up in search results
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TagToPropose');
      cy.contains('DatasetToProposeOn');

      // Verifying the applied tag is present
      cy.wait(3000);

      cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');

      cy.wait(3000);

      cy.get('[data-testid="schema-field-field_foo-tags"]').contains('TagToPropose');
      cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');

      // Data cleanup
      cy.contains('TagToPropose').within(() => cy.get('span[aria-label=close]').click({force: true}));
      cy.contains('Yes').click();
    });

    it('can propose a schema-level term to dataset and then accept the proposal from the my requests tab', () => {
      cy.login();

      // Proposing a term
      cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
      cy.get('[data-testid="schema-field-field_foo-terms"]').trigger('mouseover', {force: true});
      cy.get('[data-testid="schema-field-field_foo-terms"]').within(() => cy.contains('Add Terms').click());

      cy.focused().type('TermToPropose');
      cy.wait(1000);

      cy.get('.ant-select-item-option-content').within(() => {
        cy.wait(1000);
        cy.contains('TermToPropose').click({force: true});
      });
      cy.get('[data-testid="create-proposal-btn"]').click({force: true});
      cy.wait(5000);
      cy.reload();

      // Checking search results after proposing
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TermToPropose');
      cy.contains('DatasetToProposeOn');
      cy.contains('TermToPropose');

      // Accepting proposal
      cy.contains('Inbox').click({force: true});
      cy.get('.action-request-test-id').should('have.length', 1)
      cy.contains('Approve').first().click({force: true});
      cy.contains('Yes').click({force: true});
      cy.get('.action-request-test-id').should('have.length', 0)
  
      // Verifying the accepted proposal shows up in search results
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TermToPropose');
      cy.contains('DatasetToProposeOn');

      // Verifying the applied glossary term is present
      cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
      cy.wait(3000);
      cy.get('[data-testid="schema-field-field_foo-terms"]').contains('TermToPropose');

      // Data cleanup
      cy.contains('TermToPropose').within(() => cy.get('span[aria-label=close]').click({force: true}));
      cy.contains('Yes').click();
    });
  })
  