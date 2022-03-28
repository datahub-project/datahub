describe('schemaProposals', () => {
    it('can propose schema-level tags and terms to dataset and then decline them', () => {
      cy.login();

      // Proposing a tag
      cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
      cy.get('[data-testid="schema-field-field_foo-tags"]').trigger('mouseover', {force: true});
      cy.get('[data-testid="schema-field-field_foo-tags"]').within(() => cy.contains('Add Tag').click());

      cy.focused().type('TagToPropose');
      cy.wait(1000);

      cy.get('.ant-select-item-option-content').within(() => cy.contains('TagToPropose').click({force: true}));

      cy.get('[data-testid="create-proposal-btn"]').click({force: true});   

      // Proposing a term
      cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
      cy.get('[data-testid="schema-field-field_foo-terms"]').trigger('mouseover', {force: true});
      cy.get('[data-testid="schema-field-field_foo-terms"]').within(() => cy.contains('Add Term').click());

      cy.focused().type('CypressNode.TermToPropose');
      cy.wait(1000);

      cy.get('.ant-select-item-option-content').within(() => {
        cy.wait(1000);
        cy.contains('CypressNode.TermToPropose').click({force: true});
      });
      cy.get('[data-testid="create-proposal-btn"]').click({force: true});

      // Checking search results after proposing
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TagToPropose');
      cy.contains('DatasetToProposeOn');
      cy.contains('TagToPropose');
      cy.contains('Proposed Schema Tag');
  
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TermToPropose');
      cy.contains('DatasetToProposeOn');
      cy.contains('TermToPropose');
      cy.contains('Proposed Schema Glossary Term');

      // Rejecting proposals
      cy.contains('My Requests').click({force: true});
      cy.get('.action-request-test-id').should('have.length', 2)
      cy.contains('Decline').first().click({force: true});
      cy.contains('Yes').click({force: true});
      cy.get('.action-request-test-id').should('have.length', 1)
      cy.contains('Decline').first().click({force: true});
      cy.contains('Yes').click({force: true});
      cy.get('.action-request-test-id').should('have.length', 0)
  
      // Verifying the proposals don't show up in search results
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TagToPropose');
      cy.contains('DatasetToProposeOn').should('not.exist');
  
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TermToPropose');
      cy.contains('DatasetToProposeOn').should('not.exist');
    });

    it('can propose schema-level tags and terms to dataset and then accept them', () => {
      cy.login();

      // Proposing a tag
      cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
      cy.get('[data-testid="schema-field-field_foo-tags"]').trigger('mouseover', {force: true});
      cy.get('[data-testid="schema-field-field_foo-tags"]').within(() => cy.contains('Add Tag').click());

      cy.focused().type('TagToPropose');
      cy.wait(1000);

      cy.get('.ant-select-item-option-content').within(() => cy.contains('TagToPropose').click({force: true}));

      cy.get('[data-testid="create-proposal-btn"]').click({force: true});   

      // Proposing a term
      cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
      cy.get('[data-testid="schema-field-field_foo-terms"]').trigger('mouseover', {force: true});
      cy.get('[data-testid="schema-field-field_foo-terms"]').within(() => cy.contains('Add Term').click());

      cy.focused().type('CypressNode.TermToPropose');
      cy.wait(1000);

      cy.get('.ant-select-item-option-content').within(() => {
        cy.wait(1000);
        cy.contains('CypressNode.TermToPropose').click({force: true});
      });
      cy.get('[data-testid="create-proposal-btn"]').click({force: true});

      // Checking search results after proposing
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TagToPropose');
      cy.contains('DatasetToProposeOn');
      cy.contains('TagToPropose');
      cy.contains('Proposed Schema Tag');
  
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TermToPropose');
      cy.contains('DatasetToProposeOn');
      cy.contains('TermToPropose');
      cy.contains('Proposed Schema Glossary Term');

      // Accepting proposals
      cy.contains('My Requests').click({force: true});
      cy.get('.action-request-test-id').should('have.length', 2)
      cy.contains('Approve & Add').first().click({force: true});
      cy.contains('Yes').click({force: true});
      cy.get('.action-request-test-id').should('have.length', 1)
      cy.contains('Approve & Add').first().click({force: true});
      cy.contains('Yes').click({force: true});
      cy.get('.action-request-test-id').should('have.length', 0)
  
      // Verifying the accepted proposals show up in search results
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TagToPropose');
      cy.contains('DatasetToProposeOn');
  
      cy.visit('/');
      cy.get('input[data-testid=search-input]').typeSearchDisableCache('TermToPropose');
      cy.contains('DatasetToProposeOn');

      // Verifying the accepted proposals show up on the columns
      cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
      cy.get('[data-testid="schema-field-field_foo-tags"]').contains('TagToPropose');

      cy.get('a[href="/tag/urn:li:tag:TagToPropose"]').within(() => cy.get('span[aria-label=close]').click());
      cy.contains('Yes').click();
  
      cy.contains('TagToPropose').should('not.exist');

      cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
      cy.get('[data-testid="schema-field-field_foo-terms"]').contains('TermToPropose');

      cy.get('a[href="/glossary/urn:li:glossaryTerm:CypressNode.TermToPropose"]').within(() => cy.get('span[aria-label=close]').click());
      cy.contains('Yes').click();
  
      cy.contains('CypressNode.TermToPropose').should('not.exist');
    });
  })
  