describe('proposals', () => {
  Cypress.on('uncaught:exception', (err, runnable) => {
    return false;
  });
  
  it('can propose tag to dataset and then decline tag proposal from the dataset page', () => {
    cy.login();

    // Proposing the tag
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');

    cy.contains('Add Tag').click({ force: true });
    cy.wait(1000);

    cy.focused().type('TagToPropose');
    cy.wait(3000);

    cy.get('.ant-select-item-option-content').within(() => cy.contains('TagToPropose').click({ force: true }));
    cy.get('[data-testid="create-proposal-btn"]').click({ force: true });
    cy.wait(5000);
    cy.reload();

    // Rejecting the proposal
    cy.get('[data-testid="proposed-tag-TagToPropose"]').click({ force: true });
    cy.get('[data-testid="proposal-reject-button-TagToPropose"]').click({ force: true });
    cy.wait(1000);

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');
  });

  it('can propose term to dataset and then decline term proposal from the dataset page', () => {
    cy.login();

    // Proposing the term
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');
    cy.contains('Add Term').click({force: true});
    cy.wait(1000);

    cy.focused().type('TermToPropose');
    cy.wait(3000);

    cy.contains('CypressNode.TermToPropose').click({force: true});

    cy.get('.ant-select-item-option-content').within(() => cy.contains('CypressNode.TermToPropose').click({force: true}));
    cy.get('[data-testid="create-proposal-btn"]').click({force: true});
    cy.wait(5000);
    cy.reload();

    // Rejecting the proposal
    cy.get('[data-testid="proposed-term-TermToPropose"]').click({ force: true });
    cy.get('[data-testid="proposal-reject-button-TermToPropose"]').click({ force: true });
    cy.wait(1000);

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');
  });

  it('can propose tag to dataset and then accept tag proposal from the dataset page', () => {
    cy.login();

    // Proposing the tag
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
    cy.contains('Add Tag').click({force: true});

    cy.focused().type('TagToPropose');
    cy.wait(3000);

    cy.get('.ant-select-item-option-content').within(() => cy.contains('TagToPropose').click({ force: true }));

    cy.get('[data-testid="create-proposal-btn"]').click({force: true});
    cy.wait(5000);
    cy.reload();

    // Accepting the proposal
    cy.get('[data-testid="proposed-tag-TagToPropose"]').click({ force: true });
    cy.get('[data-testid="proposal-accept-button-TagToPropose"]').click({ force: true });
    cy.wait(3000);

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');
    cy.wait(1000);

    // Deleting the tag (data cleanup)
    cy.contains('TagToPropose').within(() => cy.get('span[aria-label=close]').click({force: true}));
    cy.wait(1000);

    cy.contains('Yes').click({force: true});
    cy.wait(1000);
  });

  it('can propose term to dataset and then accept term proposal from the dataset page', () => {
    cy.login();

    // Proposing the term
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');
    cy.contains('Add Term').click({force: true});
    cy.wait(1000);

    cy.focused().type('TermToPropose');
    cy.wait(3000);

    cy.contains('CypressNode.TermToPropose').click({force: true});

    cy.get('.ant-select-item-option-content').within(() => cy.contains('CypressNode.TermToPropose').click({force: true}));
    cy.get('[data-testid="create-proposal-btn"]').click({force: true});
    cy.wait(5000);
    cy.reload();

    // Accepting the proposal
    cy.get('[data-testid="proposed-term-TermToPropose"]').click({ force: true });
    cy.get('[data-testid="proposal-accept-button-TermToPropose"]').click({ force: true });
    cy.wait(3000);

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');
    cy.wait(1000);

    // Deleting the term (data cleanup)
    cy.get('a[href="/glossary/urn:li:glossaryTerm:CypressNode.TermToPropose"]').within(() => cy.get('span[aria-label=close]').click({force: true}));
    cy.contains('Yes').click({force: true});
    cy.wait(1000);
  });

  it('can propose tag to dataset and then decline tag proposal from the my requests tab', () => {
    cy.login();

    // Proposing the tag
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');

    cy.contains('Add Tag').click({ force: true });

    cy.focused().type('TagToPropose');
    cy.wait(3000);

    cy.get('.ant-select-item-option-content').within(() => cy.contains('TagToPropose').click({ force: true }));

    cy.get('[data-testid="create-proposal-btn"]').click({ force: true });
    cy.wait(5000);
    cy.reload();

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('exist');

    // Checking search result after proposing
    cy.visit('/');
    cy.get('input[data-testid=search-input]').typeSearchDisableCache('TagToPropose');
    cy.contains('DatasetToProposeOn');
    cy.contains('TagToPropose');
    cy.contains('Proposed Tag');

    cy.wait(1000);

    // Rejecting the proposal
    cy.contains('My Requests').click({force: true});
    cy.get('.action-request-test-id').should('have.length', 1)
    cy.contains('Decline').first().click({force: true});
    cy.contains('Yes').click({force: true});
    cy.get('.action-request-test-id').should('have.length', 0)

    cy.visit('/');
    cy.get('input[data-testid=search-input]').typeSearchDisableCache('TagToPropose');
    cy.contains('DatasetToProposeOn').should('not.exist');

    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');
  });

  it('can propose term to dataset and then decline term proposal from the my requests tab', () => {
    cy.login();

    // Proposing the term
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');
    cy.contains('Add Term').click({force: true});

    cy.focused().type('TermToPropose');
    cy.wait(3000);

    cy.contains('CypressNode.TermToPropose').click({force: true});

    cy.get('.ant-select-item-option-content').within(() => cy.contains('CypressNode.TermToPropose').click({ force: true }));
    cy.get('[data-testid="create-proposal-btn"]').click({force: true});
    cy.wait(5000);
    cy.reload();

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('exist');

    // Checking search result after proposing
    cy.visit('/');
    cy.get('input[data-testid=search-input]').typeSearchDisableCache('TermToPropose');
    cy.contains('DatasetToProposeOn');
    cy.contains('TermToPropose');
    cy.contains('Proposed Glossary Term');

    cy.wait(1000);

    // Rejecting the proposal
    cy.contains('My Requests').click({force: true});
    cy.get('.action-request-test-id').should('have.length', 1)
    cy.contains('Decline').first().click({force: true});
    cy.contains('Yes').click({force: true});
    cy.get('.action-request-test-id').should('have.length', 0)

    cy.visit('/');
    cy.get('input[data-testid=search-input]').typeSearchDisableCache('TermToPropose');
    cy.contains('DatasetToProposeOn').should('not.exist');

    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');
  });
  
  it('can propose tag to dataset and then accept tag proposal from the my requests tab', () => {
    cy.login();

    // Proposing the tag
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
    cy.contains('Add Tag').click({force: true});

    cy.focused().type('TagToPropose');
    cy.wait(3000);

    cy.get('.ant-select-item-option-content').within(() => cy.contains('TagToPropose').click({ force: true }));

    cy.get('[data-testid="create-proposal-btn"]').click({force: true});
    cy.wait(5000);
    cy.reload();
    
    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('exist');

    cy.contains('My Requests').click({force: true});

    // Accepting the proposal
    cy.get('.action-request-test-id').should('have.length', 1)
    cy.contains('Approve & Add').first().click({force: true});
    cy.contains('Yes').click({force: true});
    cy.get('.action-request-test-id').should('have.length', 0)

    // Checking search results after accepting proposal
    cy.visit('/');
    cy.get('input[data-testid=search-input]').typeSearchDisableCache('TagToPropose');
    cy.contains('DatasetToProposeOn');
    cy.contains('TagToPropose');
    cy.contains('Tag');

    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');

    cy.contains('TagToPropose').within(() => cy.get('span[aria-label=close]').click({force: true}));
    cy.wait(1000);

    cy.contains('Yes').click({force: true});
    cy.wait(1000);
  });

  it('can propose term to dataset and then accept term proposal from the my requests tab', () => {
    cy.login();

    // Proposing the term
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');
    cy.contains('Add Term').click({force: true});

    cy.focused().type('TermToPropose');
    cy.wait(3000);

    cy.contains('CypressNode.TermToPropose').click({force: true});

    cy.get('.ant-select-item-option-content').within(() => cy.contains('CypressNode.TermToPropose').click({ force: true }));
    cy.get('[data-testid="create-proposal-btn"]').click({force: true});
    cy.wait(5000);
    cy.reload();

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('exist');

    cy.wait(1000)

    cy.contains('My Requests').click({force: true});

    // Accepting the proposal
    cy.get('.action-request-test-id').should('have.length', 1)
    cy.contains('Approve & Add').first().click({force: true});
    cy.contains('Yes').click({force: true});
    cy.get('.action-request-test-id').should('have.length', 0)

    // Checking search results after accepting proposal
    cy.visit('/');
    cy.get('input[data-testid=search-input]').typeSearchDisableCache('TermToPropose');
    cy.contains('DatasetToProposeOn');
    cy.contains('TermToPropose');
    cy.contains('Glossary Term');

    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');
    cy.wait(1000);

    cy.get('a[href="/glossary/urn:li:glossaryTerm:CypressNode.TermToPropose"]').within(() => cy.get('span[aria-label=close]').click({force: true}));
    cy.contains('Yes').click({force: true});
    cy.wait(1000);
  });
})
