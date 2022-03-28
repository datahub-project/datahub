describe('proposals', () => {
  it('can propose tags and terms to dataset and then decline them', () => {
    cy.login();
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');

    cy.contains('Add Tag').click({ force: true });

    cy.focused().type('TagToPropose');
    cy.wait(1000);

    cy.get('.ant-select-item-option-content').within(() => cy.contains('TagToPropose').click({ force: true }));

    cy.get('[data-testid="create-proposal-btn"]').click({ force: true });

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('exist');

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');
    cy.contains('Add Term').click({force: true});

    cy.focused().type('TermToPropose');
    cy.wait(1000);

    cy.contains('CypressNode.TermToPropose').click({force: true});

    cy.get('.ant-select-item-option-content').within(() => cy.contains('CypressNode.TermToPropose').click());
    cy.get('[data-testid="create-proposal-btn"]').click({force: true});

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('exist');

    cy.visit('/');
    cy.get('input[data-testid=search-input]').typeSearchDisableCache('TagToPropose');
    cy.contains('DatasetToProposeOn');
    cy.contains('TagToPropose');
    cy.contains('Proposed Tag');

    cy.visit('/');
    cy.get('input[data-testid=search-input]').typeSearchDisableCache('TermToPropose');
    cy.contains('DatasetToProposeOn');
    cy.contains('TermToPropose');
    cy.contains('Proposed Glossary Term');

    cy.wait(1000);

    cy.contains('My Requests').click({force: true});
    cy.get('.action-request-test-id').should('have.length', 2)
    cy.contains('Decline').first().click({force: true});
    cy.contains('Yes').click({force: true});
    cy.get('.action-request-test-id').should('have.length', 1)
    cy.contains('Decline').first().click({force: true});
    cy.contains('Yes').click({force: true});
    cy.get('.action-request-test-id').should('have.length', 0)

    cy.visit('/');
    cy.get('input[data-testid=search-input]').typeSearchDisableCache('TagToPropose');
    cy.contains('DatasetToProposeOn').should('not.exist');

    cy.visit('/');
    cy.get('input[data-testid=search-input]').typeSearchDisableCache('TermToPropose');
    cy.contains('DatasetToProposeOn').should('not.exist');

    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');
    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');
  });
  
  it('can propose tags and terms to dataset and then accept them', () => {
    cy.login();
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
    cy.contains('Add Tag').click({force: true});

    cy.focused().type('TagToPropose');
    cy.wait(1000);

    cy.get('.ant-select-item-option-content').within(() => cy.contains('TagToPropose').click());

    cy.get('[data-testid="create-proposal-btn"]').click({force: true});


    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('exist');

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');
    cy.contains('Add Term').click({force: true});

    cy.focused().type('TermToPropose');
    cy.wait(1000);

    cy.contains('CypressNode.TermToPropose').click({force: true});

    cy.get('.ant-select-item-option-content').within(() => cy.contains('CypressNode.TermToPropose').click());
    cy.get('[data-testid="create-proposal-btn"]').click({force: true});

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('exist');

    cy.wait(1000)

    cy.contains('My Requests').click({force: true});

    cy.get('.action-request-test-id').should('have.length', 2)
    cy.contains('Approve & Add').first().click({force: true});
    cy.contains('Yes').click({force: true});
    cy.get('.action-request-test-id').should('have.length', 1)
    cy.contains('Approve & Add').first().click({force: true});
    cy.contains('Yes').click({force: true});
    cy.get('.action-request-test-id').should('have.length', 0)

    cy.visit('/');
    cy.get('input[data-testid=search-input]').typeSearchDisableCache('TagToPropose');
    cy.contains('DatasetToProposeOn');
    cy.contains('TagToPropose');
    cy.contains('Tag');

    cy.visit('/');
    cy.get('input[data-testid=search-input]').typeSearchDisableCache('TermToPropose');
    cy.contains('DatasetToProposeOn');
    cy.contains('TermToPropose');
    cy.contains('Glossary Term');

    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');
    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');

    cy.get('a[href="/tag/urn:li:tag:TagToPropose"]').within(() => cy.get('span[aria-label=close]').click({force: true}));
    cy.contains('Yes').click({force: true});
    cy.wait(1000);


    cy.get('a[href="/glossary/urn:li:glossaryTerm:CypressNode.TermToPropose"]').within(() => cy.get('span[aria-label=close]').click({force: true}));
    cy.contains('Yes').click({force: true});
    cy.wait(1000);
  });
})
