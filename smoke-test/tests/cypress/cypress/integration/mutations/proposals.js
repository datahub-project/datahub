describe('proposals', () => {
  it('can propose tags and terms to dataset and then decline & accept them', () => {
    // Declining proposals
    cy.login();
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');

    cy.contains('Add Tag').click();

    cy.focused().type('TagToPropose');

    cy.get('.ant-select-item-option-content').within(() => cy.contains('TagToPropose').click());

    cy.get('[data-testid="create-proposal-btn"]').click();

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('exist');

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');
    cy.contains('Add Term').click();

    cy.focused().type('TermToPropose');

    cy.contains('CypressNode.TermToPropose').click({force: true});

    cy.get('.ant-select-item-option-content').within(() => cy.contains('CypressNode.TermToPropose').click());
    cy.get('[data-testid="create-proposal-btn"]').click();

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('exist');

    cy.wait(1000)

    cy.contains('My Requests').click();
    cy.get('.action-request-test-id').should('have.length', 2)
    cy.contains('Decline').first().click();
    cy.contains('Yes').click();
    cy.get('.action-request-test-id').should('have.length', 1)
    cy.contains('Decline').first().click();
    cy.contains('Yes').click();
    cy.get('.action-request-test-id').should('have.length', 0)

    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');
    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');

    // Accepting proposals
   cy.contains('Add Tag').click();

    cy.focused().type('TagToPropose');

    cy.get('.ant-select-item-option-content').within(() => cy.contains('TagToPropose').click());

    cy.get('[data-testid="create-proposal-btn"]').click();

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('exist');

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');
    cy.contains('Add Term').click();

    cy.focused().type('TermToPropose');

    cy.contains('CypressNode.TermToPropose').click({force: true});

    cy.get('.ant-select-item-option-content').within(() => cy.contains('CypressNode.TermToPropose').click());
    cy.get('[data-testid="create-proposal-btn"]').click();

    cy.get('[data-testid="proposed-term-TermToPropose"]').should('exist');

    cy.wait(1000)

    cy.contains('My Requests').click();

    cy.get('.action-request-test-id').should('have.length', 2)
    cy.contains('Approve & Add').first().click();
    cy.contains('Yes').click();
    cy.get('.action-request-test-id').should('have.length', 1)
    cy.contains('Approve & Add').first().click();
    cy.contains('Yes').click();
    cy.get('.action-request-test-id').should('have.length', 0)

    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)');
    cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');
    cy.get('[data-testid="proposed-term-TermToPropose"]').should('not.exist');

    cy.get('a[href="/tag/urn:li:tag:TagToPropose"]').within(() => cy.get('span[aria-label=close]').click());
    cy.contains('Yes').click();
    cy.get('a[href="/glossary/urn:li:glossaryTerm:CypressNode.TermToPropose"]').within(() => cy.get('span[aria-label=close]').click());
    cy.contains('Yes').click();
  });
})
