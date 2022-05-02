describe('home', () => {
    it('home page shows ', () => {
      cy.login();
      cy.visit('/');
      cy.wait(5000);
      cy.get('[data-testid="entity-type-browse-card-DATASET"]').should('exist');
      cy.get('[data-testid="entity-type-browse-card-DASHBOARD"]').should('exist');
      cy.get('[data-testid="entity-type-browse-card-CHART"]').should('exist');
      cy.get('[data-testid="entity-type-browse-card-DATA_FLOW"]').should('exist');
      cy.get('[data-testid="entity-type-browse-card-GLOSSARY_TERM"]').should('exist');
    });
  })