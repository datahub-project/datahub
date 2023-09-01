describe('login', () => {
  it('logs in', () => {
    cy.visit('/');
    cy.get('input[data-testid=username]').type(Cypress.env('ADMIN_USERNAME'));
    cy.get('input[data-testid=password]').type(Cypress.env('ADMIN_PASSWORD'));
    cy.contains('Sign In').click();
    cy.contains('Welcome back, DataHub');
  });
})
