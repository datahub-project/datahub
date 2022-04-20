describe('login', () => {
  it('logs in', () => {
    cy.visit('/');
    cy.get('input[data-testid=username]').type('datahub');
    cy.get('input[data-testid=password]').type('datahub');
    cy.contains('Sign In').click();
    cy.contains('Welcome back, Data Hub');
  });
})
