describe('login', () => {
  it('logs in', () => {
    cy.visit('/');
    cy.get('input[placeholder=Username]').type('datahub');
    cy.get('input[placeholder=Password]').type('datahub');
    cy.contains('Log in').should('be.visible').click();
    cy.wait(5000);
    cy.contains('Welcome back, datahub');
    // cy.contains('Log in').click();
    // cy.contains('Welcome back, Data Hub');
  });
})
