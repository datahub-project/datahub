// ***********************************************
// This example commands.js shows you how to
// create various custom commands and overwrite
// existing commands.
//
// For more comprehensive examples of custom
// commands please read more here:
// https://on.cypress.io/custom-commands
// ***********************************************
//
//
// -- This is a parent command --
Cypress.Commands.add('login', () => {
    cy.request({
      method: 'POST',
      url: '/logIn',
      body: {
        username: 'admin',
        password: 'mypass',
      },
      retryOnStatusCodeFailure: true,
    });
})

Cypress.Commands.add('deleteUrn', (urn) => {
    cy.request({ method: 'POST', url: 'http://localhost:8080/entities?action=delete', body: {
        urn
    }, headers: {
        "X-RestLi-Protocol-Version": "2.0.0",
        "Content-Type": "application/json",
    }})
})

Cypress.Commands.add('typeSearchDisableCache', {prevSubject: 'element'}, (subject, input) => {
    const randomStrGenerator = () => Cypress._.random(0, 1e9);
    const randomStr = randomStrGenerator();
    const combinedStr = `${input} OR ${randomStr}{enter}`
    cy.get(subject.selector).type(combinedStr);
})
//
//
// -- This is a child command --
// Cypress.Commands.add('drag', { prevSubject: 'element'}, (subject, options) => { ... })
//
//
// -- This is a dual command --
// Cypress.Commands.add('dismiss', { prevSubject: 'optional'}, (subject, options) => { ... })
//
//
// -- This will overwrite an existing command --
// Cypress.Commands.overwrite('visit', (originalFn, url, options) => { ... })
