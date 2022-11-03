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
        username: 'datahub',
        password: 'datahub',
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

Cypress.Commands.add("goToGlossaryList", () => {
  cy.visit("/glossary");
  cy.contains("Glossary");
});

Cypress.Commands.add("goToDataset", (urn) => {
  cy.visit(
    "/dataset/" + urn
  );
})

Cypress.Commands.add("openThreeDotDropdown", () => {
  cy.get('div[class^="EntityHeader__SideHeaderContent-"] > div > .ant-dropdown-trigger').click();
});

Cypress.Commands.add("clickOptionWithText", (text) => {
  cy.contains(text).click();
});

Cypress.Commands.add("deleteFromDropdown", () => {
  cy.openThreeDotDropdown();
  cy.clickOptionWithText("Delete");
  cy.clickOptionWithText("Yes");
});

Cypress.Commands.add("addViaModel", (text) => {
  cy.get(".ant-form-item-control-input-content > input[type='text']").type(text);
  cy.get(".ant-modal-footer > button:nth-child(2)").click();
});

Cypress.Commands.add("ensureTextNotPresent", (text) => {
  cy.contains(text).should("not.exist");
});

Cypress.Commands.add('addTermToDataset', (urn, term) => {
  cy.goToDataset(urn);
  cy.clickOptionWithText("Add Term");
  cy.focused().type(term);
  cy.get(".ant-select-item-option-content").within(() =>
    cy.contains(term).click({ force: true })
  );
  cy.get('[data-testid="add-tag-term-from-modal-btn"]').click({
    force: true,
  });
  cy.get('[data-testid="add-tag-term-from-modal-btn"]').should("not.exist");

  cy.contains(term);
});

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
