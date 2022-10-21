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

Cypress.Commands.add("goToDataset", (urn, dataset_name) => {
  cy.visit(
    "/dataset/" + urn
  );
  cy.ensureTextPresent(dataset_name);
})

Cypress.Commands.add("goToChart", (urn) => {
  cy.visit(
    "/chart/" + urn
  );
})

Cypress.Commands.add("goToContainer", (urn) => {
  cy.visit(
    "/container/" + urn
  );
})

Cypress.Commands.add("goToDomain", (urn) => {
  cy.visit(
    "/domain/" + urn
  );
})

Cypress.Commands.add("goToAnalytics", () => {
  cy.visit("/analytics");
  cy.ensureTextPresent("Data Landscape Summary");
});

Cypress.Commands.add("openThreeDotDropdown", () => {
  cy.get('div[class^="EntityHeader__SideHeaderContent-"] > div > .ant-dropdown-trigger').click();
});

Cypress.Commands.add("clickOptionWithText", (text) => {
  cy.contains(text).click({force: true});
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

Cypress.Commands.add("ensureTextPresent", (text) => {
  cy.contains(text);
});

Cypress.Commands.add("ensureTextNotPresent", (text) => {
  cy.contains(text).should("not.exist");
});

Cypress.Commands.add("clickOptionWithTestId", (id) => {
  cy.get('[data-testid="' + id +'"]').click({
    force: true,
  });
})

Cypress.Commands.add('addTermToDataset', (urn, dataset_name, term) => {
  cy.goToDataset(urn, dataset_name);
  cy.clickOptionWithText("Add Term");
  cy.focused().type(term);
  cy.get(".ant-select-item-option-content").within(() =>
    cy.contains(term).click({ force: true })
  );
  cy.clickOptionWithTestId('add-tag-term-from-modal-btn');
  cy.get('[data-testid="add-tag-term-from-modal-btn"]').should("not.exist");

  cy.contains(term);
});

Cypress.Commands.add("openEntityTab", (tab) => {
  const selector = 'div[id$="' + tab + '"]:nth-child(1)'
  cy.highlighElement(selector);
  cy.get(selector).click()
});

Cypress.Commands.add("highlighElement", (selector) => {
  cy.wait(3000);
  cy.get(selector).then($button => {
    $button.css('border', '1px solid magenta')
  })
  cy.wait(3000);
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
