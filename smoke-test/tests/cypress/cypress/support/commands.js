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
        username: Cypress.env('ADMIN_USERNAME'),
        password: Cypress.env('ADMIN_PASSWORD'),
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

Cypress.Commands.add("logout", () => {
  cy.visit("/logOut")
  cy.waitTextVisible("Username");
  cy.waitTextVisible("Password");
});

Cypress.Commands.add("goToGlossaryList", () => {
  cy.visit("/glossary");
  cy.waitTextVisible("Glossary");
});

Cypress.Commands.add("goToDomainList", () => {
  cy.visit("/domains");
  cy.waitTextVisible("Domains");
  cy.waitTextVisible("New Domain");
});

Cypress.Commands.add("goToDataset", (urn, dataset_name) => {
  cy.visit(
    "/dataset/" + urn
  );
  cy.waitTextVisible(dataset_name);
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
  cy.waitTextVisible("Data Landscape Summary");
});

Cypress.Commands.add("goToUserList", () => {
  cy.visit("/settings/identities/users");
  cy.waitTextVisible("Manage Users & Groups");
})

Cypress.Commands.add("goToStarSearchList", () => {
  cy.visit("/search?query=%2A")
  cy.waitTextVisible("Showing")
  cy.waitTextVisible("results")
})

Cypress.Commands.add("openThreeDotDropdown", () => {
  cy.get('[data-testid="entity-header-dropdown"]').click();
});

Cypress.Commands.add("clickOptionWithText", (text) => {
  cy.contains(text).click();
});

Cypress.Commands.add("deleteFromDropdown", () => {
  cy.openThreeDotDropdown();
  cy.clickOptionWithText("Delete");
  cy.clickOptionWithText("Yes");
});

Cypress.Commands.add("addViaModel", (text, modelHeader) => {
  cy.waitTextVisible(modelHeader);
  cy.get(".ant-form-item-control-input-content > input[type='text']").first().type(text);
  cy.get(".ant-modal-footer > button:nth-child(2)").click();
});

Cypress.Commands.add("ensureTextNotPresent", (text) => {
  cy.contains(text).should("not.exist");
});

Cypress.Commands.add("waitTextPresent", (text) => {
  cy.contains(text).should('exist');
  cy.contains(text).should('have.length.above', 0);
  return cy.contains(text);
})

Cypress.Commands.add("waitTextVisible", (text) => {
  cy.contains(text).should('exist');
  cy.contains(text).should('be.visible');
  cy.contains(text).should('have.length.above', 0);
  return cy.contains(text);
})

Cypress.Commands.add("enterTextInTestId", (id, text) => {
  cy.get('[data-testid="' + id +'"]').type(text);
})

Cypress.Commands.add("clickOptionWithTestId", (id) => {
  cy.get('[data-testid="' + id +'"]').click({
    force: true,
  });
})

Cypress.Commands.add("hideOnboardingTour", () => {
  cy.get('body').type("{ctrl} {meta} h");
});

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

Cypress.Commands.add("removeDomainFromDataset", (urn, dataset_name, domain_urn) => {
  cy.goToDataset(urn, dataset_name);
  cy.get('.sidebar-domain-section [href="/domain/' + domain_urn + '"] .anticon-close').click();
  cy.clickOptionWithText("Yes");
})

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

Cypress.Commands.add("mouseover", (selector) => {
  return cy.get(selector).trigger(
    "mouseover",
    { force: true }
  );
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
