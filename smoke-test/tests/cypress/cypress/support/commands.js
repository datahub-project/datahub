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

function selectorWithtestId (id) {
  return '[data-testid="' + id +'"]';
}

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
  cy.get(selectorWithtestId("manage-account-menu")).click();
  cy.get(selectorWithtestId("log-out-menu-item")).click({ force: true });
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

Cypress.Commands.add("goToViewsSettings", () => {
  cy.visit("/settings/views");
  cy.waitTextVisible("Manage Views");
});

Cypress.Commands.add("goToIngestionPage", () => {
  cy.visit("/ingestion");
  cy.waitTextVisible("Manage Ingestion");
});

Cypress.Commands.add("goToDataset", (urn, dataset_name) => {
  cy.visit(
    "/dataset/" + urn
  );
  cy.waitTextVisible(dataset_name);
});

Cypress.Commands.add("goToEntityLineageGraph", (entity_type, urn) => {
  cy.visit(
    `/${entity_type}/${urn}?is_lineage_mode=true`
  );
})

Cypress.Commands.add("goToEntityLineageGraph", (entity_type, urn, start_time_millis, end_time_millis) => {
  cy.visit(
    `/${entity_type}/${urn}?is_lineage_mode=true&start_time_millis=${start_time_millis}&end_time_millis=${end_time_millis}`
  );
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
  cy.contains("Data Landscape Summary", {timeout: 10000});
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
  cy.clickOptionWithTestId("entity-header-dropdown")
});

Cypress.Commands.add("clickOptionWithText", (text) => {
  cy.contains(text).click();
});

Cypress.Commands.add("deleteFromDropdown", () => {
  cy.openThreeDotDropdown();
  cy.clickOptionWithText("Delete");
  cy.clickOptionWithText("Yes");
});

Cypress.Commands.add("addViaFormModal", (text, modelHeader) => {
  cy.waitTextVisible(modelHeader);
  cy.get(".ant-form-item-control-input-content > input[type='text']").first().type(text);
  cy.get(".ant-modal-footer > button:nth-child(2)").click();
});

Cypress.Commands.add("addViaModal", (text, modelHeader) => {
  cy.waitTextVisible(modelHeader);
  cy.get(".ant-input-affix-wrapper > input[type='text']").first().type(text);
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

Cypress.Commands.add("openMultiSelect", (data_id) => {
  let selector = `${selectorWithtestId(data_id)}`
  cy.get(`.ant-select${selector} > .ant-select-selector > .ant-select-selection-search`).click();
})

Cypress.Commands.add( 'multiSelect', (within_data_id , text) => {
  cy.openMultiSelect(within_data_id);
  cy.waitTextVisible(text);
  cy.clickOptionWithText(text);
});

Cypress.Commands.add("enterTextInTestId", (id, text) => {
  cy.get(selectorWithtestId(id)).type(text);
})

Cypress.Commands.add("clickOptionWithTestId", (id) => {
  cy.get(selectorWithtestId(id)).first().click({
    force: true,
  });
})

Cypress.Commands.add("clickFirstOptionWithTestId", (id) => {
  cy.get(selectorWithtestId(id)).first().click({
    force: true,
  });
})

Cypress.Commands.add("hideOnboardingTour", () => {
  cy.get('body').type("{ctrl} {meta} h");
});

Cypress.Commands.add("clearView", (viewName) => {
  cy.clickOptionWithTestId("view-select");
  cy.clickOptionWithTestId("view-select-clear");
  cy.get("input[data-testid='search-input']").click();
  cy.contains(viewName).should("not.be.visible");
})

Cypress.Commands.add('addTermToDataset', (urn, dataset_name, term) => {
  cy.goToDataset(urn, dataset_name);
  cy.clickOptionWithText("Add Term");
  cy.selectOptionInTagTermModal(term);
  cy.contains(term);
});

Cypress.Commands.add('selectOptionInTagTermModal', (text) => {
  cy.enterTextInTestId("tag-term-modal-input", text);
  cy.clickOptionWithTestId("tag-term-option");
  let btn_id = "add-tag-term-from-modal-btn";
  cy.clickOptionWithTestId(btn_id);
  cy.get(selectorWithtestId(btn_id)).should("not.exist");
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
