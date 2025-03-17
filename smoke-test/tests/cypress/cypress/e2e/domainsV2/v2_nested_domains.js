const domainName = "CypressNestedDomain";
const handledResizeLoopErrors = () => {
  const resizeObserverLoopLimitErrRe = "ResizeObserver loop limit exceeded";
  const resizeObserverLoopErrRe =
    "ResizeObserver loop completed with undelivered notifications.";

  cy.on("uncaught:exception", (err) => {
    if (
      err.message.includes(resizeObserverLoopLimitErrRe) ||
      err.message.includes(resizeObserverLoopErrRe)
    ) {
      return false; // Prevent Cypress from failing the test on these specific errors
    }
  });
};

const createDomain = (domain) => {
  cy.get('[id="browse-v2"]').find('[type="button"]').first().click();
  cy.waitTextVisible("Create New Domain");
  cy.clickOptionWithText("Create New Domain");
  if (domain === "sub domain") {
    cy.get(".ant-select-selection-item").should("be.visible");
    cy.mouseover(".ant-select-selection-item");
    cy.get('[aria-label="close-circle"]').click();
  }
  cy.get('[data-testid="create-domain-name"]').click().type(domainName);
  cy.clickOptionWithTestId("create-domain-button");
  cy.waitTextVisible("Created domain!");
};

const moveDomaintoRootLevel = () => {
  cy.clickOptionWithText(domainName);
  cy.clickOptionWithTestId("entity-menu-move-button");
  cy.get('[data-testid="move-domain-modal"]')
    .contains("Marketing")
    .click({ force: true });
  cy.waitTextVisible("Marketing");
  cy.clickOptionWithTestId("move-domain-modal-move-button");
};

const checkSubDomainCreated = () => {
  cy.get('[data-testid="open-domain-item"]').click();
  cy.waitTextVisible(domainName);
};
const moveDomaintoParent = () => {
  checkSubDomainCreated();
  cy.clickOptionWithText(domainName);
  cy.waitTextVisible(domainName);
  cy.wait(1000); // wait 1 second while domain data is set in context, otherwise this can flake with no urn in DomainsContext yet
  cy.clickOptionWithTestId("entity-menu-move-button");
  cy.clickOptionWithText("Move To");
  cy.clickOptionWithTestId("move-domain-modal-move-button");
};

const getDomainList = (domainName) => {
  cy.contains("span.ant-typography-ellipsis", domainName)
    .parent('[data-testid="domain-list-item"]')
    .find('[aria-label="right"]')
    .click();
};

const deleteFromDomainDropdown = () => {
  cy.clickOptionWithTestId("entity-menu-delete-button");
  cy.waitTextVisible("Are you sure you want to remove this Domain?");
  cy.clickOptionWithText("Yes");
};

const deleteDomain = () => {
  cy.clickOptionWithText(domainName).waitTextVisible("Domains");
  deleteFromDomainDropdown();
};

const verifyEditAndPerformAddAndRemoveActionForDomain = (
  entity,
  action,
  text,
  body,
) => {
  cy.clickOptionWithText(entity);
  cy.clickOptionWithText(action);
  cy.get('[data-testid="tag-term-modal-input"]').type(text);
  cy.get('[data-testid="tag-term-option"]').contains(text).click();
  cy.clickOptionWithText(body);
  cy.get('[data-testid="add-tag-term-from-modal-btn"]').click();
  cy.waitTextVisible(text);
};

const clearAndType = (text) => {
  cy.get('[role="textbox"]').first().click().clear().type(text);
};

const clearAndDelete = () => {
  cy.clickFirstOptionWithTestId("edit-documentation-button");
  cy.get('[role="textbox"]').first().click().clear();
  cy.clickOptionWithTestId("description-editor-save-button");
  cy.waitTextVisible("No documentation");
  cy.mouseover(".ant-list-item-meta-content");
  cy.get(".ant-btn-circle").click();
  cy.waitTextVisible("Link Removed");
};

describe("Verify nested domains test functionalities", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.goToDomainList();
    cy.wait(2000);
    handledResizeLoopErrors();
  });
  it("Verify Create a new domain", () => {
    cy.waitTextVisible("Marketing");
    createDomain();
    cy.waitTextVisible("Domains");
  });

  it("verify Move domain root level to parent level", () => {
    cy.waitTextVisible(domainName);
    moveDomaintoRootLevel();
    cy.goToDomainList();
  });

  it("Verify Move domain parent level to root level", () => {
    moveDomaintoParent();
    cy.waitTextVisible("Moved Domain!");
    cy.goToDomainList();
    cy.waitTextVisible(domainName);
  });
  it("Verify Documentation tab by adding editing Description and adding link", () => {
    cy.clickOptionWithText(domainName);
    cy.clickOptionWithId("#rc-tabs-0-tab-Documentation");
    cy.clickFirstOptionWithText("Add Documentation");
    clearAndType("Test added");
    cy.clickOptionWithTestId("description-editor-save-button");
    cy.waitTextVisible("Description Updated");
    cy.get("Description Updated").should("not.exist");
    cy.waitTextVisible("Test added");
    cy.clickFirstOptionWithTestId("add-link-button");
    cy.enterTextInTestId("add-link-modal-url", "www.test.com");
    cy.enterTextInTestId("add-link-modal-label", "Test Label");
    cy.clickOptionWithTestId("add-link-modal-add-button");
    cy.waitTextVisible("Test Label");
    cy.goToDomainList();
    cy.clickOptionWithText(domainName);
    cy.waitTextVisible("Test added");
    cy.clickOptionWithText("Documentation");
    clearAndDelete();
  });
  it("Verify Right side panel functionalities", () => {
    cy.clickOptionWithText(domainName);
    cy.clickOptionWithTestId("editDocumentation");
    clearAndType("Test documentation");
    cy.clickOptionWithTestId("description-editor-save-button");
    cy.waitTextVisible("Description Updated");
    cy.ensureTextNotPresent("Add Documentation");
    cy.get("Description Updated").should("not.exist");
    cy.get('[data-testid="edit-documentation-button"]').should("be.visible");
    cy.waitTextVisible("Test documentation");
    cy.clickOptionWithTestId("add-link-button");
    cy.waitTextVisible("URL");
    cy.enterTextInTestId("add-link-modal-url", "www.test.com");
    cy.enterTextInTestId("add-link-modal-label", "Test Label");
    cy.clickOptionWithTestId("add-link-modal-add-button");
    cy.waitTextVisible("Link Added");
    cy.contains("Link Added").should("not.exist");
    cy.waitTextVisible("Test Label");
    cy.clickOptionWithTestId("toggleSidebar");
    cy.clickOptionWithTestId("addOwner");
    cy.waitTextVisible("Find a user or group");
    cy.clickTextOptionWithClass(
      ".rc-virtual-list-holder-inner",
      Cypress.env("ADMIN_DISPLAYNAME"),
    );
    cy.clickOptionWithText("Find a user or group");
    cy.clickOptionWithId("#addOwnerButton");
    cy.waitTextVisible(Cypress.env("ADMIN_DISPLAYNAME"));
    cy.goToDomainList();
    cy.clickOptionWithText(domainName);
    cy.waitTextVisible("Test documentation");
    cy.waitTextVisible(Cypress.env("ADMIN_DISPLAYNAME"));
    cy.clickOptionWithText(domainName);
    cy.clickOptionWithText("Documentation");
    clearAndDelete();
  });
  it("Verify Edit Domain Name", () => {
    cy.clickFirstOptionWithText(domainName);
    // edit name
    cy.get(".anticon-edit")
      .eq(0)
      .click()
      .then(() => {
        cy.get(".ant-typography-edit-content").type(" Edited").type("{enter}");
      });
    cy.waitTextVisible(`${domainName} Edited`);
  });
  it("Verify Remove the domain", () => {
    deleteDomain();
    cy.goToDomainList();
    cy.ensureTextNotPresent(domainName);
  });
  it("Verify Add and delete parent domain from parent domain", () => {
    cy.visit("domain/urn:li:domain:marketing/Documentation");
    cy.get('[aria-label="edit"]').should("be.visible");
    createDomain("sub domain");
    cy.ensureTextNotPresent("Created domain!");
    cy.goToDomainList();
    cy.get('[class^="ManageDomainsPageV2__PageWrapper"]').should(
      "contain.text",
      domainName,
    );
    cy.clickOptionWithText(domainName);
    deleteFromDomainDropdown();
    cy.ensureTextNotPresent(domainName);
  });
  it("Verify Add and delete sub domain", () => {
    cy.visit("domain/urn:li:domain:marketing/Documentation");
    cy.get('[aria-label="edit"]').should("be.visible");
    createDomain();
    cy.ensureTextNotPresent("Created domain!");
    cy.goToDomainList();
    checkSubDomainCreated();
    cy.should("contains.text", domainName);
    cy.clickOptionWithText(domainName);
    deleteFromDomainDropdown();
    cy.ensureTextNotPresent(domainName);
  });
  it("Verify entities tab with adding and deleting assets and performing some actions", () => {
    cy.clickFirstOptionWithText("Marketing");
    cy.clickOptionWithText("Add to Assets");
    cy.waitTextVisible("Add assets to Domain");
    cy.enterTextInSpecificTestId("search-bar", 2, "Baz Chart 2");
    cy.get('[data-testid="preview-urn:li:chart:(looker,cypress_baz2)"]');
    cy.clickOptionWithSpecificClass(".ant-checkbox", 1);
    cy.clickOptionWithId("#continueButton");
    cy.waitTextVisible("Added assets to Domain!");
    cy.get('[data-node-key="Assets"]').click();
    cy.clickOptionWithSpecificClass(".anticon.anticon-edit", 1);
    cy.clickOptionWithSpecificClass(".ant-checkbox", 1);
    verifyEditAndPerformAddAndRemoveActionForDomain(
      "Tags",
      "Add tags",
      "Cypress",
      "Add Tags",
    );
    cy.clickOptionWithText("Baz Chart 2");
    cy.waitTextVisible("Cypress");
    cy.waitTextVisible("Marketing");
    cy.go("back");
    cy.clickOptionWithSpecificClass(".anticon.anticon-edit", 1);
    cy.clickOptionWithSpecificClass(".ant-checkbox", 1);
    verifyEditAndPerformAddAndRemoveActionForDomain(
      "Tags",
      "Remove tags",
      "Cypress",
      "Remove Tags",
    );
    cy.clickTextOptionWithClass(".ant-dropdown-trigger", "Domain");
    cy.clickOptionWithText("Unset Domain");
    cy.clickOptionWithText("Yes");
    cy.clickOptionWithText("Baz Chart 2");
    cy.waitTextVisible("Dashboards");
    cy.reload();
    cy.ensureTextNotPresent("Cypress");
    cy.ensureTextNotPresent("Marketing");
  });
});
