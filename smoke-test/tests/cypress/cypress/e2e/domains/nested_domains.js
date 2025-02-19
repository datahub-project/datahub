const domainName = "CypressNestedDomain";

const createDomain = () => {
  cy.get(".anticon-plus").first().click();
  cy.waitTextVisible("Create New Domain");
  cy.get('[data-testid="create-domain-name"]').click().type(domainName);
  cy.clickOptionWithTestId("create-domain-button");
  cy.waitTextVisible("Created domain!");
};

const moveDomaintoRootLevel = () => {
  cy.clickOptionWithText(domainName);
  cy.openThreeDotDropdown();
  cy.clickOptionWithTestId("entity-menu-move-button");
  cy.get('[data-testid="move-domain-modal"]')
    .contains("Marketing")
    .click({ force: true });
  cy.waitTextVisible("Marketing");
  cy.clickOptionWithTestId("move-domain-modal-move-button");
};

const moveDomaintoParent = () => {
  cy.get('[data-testid="domain-list-item"]')
    .contains("Marketing")
    .prev()
    .click();
  cy.clickOptionWithText(domainName);
  cy.waitTextVisible(domainName);
  cy.openThreeDotDropdown();
  cy.waitTextVisible("Filters");
  cy.clickOptionWithTestId("entity-menu-move-button");
  cy.clickOptionWithTestId("move-domain-modal-move-button");
};

const getDomainList = (domainName) => {
  cy.contains("span.ant-typography-ellipsis", domainName)
    .parent('[data-testid="domain-list-item"]')
    .find('[aria-label="right"]')
    .click();
};

const deleteFromDomainDropdown = () => {
  cy.clickOptionWithText("Filters");
  cy.openThreeDotDropdown();
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
  cy.get('[role="textbox"]').click().clear().type(text);
};

const clearAndDelete = () => {
  cy.clickOptionWithText("Edit");
  cy.get('[role="textbox"]').click().clear();
  cy.clickOptionWithTestId("description-editor-save-button");
  cy.waitTextVisible("No documentation");
  cy.mouseover(".ant-list-item-meta-content");
  cy.get('[aria-label="delete"]').click().wait(1000);
  cy.get("button")
    .contains("span", "Yes")
    .should("be.visible")
    .click({ force: true });
  cy.waitTextVisible("Link Removed");
};

describe("Verify nested domains test functionalities", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(false);
    cy.loginWithCredentials();
    cy.goToDomainList();
    cy.ignoreResizeObserverLoop();
  });

  it("Verify Create a new domain", () => {
    createDomain();
    cy.waitTextVisible("Domains");
  });

  it("Verify Documentation tab by adding editing Description and adding link", () => {
    cy.clickOptionWithText(domainName);
    cy.clickOptionWithId("#rc-tabs-0-tab-Documentation");
    cy.clickFirstOptionWithText("Add Documentation");
    clearAndType("Test added");
    cy.clickOptionWithTestId("description-editor-save-button");
    cy.waitTextVisible("Description Updated");
    cy.waitTextVisible("Test added");
    cy.clickFirstOptionWithTestId("add-link-button");
    cy.waitTextVisible("Add Link");
    cy.enterTextInTestId("add-link-modal-url", "www.test.com");
    cy.enterTextInTestId("add-link-modal-label", "Test Label");
    cy.clickOptionWithTestId("add-link-modal-add-button");
    cy.waitTextVisible("Test Label");
    cy.goToDomainList();
    cy.waitTextVisible("Test added");
    cy.clickOptionWithText(domainName);
    cy.clickOptionWithText("Documentation");
    clearAndDelete();
  });

  it("Verify Right side panel functionalities", () => {
    cy.clickOptionWithText(domainName);
    cy.waitTextVisible("Filters");
    cy.get('[data-node-key="Documentation"]').click();
    cy.clickOptionWithText("Add Documentation");
    clearAndType("Test documentation");
    cy.clickOptionWithTestId("description-editor-save-button");
    cy.ensureTextNotPresent("Add Documentation");
    cy.waitTextVisible("Test documentation");
    cy.clickFirstOptionWithSpecificTestId("add-link-button", 1);
    cy.waitTextVisible("URL");
    cy.enterTextInTestId("add-link-modal-url", "www.test.com");
    cy.enterTextInTestId("add-link-modal-label", "Test Label");
    cy.clickOptionWithTestId("add-link-modal-add-button");
    cy.waitTextVisible("Test Label");
    cy.clickOptionWithTestId("add-owners-button");
    cy.waitTextVisible("Find a user or group");
    cy.clickTextOptionWithClass(
      ".rc-virtual-list-holder-inner",
      Cypress.env("ADMIN_DISPLAYNAME"),
    );
    cy.clickOptionWithText("Find a user or group");
    cy.clickOptionWithId("#addOwnerButton");
    cy.waitTextVisible(Cypress.env("ADMIN_DISPLAYNAME"));
    cy.goToDomainList();
    cy.waitTextVisible("Test documentation");
    cy.waitTextVisible(Cypress.env("ADMIN_DISPLAYNAME"));
    cy.clickOptionWithText(domainName);
    cy.clickOptionWithText("Documentation");
    clearAndDelete();
  });

  it("verify Move domain root level to parent level", () => {
    cy.waitTextVisible(domainName);
    moveDomaintoRootLevel();
    cy.waitTextVisible("Moved Domain!");
    cy.ensureTextNotPresent("Moved Domain!");
    cy.goToDomainList();
    cy.waitTextVisible("1 sub-domain");
  });

  it("Verify Move domain parent level to root level", () => {
    moveDomaintoParent();
    cy.waitTextVisible("Moved Domain!");
    cy.ensureTextNotPresent("Moved Domain!");
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
    cy.waitTextVisible("Test added");
    cy.clickFirstOptionWithTestId("add-link-button");
    cy.waitTextVisible("Add Link");
    cy.enterTextInTestId("add-link-modal-url", "www.test.com");
    cy.enterTextInTestId("add-link-modal-label", "Test Label");
    cy.clickOptionWithTestId("add-link-modal-add-button");
    cy.waitTextVisible("Test Label");
    cy.goToDomainList();
    cy.waitTextVisible("Test added");
    cy.clickOptionWithText(domainName);
    cy.clickOptionWithText("Documentation");
    clearAndDelete();
  });

  it("Verify Edit Domain Name", () => {
    cy.clickFirstOptionWithText(domainName);
    cy.clickOptionWithText("Filters");

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

  it("Verify Add and delete sub domain", () => {
    cy.clickFirstOptionWithText("Marketing");
    cy.clickOptionWithText("Filters");
    createDomain();
    cy.ensureTextNotPresent("Created domain!");
    getDomainList("Marketing");
    cy.clickOptionWithText(domainName);
    deleteFromDomainDropdown();
    cy.ensureTextNotPresent(domainName);
  });

  it("Verify entities tab with adding and deleting assets and performing some actions", () => {
    cy.clickFirstOptionWithText("Marketing");
    cy.clickOptionWithText("Add assets");
    cy.waitTextVisible("Add assets to Domain");
    cy.enterTextInSpecificTestId("search-bar", 3, "Baz Chart 1");
    cy.clickOptionWithSpecificClass(".ant-checkbox", 1);
    cy.clickOptionWithId("#continueButton");
    cy.waitTextVisible("Added assets to Domain!");
    cy.openThreeDotMenu();
    cy.clickOptionWithText("Edit");
    cy.clickOptionWithSpecificClass(".ant-checkbox", 1);
    verifyEditAndPerformAddAndRemoveActionForDomain(
      "Tags",
      "Add tags",
      "Cypress",
      "Add Tags",
    );
    cy.clickOptionWithText("Baz Chart 1");
    cy.waitTextVisible("Cypress");
    cy.waitTextVisible("Marketing");
    cy.go("back");
    cy.openThreeDotMenu();
    cy.clickOptionWithText("Edit");
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
    cy.clickOptionWithText("Baz Chart 1");
    cy.waitTextVisible("Dashboards");
    cy.reload();
    cy.ensureTextNotPresent("Cypress");
    cy.ensureTextNotPresent("Marketing");
  });
});
