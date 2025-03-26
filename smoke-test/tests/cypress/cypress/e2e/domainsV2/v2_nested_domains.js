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
  it("Verify Documentation tab by adding, editing Description, and adding a link", () => {
    const testRunId = `Test added ${Date.now()}`; // Generate a unique test run ID

    cy.clickOptionWithText(domainName);
    cy.clickOptionWithId("#rc-tabs-0-tab-Documentation");

    // Check if "Add Documentation" button is visible; otherwise, click "Edit Documentation"
    cy.get("body").then(($body) => {
      if ($body.find('[data-testid="add-documentation"]').length) {
        cy.get('[data-testid="add-documentation"]').click();
      } else {
        cy.get("button").contains("Edit").click();
      }
    });

    // Clear existing text and type the unique test run ID
    clearAndType(testRunId);
    cy.clickOptionWithTestId("description-editor-save-button");

    // Verify the unique documentation text is saved
    cy.waitTextVisible(testRunId);

    // Add a new link
    cy.clickFirstOptionWithTestId("add-link-button");
    cy.enterTextInTestId("add-link-modal-url", "www.test.com");
    cy.enterTextInTestId("add-link-modal-label", "Test Label");
    cy.clickOptionWithTestId("add-link-modal-add-button");

    // Verify link addition
    cy.waitTextVisible("Test Label");

    // Navigate back to ensure persistence
    cy.goToDomainList();
    cy.clickOptionWithText(domainName);
    cy.waitTextVisible(testRunId);

    // Reopen Documentation tab and clean up
    cy.clickOptionWithText("Documentation");
    clearAndDelete();
  });

  it("Verify Right Side Panel functionalities", () => {
    const testRunId = `Test documentation ${Date.now()}`;

    cy.clickOptionWithText(domainName);

    // Ensure "Edit Documentation" is visible before clicking
    cy.get('[data-testid="editDocumentation"]').click({ force: true });

    // Clear existing text and type the unique test run ID
    clearAndType(testRunId);
    cy.clickOptionWithTestId("description-editor-save-button");

    // Ensure "Add Documentation" is not present and "Edit Documentation" exists
    cy.ensureTextNotPresent("Add Documentation");

    // Confirm the saved description is visible
    cy.waitTextVisible(testRunId);

    // Add a new link
    cy.clickOptionWithTestId("add-link-button");
    cy.enterTextInTestId("add-link-modal-url", "www.test.com");
    cy.enterTextInTestId("add-link-modal-label", "Test Label");
    cy.clickOptionWithTestId("add-link-modal-add-button");

    // Verify link addition
    cy.waitTextVisible("Test Label");

    // Toggle sidebar (fix for visibility issue)
    cy.get('[data-testid="toggleSidebar"]')
      .should("exist")
      .should("be.visible")
      .scrollIntoView()
      .click();

    // Add an owner
    cy.clickOptionWithTestId("addOwner");
    cy.enterTextInTestId(
      "edit-owners-modal-find-actors-input",
      Cypress.env("ADMIN_DISPLAYNAME") || "DataHub",
    );

    // Select the admin user from the dropdown
    cy.get(".rc-virtual-list-holder-inner")
      .should("exist")
      .should("be.visible")
      .scrollIntoView()
      .click()
      .then(() => {
        cy.clickOptionWithText("Find a user or group");
        cy.clickOptionWithId("#addOwnerButton");
      });

    // Ensure the owner was added successfully
    cy.waitTextVisible(Cypress.env("ADMIN_DISPLAYNAME") || "DataHub");

    // Navigate away and back to confirm persistence
    cy.goToDomainList();
    cy.clickOptionWithText(domainName);
    cy.waitTextVisible(testRunId);
    cy.waitTextVisible(Cypress.env("ADMIN_DISPLAYNAME") || "DataHub");

    // Reopen Documentation tab and clean up
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
  it.only("Verify entities tab with adding and deleting assets and performing some actions", () => {
    cy.clickFirstOptionWithText("Marketing");
    cy.clickOptionWithText("Add to Assets");
    cy.waitTextVisible("Add assets to Domain");
    cy.enterTextInSpecificTestId("search-bar", 2, "Baz Chart 2");
    cy.get('[data-testid="preview-urn:li:chart:(looker,cypress_baz2)"]');
    cy.clickOptionWithSpecificClass(".ant-checkbox", 1);
    cy.clickOptionWithId("#continueButton");
    cy.waitTextVisible("Added assets to Domain!");
    cy.get('[data-node-key="Assets"]').click();

    cy.waitTextVisible("Filters");
    cy.get('[data-testid="search-results-edit-button"]', {
      timeout: 10000,
    }).click();
    cy.waitTextVisible("0 selected");
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
    cy.get('[data-testid="search-results-edit-button"]', {
      timeout: 10000,
    }).click();
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
