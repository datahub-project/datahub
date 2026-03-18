const test_id = Math.floor(Math.random() * 100000);
const platform_policy_name = `Platform test policy ${test_id}`;
const platform_policy_edited = `Platform test policy ${test_id} EDITED`;
const metadata_policy_name = `Metadata test policy ${test_id}`;
const metadata_policy_edited = `Metadata test policy ${test_id} EDITED`;

function searchForPolicy(policyName) {
  cy.wait(1000);
  cy.get('[data-testid="search-bar-input"]').should("be.visible");
  cy.get('[data-testid="search-bar-input"]').clear().type(policyName);
  cy.get('[data-testid="search-bar-input"]').blur();
}

function openRowMenu(policyName) {
  cy.contains("tr", policyName).find("button").last().click({ force: true });
  cy.wait(300);
}

function clickMenuAction(actionText) {
  cy.get('[data-testid^="menu-item-"]').contains(actionText).click();
}

function clickFocusAndType(Id, text) {
  cy.clickOptionWithTestId(Id).focused().clear().type(text);
}

function updateAndSave(Id, groupName, text) {
  cy.clickOptionWithTestId(Id).type(groupName);
  cy.get(`[title='${text}']`).click();
  cy.focused().blur();
}

function clickOnButton(saveButton) {
  cy.get(`#${saveButton}`).click();
}

function changeFilterToAll() {
  cy.get('[data-testid="policy-filter"]').click();
  cy.get('[data-testid="option-ALL"]').click();
  cy.wait(1000);
}

function createPolicy(decription, policyName) {
  clickFocusAndType("policy-description", decription);
  clickOnButton("nextButton");
  updateAndSave("privileges", "All", "All Privileges", "nextButton");
  clickOnButton("nextButton");
  updateAndSave("users", "All", "All Users");
  updateAndSave("groups", "All", "All Groups");
  clickOnButton("saveButton");
  cy.waitTextVisible("Successfully saved policy.");
  searchForPolicy(policyName);
  cy.waitTextVisible(policyName);
}

function editPolicy(
  policyName,
  editPolicy,
  description,
  policyEdited,
  visibleDiscription,
) {
  searchForPolicy(policyName);
  openRowMenu(policyName);
  clickMenuAction("Edit");
  cy.clickOptionWithTestId("policy-name");
  cy.focused().clear().type(editPolicy);
  cy.clickOptionWithTestId("policy-description");
  cy.focused().clear().type(description);
  clickOnButton("nextButton");
  clickOnButton("nextButton");
  clickOnButton("saveButton");
  cy.waitTextVisible("Successfully saved policy.");
  cy.waitTextVisible(policyEdited);
  cy.waitTextVisible(visibleDiscription);
}

function deletePolicy(policyEdited, deletePolicy) {
  searchForPolicy(policyEdited);
  openRowMenu(policyEdited);
  clickMenuAction("Deactivate");
  cy.waitTextVisible("Successfully deactivated policy.");
  openRowMenu(policyEdited);
  clickMenuAction("Activate");
  cy.waitTextVisible("Successfully activated policy.");
  openRowMenu(policyEdited);
  clickMenuAction("Delete");
  cy.waitTextVisible(deletePolicy);
  cy.clickOptionWithText("Yes");
  cy.waitTextVisible("Successfully removed policy.");
  cy.ensureTextNotPresent(policyEdited);
}

describe("create and manage platform and metadata policies", () => {
  beforeEach(() => {
    cy.visitWithLogin("/settings/permissions/policies");
  });

  it("create platform policy", () => {
    cy.waitTextVisible("Manage Permissions");
    changeFilterToAll();
    cy.clickOptionWithText("Create new policy");
    clickFocusAndType("policy-name", platform_policy_name);
    cy.get('[data-testid="policy-type"] [title="Metadata"]').click();
    cy.clickOptionWithTestId("platform");
    createPolicy(
      `Platform policy description ${test_id}`,
      platform_policy_name,
    );
  });

  it("edit platform policy", () => {
    changeFilterToAll();
    editPolicy(
      `${platform_policy_name}`,
      platform_policy_edited,
      `Platform policy description ${test_id} EDITED`,
      platform_policy_edited,
      `Platform policy description ${test_id} EDITED`,
    );
  });

  it("deactivate and activate platform policy", () => {
    changeFilterToAll();
    deletePolicy(
      `${platform_policy_edited}`,
      `Delete ${platform_policy_edited}`,
      `${platform_policy_edited}`,
    );
  });

  it("create metadata policy", () => {
    changeFilterToAll();
    cy.clickOptionWithText("Create new policy");
    clickFocusAndType("policy-name", metadata_policy_name);
    cy.get('[data-testid="policy-type"]').should("have.text", "Metadata");
    createPolicy(
      `Metadata policy description ${test_id}`,
      metadata_policy_name,
    );
  });

  it("edit metadata policy", () => {
    changeFilterToAll();
    editPolicy(
      `${metadata_policy_name}`,
      metadata_policy_edited,
      `Metadata policy description ${test_id} EDITED`,
      metadata_policy_edited,
      `Metadata policy description ${test_id} EDITED`,
    );
  });

  it("deactivate and activate metadata policy", () => {
    changeFilterToAll();
    deletePolicy(
      `${metadata_policy_name}`,
      `Delete ${metadata_policy_name}`,
      `${metadata_policy_edited}`,
    );
  });
});
