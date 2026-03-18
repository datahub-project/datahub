const test_id = Math.floor(Math.random() * 100000);
const platform_policy_name = `Platform test policy ${test_id}`;
const platform_policy_edited = `Platform test policy ${test_id} EDITED`;
const metadata_policy_name = `Metadata test policy ${test_id}`;
const metadata_policy_edited = `Metadata test policy ${test_id} EDITED`;

function waitForPoliciesPageReady() {
  cy.waitTextVisible("Manage Permissions");
  cy.get('[data-testid="search-bar-input"]').should("be.visible");
  cy.get("tbody tr").should("have.length.at.least", 1);
  cy.wait(1000);
}

function searchForPolicy(policyName) {
  cy.get('[data-testid="search-bar-input"]')
    .should("be.visible")
    .click()
    .clear()
    .type(policyName);
  cy.wait(500);
}

function openRowMenu(policyName) {
  cy.contains("tr", policyName)
    .find("button")
    .last()
    .should("not.be.disabled")
    .click();
  cy.get('[data-testid^="menu-item-"]').should("be.visible");
}

function clickMenuAction(actionText) {
  cy.get('[data-testid^="menu-item-"]')
    .contains(actionText)
    .should("be.visible")
    .click();
}

function clickFocusAndType(Id, text) {
  cy.get(`[data-testid="${Id}"]`).should("be.visible").click();
  cy.get(`[data-testid="${Id}"]`).should("be.visible").clear().type(text);
}

function updateAndSave(Id, groupName, text) {
  cy.clickOptionWithTestId(Id).type(groupName);
  cy.get(`[title='${text}']`).click();
  cy.document().then((doc) => {
    if (doc.activeElement && doc.activeElement !== doc.body) {
      doc.activeElement.blur();
    }
  });
}

function clickOnButton(saveButton) {
  cy.get(`#${saveButton}`).click();
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
  cy.contains("Successfully saved policy.").should("not.exist");
  searchForPolicy(policyName);
  cy.get("tbody").contains(policyName).should("be.visible");
}

function editPolicy(
  policyName,
  editPolicyName,
  description,
  policyEdited,
  visibleDiscription,
) {
  cy.visit("/settings/permissions/policies");
  waitForPoliciesPageReady();
  searchForPolicy(policyName);
  openRowMenu(policyName);
  clickMenuAction("Edit");
  cy.get('[data-testid="policy-name"]')
    .should("be.visible")
    .clear()
    .type(editPolicyName);
  cy.get('[data-testid="policy-description"]')
    .should("be.visible")
    .clear()
    .type(description);
  clickOnButton("nextButton");
  clickOnButton("nextButton");
  clickOnButton("saveButton");
  cy.waitTextVisible("Successfully saved policy.");
  cy.contains("Successfully saved policy.").should("not.exist");
  cy.waitTextVisible(policyEdited);
  cy.waitTextVisible(visibleDiscription);
}

function deletePolicy(policyEdited, deletePolicyTitle) {
  cy.visit("/settings/permissions/policies");
  waitForPoliciesPageReady();
  cy.get('[data-testid="policy-filter-base"]').should("be.visible").click();
  cy.get('[data-testid="option-ALL"]').should("be.visible").click();
  cy.wait(500);
  searchForPolicy(policyEdited);
  openRowMenu(policyEdited);
  clickMenuAction("Deactivate");
  cy.waitTextVisible("Successfully deactivated policy.");
  cy.contains("Successfully deactivated policy.").should("not.exist");
  cy.wait(500);
  openRowMenu(policyEdited);
  clickMenuAction("Activate");
  cy.waitTextVisible("Successfully activated policy.");
  cy.contains("Successfully activated policy.").should("not.exist");
  cy.wait(500);
  openRowMenu(policyEdited);
  clickMenuAction("Delete");
  cy.waitTextVisible(deletePolicyTitle);
  cy.clickOptionWithText("Yes");
  cy.waitTextVisible("Successfully removed policy.");
  cy.contains("Successfully removed policy.").should("not.exist");
  cy.ensureTextNotPresent(policyEdited);
}

describe("create and manage platform and metadata policies", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.visit("/settings/permissions/policies");
  });

  it("verify create, edit, delete platform policy", () => {
    waitForPoliciesPageReady();
    cy.get('[data-testid="add-policy-button"]').should("be.visible").click();
    cy.get('[data-testid="policy-name"]').should("be.visible");
    clickFocusAndType("policy-name", platform_policy_name);
    cy.get('[data-testid="policy-type"] [title="Metadata"]').click();
    cy.clickOptionWithTestId("platform");
    createPolicy(
      `Platform policy description ${test_id}`,
      platform_policy_name,
    );
    editPolicy(
      `${platform_policy_name}`,
      platform_policy_edited,
      `Platform policy description ${test_id} EDITED`,
      platform_policy_edited,
      `Platform policy description ${test_id} EDITED`,
    );
    deletePolicy(
      `${platform_policy_edited}`,
      `Delete ${platform_policy_edited}`,
    );
  });

  it("verify create, edit, delete metadata policy", () => {
    waitForPoliciesPageReady();
    cy.get('[data-testid="add-policy-button"]').should("be.visible").click();
    cy.get('[data-testid="policy-name"]').should("be.visible");
    clickFocusAndType("policy-name", metadata_policy_name);
    cy.get('[data-testid="policy-type"]').should("have.text", "Metadata");
    createPolicy(
      `Metadata policy description ${test_id}`,
      metadata_policy_name,
    );
    editPolicy(
      `${metadata_policy_name}`,
      metadata_policy_edited,
      `Metadata policy description ${test_id} EDITED`,
      metadata_policy_edited,
      `Metadata policy description ${test_id} EDITED`,
    );
    deletePolicy(
      `${metadata_policy_edited}`,
      `Delete ${metadata_policy_edited}`,
    );
  });
});
