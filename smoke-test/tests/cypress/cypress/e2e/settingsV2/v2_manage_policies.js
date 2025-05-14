const test_id = Math.floor(Math.random() * 100000);
const platform_policy_name = `Platform test policy ${test_id}`;
const platform_policy_edited = `Platform test policy ${test_id} EDITED`;
const metadata_policy_name = `Metadata test policy ${test_id}`;
const metadata_policy_edited = `Metadata test policy ${test_id} EDITED`;

function searchAndToggleMetadataPolicyStatus(metadataPolicyName, targetStatus) {
  cy.contains("Platform").should("be.visible");
  cy.get('[data-testid="search-input"]').should("be.visible");
  cy.get('[data-testid="search-input"]').eq(1).type(metadataPolicyName);
  cy.contains("tr", metadataPolicyName).as("metadataPolicyRow");
  cy.contains(targetStatus).click();
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
  cy.get('[data-testid="search-input"]').eq(1).type(policyName);
  cy.get(".ant-table-tbody").contains(policyName).should("be.visible");
}

function editPolicy(
  policyName,
  editPolicy,
  description,
  policyEdited,
  visibleDiscription,
) {
  cy.visit("/settings/permissions/policies");
  cy.waitTextVisible("Users & Groups");
  searchAndToggleMetadataPolicyStatus(policyName, "EDIT");
  cy.clickOptionWithTestId("policy-name");
  cy.focused().clear().type(editPolicy);
  cy.clickOptionWithTestId("policy-description");
  cy.focused().clear().type(description);
  clickOnButton("nextButton");
  clickOnButton("nextButton");
  clickOnButton("saveButton");
  cy.waitTextVisible("Successfully saved policy.");
  cy.contains("Successfully saved policy.").should("not.exist");
  cy.waitTextVisible(policyEdited);
  cy.waitTextVisible(visibleDiscription);
}

function deletePolicy(policyEdited, deletePolicy) {
  cy.visit("/settings/permissions/policies");
  cy.waitTextVisible("Users & Groups");
  cy.get(".ant-select-selection-item").should("be.visible").click();
  cy.get(".rc-virtual-list-holder-inner")
    .contains("All")
    .should("be.visible")
    .click();
  cy.get(".ant-select-selection-item").should("be.visible").click();
  searchAndToggleMetadataPolicyStatus(policyEdited, "DEACTIVATE");
  cy.waitTextVisible("Successfully deactivated policy.");
  cy.contains("DEACTIVATE").should("not.exist");
  cy.contains("ACTIVATE").click();
  cy.waitTextVisible("Successfully activated policy.");
  cy.get("[data-icon='delete']").click();
  cy.waitTextVisible(deletePolicy);
  cy.clickOptionWithText("Yes");
  cy.waitTextVisible("Successfully removed policy.");
  cy.waitTextVisible("Successfully removed policy.").should("not.exist");
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
    cy.waitTextVisible("Users & Groups");
    cy.clickOptionWithText("Create new policy");
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
      `${platform_policy_edited}`,
    );
  });

  it("verify create, edit, delete metadata policy", () => {
    cy.waitTextVisible("Users & Groups");
    cy.clickOptionWithText("Create new policy");
    clickFocusAndType("policy-name", metadata_policy_name);
    cy.get('[data-testid="policy-type"]').should("have.text", "Metadata");
    createPolicy(
      `Metadata policy description ${test_id}`,
      metadata_policy_name,
    );
    // cy.get('span[title="All"]').click()
    editPolicy(
      `${metadata_policy_name}`,
      metadata_policy_edited,
      `Metadata policy description ${test_id} EDITED`,
      metadata_policy_edited,
      `Metadata policy description ${test_id} EDITED`,
    );
    deletePolicy(
      `${metadata_policy_name}`,
      `Delete ${metadata_policy_name}`,
      `${metadata_policy_edited}`,
    );
  });
});
