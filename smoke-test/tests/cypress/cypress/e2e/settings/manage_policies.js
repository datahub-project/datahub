const test_id = Math.floor(Math.random() * 100000);
const platform_policy_name = `Platform test policy ${test_id}`;
const platform_policy_edited = `Platform test policy ${test_id} EDITED`;
const metadata_policy_name = `Metadata test policy ${test_id}`;
const metadata_policy_edited = `Metadata test policy ${test_id} EDITED`;

function searchForPolicy(metadataPolicyName) {
  cy.get('[data-testid="search-input"]').should("be.visible");
  cy.get('[data-testid="search-input"]').eq(1).type(metadataPolicyName);
  cy.get('[data-testid="search-input"]').eq(1).blur();
}

function searchAndToggleMetadataPolicyStatus(metadataPolicyName, targetStatus) {
  searchForPolicy(metadataPolicyName);
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
  searchAndToggleMetadataPolicyStatus(policyName, "EDIT");
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
  searchAndToggleMetadataPolicyStatus(policyEdited, "DEACTIVATE");
  cy.waitTextVisible("Successfully deactivated policy.");
  cy.contains("DEACTIVATE").should("not.exist");
  cy.contains("ACTIVATE").click();
  cy.waitTextVisible("Successfully activated policy.");
  cy.get("[data-icon='delete']").click();
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
    editPolicy(
      `${platform_policy_name}`,
      platform_policy_edited,
      `Platform policy description ${test_id} EDITED`,
      platform_policy_edited,
      `Platform policy description ${test_id} EDITED`,
    );
  });

  it("deactivate and activate platform policy", () => {
    deletePolicy(
      `${platform_policy_edited}`,
      `Delete ${platform_policy_edited}`,
      `${platform_policy_edited}`,
    );
  });

  it("create metadata policy", () => {
    cy.clickOptionWithText("Create new policy");
    clickFocusAndType("policy-name", metadata_policy_name);
    cy.get('[data-testid="policy-type"]').should("have.text", "Metadata");
    createPolicy(
      `Metadata policy description ${test_id}`,
      metadata_policy_name,
    );
  });

  it("edit metadata policy", () => {
    editPolicy(
      `${metadata_policy_name}`,
      metadata_policy_edited,
      `Metadata policy description ${test_id} EDITED`,
      metadata_policy_edited,
      `Metadata policy description ${test_id} EDITED`,
    );
  });

  it("deactivate and activate metadata policy", () => {
    deletePolicy(
      `${metadata_policy_name}`,
      `Delete ${metadata_policy_name}`,
      `${metadata_policy_edited}`,
    );
  });
});
