const test_id = Math.floor(Math.random() * 100000);
const platform_policy_name = `Platform test policy ${test_id}`;
const platform_policy_edited = `Platform test policy ${test_id} EDITED`;
const metadata_policy_name = `Metadata test policy ${test_id}`;
const metadata_policy_edited = `Metadata test policy ${test_id} EDITED`;



function searchAndToggleMetadataPolicyStatus(metadataPolicyName, targetStatus) {
  cy.get('[data-testid="search-input"]').should('be.visible');
  cy.get('[data-testid="search-input"]').eq(1).type(metadataPolicyName);
  cy.contains('tr', metadataPolicyName).as('metadataPolicyRow');
  cy.contains(targetStatus).click();
}

function clickFocusAndType(Id, text) {
  cy.clickOptionWithTestId(Id)
    .focused().clear()
    .type(text);
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
  clickFocusAndType("policy-description", decription)
  clickOnButton("nextButton");
  updateAndSave("privileges", "All", "All Privileges", "nextButton")
  clickOnButton("nextButton");
  updateAndSave("users", "All", "All Users")
  updateAndSave("groups", "All", "All Groups")
  clickOnButton("saveButton");
  cy.waitTextVisible("Successfully saved policy.");
  cy.waitTextVisible(policyName);
}

function editPolicy(policyName, editPolicy, description, policyEdited, visibleDiscription) {
  searchAndToggleMetadataPolicyStatus(policyName, 'EDIT')
  cy.clickOptionWithTestId("policy-name")
  cy.focused().clear().type(editPolicy);
  cy.clickOptionWithTestId("policy-description");
  cy.focused().clear().type(description);
  clickOnButton("nextButton");
  clickOnButton("nextButton");
  clickOnButton("saveButton");
  cy.waitTextVisible("Successfully saved policy.");
  cy.waitTextVisible(policyEdited);
  cy.waitTextVisible(visibleDiscription);;
}

function deletePolicy(policyEdited, deletePolicy) {
  searchAndToggleMetadataPolicyStatus(policyEdited, 'DEACTIVATE')
  cy.waitTextVisible("Successfully deactivated policy.")
  cy.contains('DEACTIVATE').should('not.exist')
  cy.contains('ACTIVATE').click();
  cy.waitTextVisible("Successfully activated policy.")
  cy.get("[data-icon='delete']").click();
  cy.waitTextVisible(deletePolicy);
  cy.clickOptionWithText("Yes");
  cy.waitTextVisible("Successfully removed policy.");
  cy.ensureTextNotPresent(policyEdited);
}

describe("create and manage platform and metadata policies", () => {
  beforeEach(() => {
    cy.loginWithCredentials();
    cy.visit("/settings/permissions/policies");
  });

  it("create platform policy", () => {
    cy.waitTextVisible("Manage Permissions");
    cy.clickOptionWithText("Create new policy");
    clickFocusAndType("policy-name", platform_policy_name)
    cy.get('[data-testid="policy-type"] [title="Metadata"]').click();
    cy.clickOptionWithTestId("platform");
    createPolicy(`Platform policy description ${test_id}`, platform_policy_name)
  });

  it("edit platform policy", () => {
    editPolicy(`${platform_policy_name}`, platform_policy_edited,
      `Platform policy description ${test_id} EDITED`,
      platform_policy_edited, `Platform policy description ${test_id} EDITED`)
  });

  it("deactivate and activate platform policy", () => {
    deletePolicy(`${platform_policy_edited}`, `Delete ${platform_policy_edited}`, `${platform_policy_edited}`)
  });

  it("create metadata policy", () => {
    cy.clickOptionWithText("Create new policy");
    clickFocusAndType("policy-name", metadata_policy_name)
    cy.get('[data-testid="policy-type"]').should('have.text', 'Metadata');
    createPolicy(`Metadata policy description ${test_id}`, metadata_policy_name)
  });

  it("edit metadata policy", () => {
    editPolicy(`${metadata_policy_name}`, metadata_policy_edited,
      `Metadata policy description ${test_id} EDITED`,
      metadata_policy_edited, `Metadata policy description ${test_id} EDITED`)
  });

  it("deactivate and activate metadata policy", () => {
    deletePolicy(`${metadata_policy_name}`, `Delete ${metadata_policy_name}`, `${metadata_policy_edited}`)
  });

});



// const test_id = Math.floor(Math.random() * 100000);
// const platform_policy_name = `Platform test policy ${test_id}`;
// const platform_policy_edited = `Platform test policy ${test_id} EDITED`;
// const metadata_policy_name = `Metadata test policy ${test_id}`;
// const metadata_policy_edited = `Metadata test policy ${test_id} EDITED`;

// describe("create and manage platform and metadata policies", () => {

//     it("create platform policy", () => {
//         cy.loginWithCredentials();
//         cy.visit("/settings/permissions/policies");
//         cy.waitTextVisible("Manage Permissions");
//         cy.clickOptionWithText("Create new policy");
//         cy.clickOptionWithTestId("policy-name")
//           .focused()
//           .type(platform_policy_name);
//         cy.get('[data-testid="policy-type"] [title="Metadata"]').click();
//         cy.clickOptionWithTestId("platform");
//         cy.clickOptionWithTestId("policy-description")
//           .focused()
//           .type(`Platform policy description ${test_id}`);
//         cy.get("#nextButton").click();
//         cy.get('[data-testid="privileges"]').type("All");
//         cy.clickOptionWithText("All Privileges").focused().blur();
//         cy.get("#nextButton").click();
//         cy.get('[data-testid="users"]').type("All");
//         cy.get("[title='All Users']").click();
//         cy.focused().blur();
//         cy.get('[data-testid="groups"]').type("All");
//         cy.get("[title='All Groups']").click();
//         cy.focused().blur();
//         cy.get("#saveButton").click();
//         cy.waitTextVisible("Successfully saved policy.");
//         cy.waitTextVisible(platform_policy_name); 
//     });

//     it("edit platform policy", () => {
//         cy.loginWithCredentials();
//         cy.visit("/settings/permissions/policies");
//         cy.contains('tr', `${platform_policy_name}` )
//           .contains('EDIT')
//           .click();
//         cy.clickOptionWithTestId("policy-name");
//         cy.focused().clear().type(platform_policy_edited);
//         cy.clickOptionWithTestId("policy-description");
//         cy.focused().clear().type(`Platform policy description ${test_id} EDITED`);
//         cy.get("#nextButton").click();
//         cy.get("#nextButton").click();
//         cy.get("#saveButton").click();
//         cy.waitTextVisible("Successfully saved policy.");
//         cy.waitTextVisible(platform_policy_edited); 
//         cy.waitTextVisible(`Platform policy description ${test_id} EDITED`); 
//     });

//     it("deactivate and activate platform policy", () => {
//         cy.loginWithCredentials();
//         cy.visit("/settings/permissions/policies");
//         cy.contains('tr', `${platform_policy_edited}` )
//           .contains('DEACTIVATE')
//           .click();
//         cy.waitTextVisible("Successfully deactivated policy.")
//         cy.contains('tr', `${platform_policy_edited}` )
//           .contains('INACTIVE')
//           .should("be.visible");
//         cy.contains('tr', `${platform_policy_edited}` )
//           .contains('ACTIVATE')
//           .click();
//         cy.waitTextVisible("Successfully activated policy.")
//         cy.contains('tr', `${platform_policy_edited}` )
//           .contains('ACTIVE')
//           .should("be.visible");
//         cy.contains('tr', `${platform_policy_edited}` )
//           .find("[data-icon='delete']")
//           .click();
//         cy.waitTextVisible(`Delete ${platform_policy_edited}`);
//         cy.clickOptionWithText("Yes");
//         cy.waitTextVisible("Successfully removed policy.");
//         cy.ensureTextNotPresent(`${platform_policy_edited}`);

//     });
  
//     it("create metadata policy", () => {
//         cy.loginWithCredentials();
//         cy.visit("/settings/permissions/policies");
//         cy.clickOptionWithText("Create new policy");
//         cy.clickOptionWithTestId("policy-name")
//           .focused()
//           .type(metadata_policy_name);
//         cy.get('[data-testid="policy-type"]').should('have.text', 'Metadata');
//         cy.clickOptionWithTestId("policy-description")
//           .focused()
//           .type(`Metadata policy description ${test_id}`);
//         cy.get("#nextButton").click();
//         cy.get('[data-testid="privileges"]').type("All");
//         cy.clickOptionWithText("All Privileges").focused().blur();
//         cy.get("#nextButton").click();
//         cy.get('[data-testid="users"]').type("All");
//         cy.get("[title='All Users']").click();
//         cy.focused().blur();
//         cy.get('[data-testid="groups"]').type("All");
//         cy.get("[title='All Groups']").click();
//         cy.focused().blur();
//         cy.get("#saveButton").click();
//         cy.waitTextVisible("Successfully saved policy.");
//         cy.waitTextVisible(metadata_policy_name); 
//     });

//     it("edit metadata policy", () => {
//         cy.loginWithCredentials();
//         cy.visit("/settings/permissions/policies");
//         cy.contains('tr', `${metadata_policy_name}` )
//           .contains('EDIT')
//           .click();
//         cy.clickOptionWithTestId("policy-name")
//         cy.focused().clear().type(metadata_policy_edited);
//         cy.clickOptionWithTestId("policy-description");
//         cy.focused().clear().type(`Metadata policy description ${test_id} EDITED`);
//         cy.get("#nextButton").click();
//         cy.get("#nextButton").click();
//         cy.get("#saveButton").click();
//         cy.waitTextVisible("Successfully saved policy.");
//         cy.waitTextVisible(metadata_policy_edited); 
//         cy.waitTextVisible(`Metadata policy description ${test_id} EDITED`); 
//     });

//     it("deactivate and activate metadata policy", () => {
//         cy.loginWithCredentials();
//         cy.visit("/settings/permissions/policies");
//         cy.contains('tr', `${metadata_policy_edited}` )
//           .contains('DEACTIVATE')
//           .click();
//         cy.waitTextVisible("Successfully deactivated policy.")
//         cy.contains('tr', `${metadata_policy_edited}` )
//           .contains('INACTIVE')
//           .should("be.visible");
//         cy.contains('tr', `${metadata_policy_edited}` )
//           .contains('ACTIVATE')
//           .click();
//         cy.waitTextVisible("Successfully activated policy.")
//         cy.contains('tr', `${metadata_policy_edited}` )
//           .contains('ACTIVE')
//           .should("be.visible");
//         cy.contains('tr', `${metadata_policy_edited}` )
//           .find("[data-icon='delete']")
//           .click();
//         cy.waitTextVisible(`Delete ${metadata_policy_edited}`);
//         cy.clickOptionWithText("Yes");
//         cy.waitTextVisible("Successfully removed policy.");
//         cy.ensureTextNotPresent(`${metadata_policy_edited}`);
//     });
  
// });