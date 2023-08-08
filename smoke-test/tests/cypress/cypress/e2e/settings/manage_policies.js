const test_id = Math.floor(Math.random() * 100000);
const platform_policy_name = `Platform test policy ${test_id}`;
const platform_policy_edited = `Platform test policy ${test_id} EDITED`;
const metadata_policy_name = `Metadata test policy ${test_id}`;
const metadata_policy_edited = `Metadata test policy ${test_id} EDITED`;

describe("create and manage platform and metadata policies", () => {

    it("create platform policy", () => {
        cy.loginWithCredentials();
        cy.visit("/settings/permissions/policies");
        cy.waitTextVisible("Manage Permissions");
        cy.clickOptionWithText("Create new policy");
        cy.clickOptionWithTestId("policy-name")
          .focused()
          .type(platform_policy_name);
        cy.get('[data-testid="policy-type"] [title="Metadata"]').click();
        cy.clickOptionWithTestId("platform");
        cy.clickOptionWithTestId("policy-description")
          .focused()
          .type(`Platform policy description ${test_id}`);
        cy.get("#nextButton").click();
        cy.get('[data-testid="privileges"]').type("All");
        cy.clickOptionWithText("All Privileges").focused().blur();
        cy.get("#nextButton").click();
        cy.get('[data-testid="users"]').type("All");
        cy.get("[title='All Users']").click();
        cy.focused().blur();
        cy.get('[data-testid="groups"]').type("All");
        cy.get("[title='All Groups']").click();
        cy.focused().blur();
        cy.get("#saveButton").click();
        cy.waitTextVisible("Successfully saved policy.");
        cy.waitTextVisible(platform_policy_name); 
    });

    it("edit platform policy", () => {
        cy.loginWithCredentials();
        cy.visit("/settings/permissions/policies");
        cy.contains('tr', `${platform_policy_name}` )
          .contains('EDIT')
          .click();
        cy.clickOptionWithTestId("policy-name");
        cy.focused().clear().type(platform_policy_edited);
        cy.clickOptionWithTestId("policy-description");
        cy.focused().clear().type(`Platform policy description ${test_id} EDITED`);
        cy.get("#nextButton").click();
        cy.get("#nextButton").click();
        cy.get("#saveButton").click();
        cy.waitTextVisible("Successfully saved policy.");
        cy.waitTextVisible(platform_policy_edited); 
        cy.waitTextVisible(`Platform policy description ${test_id} EDITED`); 
    });

    it("deactivate and activate platform policy", () => {
        cy.loginWithCredentials();
        cy.visit("/settings/permissions/policies");
        cy.contains('tr', `${platform_policy_edited}` )
          .contains('DEACTIVATE')
          .click();
        cy.waitTextVisible("Successfully deactivated policy.")
        cy.contains('tr', `${platform_policy_edited}` )
          .contains('INACTIVE')
          .should("be.visible");
        cy.contains('tr', `${platform_policy_edited}` )
          .contains('ACTIVATE')
          .click();
        cy.waitTextVisible("Successfully activated policy.")
        cy.contains('tr', `${platform_policy_edited}` )
          .contains('ACTIVE')
          .should("be.visible");
        cy.contains('tr', `${platform_policy_edited}` )
          .find("[data-icon='delete']")
          .click();
        cy.waitTextVisible(`Delete ${platform_policy_edited}`);
        cy.clickOptionWithText("Yes");
        cy.waitTextVisible("Successfully removed policy.");
        cy.ensureTextNotPresent(`${platform_policy_edited}`);

    });
  
    it("create metadata policy", () => {
        cy.loginWithCredentials();
        cy.visit("/settings/permissions/policies");
        cy.clickOptionWithText("Create new policy");
        cy.clickOptionWithTestId("policy-name")
          .focused()
          .type(metadata_policy_name);
        cy.get('[data-testid="policy-type"]').should('have.text', 'Metadata');
        cy.clickOptionWithTestId("policy-description")
          .focused()
          .type(`Metadata policy description ${test_id}`);
        cy.get("#nextButton").click();
        cy.get('[data-testid="privileges"]').type("All");
        cy.clickOptionWithText("All Privileges").focused().blur();
        cy.get("#nextButton").click();
        cy.get('[data-testid="users"]').type("All");
        cy.get("[title='All Users']").click();
        cy.focused().blur();
        cy.get('[data-testid="groups"]').type("All");
        cy.get("[title='All Groups']").click();
        cy.focused().blur();
        cy.get("#saveButton").click();
        cy.waitTextVisible("Successfully saved policy.");
        cy.waitTextVisible(metadata_policy_name); 
    });

    it("edit metadata policy", () => {
        cy.loginWithCredentials();
        cy.visit("/settings/permissions/policies");
        cy.contains('tr', `${metadata_policy_name}` )
          .contains('EDIT')
          .click();
        cy.clickOptionWithTestId("policy-name")
        cy.focused().clear().type(metadata_policy_edited);
        cy.clickOptionWithTestId("policy-description");
        cy.focused().clear().type(`Metadata policy description ${test_id} EDITED`);
        cy.get("#nextButton").click();
        cy.get("#nextButton").click();
        cy.get("#saveButton").click();
        cy.waitTextVisible("Successfully saved policy.");
        cy.waitTextVisible(metadata_policy_edited); 
        cy.waitTextVisible(`Metadata policy description ${test_id} EDITED`); 
    });

    it("deactivate and activate metadata policy", () => {
        cy.loginWithCredentials();
        cy.visit("/settings/permissions/policies");
        cy.contains('tr', `${metadata_policy_edited}` )
          .contains('DEACTIVATE')
          .click();
        cy.waitTextVisible("Successfully deactivated policy.")
        cy.contains('tr', `${metadata_policy_edited}` )
          .contains('INACTIVE')
          .should("be.visible");
        cy.contains('tr', `${metadata_policy_edited}` )
          .contains('ACTIVATE')
          .click();
        cy.waitTextVisible("Successfully activated policy.")
        cy.contains('tr', `${metadata_policy_edited}` )
          .contains('ACTIVE')
          .should("be.visible");
        cy.contains('tr', `${metadata_policy_edited}` )
          .find("[data-icon='delete']")
          .click();
        cy.waitTextVisible(`Delete ${metadata_policy_edited}`);
        cy.clickOptionWithText("Yes");
        cy.waitTextVisible("Successfully removed policy.");
        cy.ensureTextNotPresent(`${metadata_policy_edited}`);
    });
  
});