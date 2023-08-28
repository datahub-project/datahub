const test_id = Math.floor(Math.random() * 100000);
const username = `Example Name ${test_id}`;
const email = `example${test_id}@example.com`
const password = "Example password"
const group_name = `Test group ${test_id}`;

describe("create and manage group", () => {
    it("add test user", () => {
        cy.loginWithCredentials();
        cy.visit("/settings/identities/users");
        cy.waitTextVisible("Invite Users");
        cy.clickOptionWithText("Invite Users");
         cy.waitTextVisible(/signup\?invite_token=\w{32}/).then(($elem) => {
            const inviteLink = $elem.text();
            cy.visit("/settings/identities/users");
            cy.logout();
            cy.visit(inviteLink);
            cy.enterTextInTestId("email", email);
            cy.enterTextInTestId("name", username);
            cy.enterTextInTestId("password", password);
            cy.enterTextInTestId("confirmPassword", password);
            cy.mouseover("#title").click();
            cy.waitTextVisible("Other").click();
            cy.get("[type=submit]").click();
            cy.waitTextVisible("Welcome to DataHub");
            cy.hideOnboardingTour();
            cy.waitTextVisible(username);
        })
    });

    it("create a group", () => {
        cy.loginWithCredentials();
        cy.visit("/settings/identities/groups")
        cy.waitTextVisible("Create group");
        cy.clickOptionWithText("Create group");
        cy.waitTextVisible("Create new group");
        cy.get("#name").type(group_name);
        cy.get("#description").type("Test group description");
        cy.contains("Advanced").click();
        cy.waitTextVisible("Group Id");
        cy.get("#groupId").type(test_id);
        cy.get("#createGroupButton").click();
        cy.waitTextVisible("Created group!");
        cy.waitTextVisible(group_name);
    });

    it("add test user to a group", () => {
        cy.loginWithCredentials();
        cy.visit("/settings/identities/users");
        cy.get(".ant-tabs-tab-btn").contains("Groups").click();
        cy.clickOptionWithText(group_name);
        cy.get(".ant-typography").contains(group_name).should("be.visible");
        cy.get(".ant-tabs-tab").contains("Members").click();
        cy.waitTextVisible("No members in this group yet.");
        cy.clickOptionWithText("Add Member");
        cy.contains("Search for users...").click({ force: true });
        cy.focused().type(username);
        cy.get(".ant-select-item-option").contains(username).click();
        cy.focused().blur();
        cy.contains(username).should("have.length", 1);
        cy.get('[role="dialog"] button').contains("Add").click({ force: true });
        cy.waitTextVisible("Group members added!");
        cy.contains(username, {timeout: 10000}).should("be.visible");
    });

    it("update group info", () => {
        var expected_name = Cypress.env('ADMIN_USERNAME');
        cy.loginWithCredentials();
        cy.visit("/settings/identities/groups");
        cy.clickOptionWithText(group_name);
        cy.contains(group_name).find('[aria-label="Edit"]').click();
        cy.focused().clear().type(`Test group EDITED ${test_id}{enter}`);
        cy.waitTextVisible("Name Updated");
        cy.contains(`Test group EDITED ${test_id}`).should("be.visible");
        cy.contains("Test group description").find('[aria-label="edit"]').click();
        cy.focused().type(" EDITED{enter}");
        cy.waitTextVisible("Changes saved.");
        cy.contains("Test group description EDITED").should("be.visible");
        cy.clickOptionWithText("Add Owners");
        cy.contains("Search for users or groups...").click({ force: true });
        cy.focused().type(expected_name);
        cy.get(".ant-select-item-option").contains(expected_name, { matchCase: false }).click();
        cy.focused().blur();
        cy.contains(expected_name).should("have.length", 1);
        cy.get('[role="dialog"] button').contains("Done").click();
        cy.waitTextVisible("Owners Added");
        cy.contains(expected_name, { matchCase: false }).should("be.visible");
        cy.clickOptionWithText("Edit Group");
        cy.waitTextVisible("Edit Profile");
        cy.get("#email").type(`${test_id}@testemail.com`);
        cy.get("#slack").type(`#${test_id}`);
        cy.clickOptionWithText("Save Changes");
        cy.waitTextVisible("Changes saved.");
        cy.waitTextVisible(`${test_id}@testemail.com`);
        cy.waitTextVisible(`#${test_id}`);
    });

    it("test user verify group participation", () => {
        cy.loginWithCredentials(email,password);
        cy.visit("/settings/identities/groups");
        cy.hideOnboardingTour();
        cy.clickOptionWithText(`Test group EDITED ${test_id}`);
        cy.get(".ant-tabs-tab").contains("Members").click();
        cy.waitTextVisible(username);  
    });

    it("remove group", () => {
        cy.loginWithCredentials();
        cy.visit("/settings/identities/groups");
        cy.get(`[href="/group/urn:li:corpGroup:${test_id}"]`).next().click();
        cy.clickOptionWithText("Delete");
        cy.clickOptionWithText("Yes");
        cy.waitTextVisible("Deleted Group!"); 
        cy.ensureTextNotPresent(`Test group EDITED ${test_id}`);
    });

});