const test_id = Math.floor(Math.random() * 100000);
const username = `Example Name ${test_id}`;
const email = `example${test_id}@example.com`;
const password = "Example password";
const group_name = `Test group ${test_id}`;

const addOwner = (owner, type, elementId) => {
    cy.clickOptionWithText("Add Owners");
    cy.contains("Search for users or groups...").click({ force: true });
    cy.focused().type(owner);
    cy.clickOptionWithText(owner);
    cy.focused().blur();
    cy.waitTextVisible(owner);
    cy.get('[role="dialog"]').contains("Technical Owner").click();
    cy.get('[role="listbox"]').parent().contains(type).click();
    cy.get('[role="dialog"]').contains(type).should("be.visible");
    cy.clickOptionWithText("Done");
    cy.waitTextVisible("Owners Added");
    cy.waitTextVisible(type);
    cy.waitTextVisible(owner).wait(3000);
    cy.clickOptionWithText(owner);
    cy.waitTextVisible("SampleCypressHiveDataset");
    cy.goToDataset("urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)", "SampleCypressHiveDataset");
    cy.get(elementId).next().click();
    cy.clickOptionWithText("Yes");
    cy.waitTextVisible("Owner Removed");
    cy.ensureTextNotPresent(owner);
    cy.ensureTextNotPresent(type);
}

describe("add, remove ownership for dataset", () => {
    it("create test user and test group, add user to a group", () => {
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
            cy.logout();
        })
        cy.loginWithCredentials();
        cy.visit("/settings/identities/groups")
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
        cy.clickOptionWithText(group_name);
        cy.contains(group_name).should("be.visible");
        cy.get('[role="tab"]').contains("Members").click();
        cy.waitTextVisible("No members in this group yet.");
        cy.clickOptionWithText("Add Member");
        cy.contains("Search for users...").click({ force: true });
        cy.focused().type(username);
        cy.contains(username).click();
        cy.focused().blur();
        cy.contains(username).should("have.length", 1);
        cy.get('[role="dialog"] button').contains("Add").click({ force: true });
        cy.waitTextVisible("Group members added!");
        cy.contains(username, {timeout: 10000}).should("be.visible");
    });

    it("open test dataset page, add and remove user ownership(test every type)", () => {
        cy.loginWithCredentials();
        cy.goToDataset("urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)", "SampleCypressHiveDataset");
        //business owner
        addOwner(username, "Business Owner", `[href="/user/urn:li:corpuser:example${test_id}@example.com"]`);
        //data steward
        addOwner(username, "Data Steward", `[href="/user/urn:li:corpuser:example${test_id}@example.com"]`);
        //none
        addOwner(username, "None", `[href="/user/urn:li:corpuser:example${test_id}@example.com"]`);
        //technical owner
        addOwner(username, "Technical Owner", `[href="/user/urn:li:corpuser:example${test_id}@example.com"]`);
    });

    it("open test dataset page, add and remove group ownership(test every type)", () => {
        cy.loginWithCredentials();
        cy.goToDataset("urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)", "SampleCypressHiveDataset");
        //business owner
        addOwner(group_name, "Business Owner", `[href="/group/urn:li:corpGroup:${test_id}"]`);
        //data steward
        addOwner(group_name, "Data Steward", `[href="/group/urn:li:corpGroup:${test_id}"]`);
        //none
        addOwner(group_name, "None", `[href="/group/urn:li:corpGroup:${test_id}"]`);
        //technical owner
        addOwner(group_name, "Technical Owner", `[href="/group/urn:li:corpGroup:${test_id}"]`);
    });
});