const test_id = crypto.getRandomValues(new Uint32Array(1))[0];
const username = `Example Name ${test_id}`;
const email = `example${test_id}@example.com`;
const password = "Example password";
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
      cy.waitTextVisible("Welcome back");
      cy.hideOnboardingTour();
      cy.waitTextVisible(username);
    });
  });

  it("create a group", () => {
    cy.loginWithCredentials();
    cy.visit("/settings/identities/groups");
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
    cy.contains(username, { timeout: 10000 }).should("be.visible");
  });

  it("update group info", () => {
    cy.loginWithCredentials();
    cy.visit("/settings/identities/groups");
    cy.clickOptionWithText(group_name);
    cy.contains(group_name).find('[aria-label="Edit"]').click();
    cy.focused().clear().type(`Test group EDITED ${test_id}{enter}`);
    cy.waitTextVisible("Name Updated");
    cy.contains(`Test group EDITED ${test_id}`).should("be.visible");
    cy.get('[data-testid="edit-icon"]').click();
    cy.waitTextVisible("Edit Description");
    cy.get("#description").should("be.visible").type(" EDITED");
    cy.get("#updateGroupButton").click();
    cy.waitTextVisible("Changes saved.");
    cy.contains("Test group description EDITED").should("be.visible");
    cy.clickOptionWithText("Add Owners");
    cy.get('[id="owner"]').click({ force: true });
    cy.focused().type(username);
    cy.get(".ant-select-item-option")
      .contains(username, { matchCase: false })
      .click();
    cy.focused().blur();
    cy.contains(username, { matchCase: false }).should("have.length", 1);
    cy.get('[role="dialog"] button').contains("Done").click();
    cy.waitTextVisible("Owners Added");
    cy.contains(username, { matchCase: false }).should("be.visible");
    cy.clickOptionWithText("Edit Group");
    cy.waitTextVisible("Edit Profile");
    cy.get("#email").type(`${test_id}@testemail.com`);
    cy.get("#slack").type(`#${test_id}`);
    cy.clickOptionWithText("Save Changes");
    cy.waitTextVisible("Changes saved.");
    cy.waitTextVisible(`${test_id}@testemail.com`);
    cy.waitTextVisible(`#${test_id}`);
  });

  it("test User verify group participation", () => {
    cy.loginWithCredentials();
    cy.visit("/settings/identities/groups");
    cy.hideOnboardingTour();
    cy.clickOptionWithText(`Test group EDITED ${test_id}`);
    cy.get(".ant-tabs-tab").contains("Members").click();
    cy.waitTextVisible(username);
  });

  it("assign role to group ", () => {
    cy.loginWithCredentials();
    cy.visit("/settings/identities/groups");
    cy.get(`[href="/group/urn:li:corpGroup:${test_id}"]`).next().click();
    cy.get(".ant-select-item-option").contains("Admin").click();
    cy.get("button.ant-btn-primary").contains("OK").click();
    cy.get(`[href="/group/urn:li:corpGroup:${test_id}"]`).waitTextVisible(
      "Admin",
    );
  });

  it("remove group", () => {
    cy.loginWithCredentials();
    cy.visit("/settings/identities/groups");
    cy.get(
      `[href="/group/urn:li:corpGroup:${test_id}"]`,
    ).openThreeDotDropdown();
    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    cy.waitTextVisible("Deleted Group!");
    cy.ensureTextNotPresent(`Test group EDITED ${test_id}`);
  });
});
