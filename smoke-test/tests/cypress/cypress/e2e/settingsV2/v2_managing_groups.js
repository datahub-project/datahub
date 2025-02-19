const test_id = Math.floor(Math.random() * 100000);
const username = `Example Name ${test_id}`;
const email = `example${test_id}@example.com`;
const password = "Example password";
const group_name = `Test group ${test_id}`;

describe("create and manage group", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.skipIntroducePage();
    cy.on("uncaught:exception", (err, runnable) => false);
  });

  it("add test user", () => {
    cy.visitWithLogin("/settings/identities/users");
    cy.waitTextVisible("Settings");
    cy.wait(3000);
    cy.clickOptionWithText("Invite Users");
    cy.waitTextVisible(/signup\?invite_token=\w{32}/).then(($elem) => {
      const inviteLink = $elem.text();
      cy.visit("/settings/identities/users");
      cy.logoutV2();
      cy.visit(inviteLink);
      cy.skipIntroducePage();
      cy.enterTextInTestId("email", email);
      cy.enterTextInTestId("name", username);
      cy.enterTextInTestId("password", password);
      cy.enterTextInTestId("confirmPassword", password);
      cy.mouseover("#title").click();
      cy.waitTextVisible("Other").click();
      cy.get("[type=submit]").click();
      cy.contains("Accepted invite!").should("not.exist");
      cy.wait(5000);
      cy.waitTextVisible(username);
    });
  });

  it("create a group", () => {
    cy.visitWithLogin("/settings/identities/groups");
    cy.waitTextVisible("Settings");
    cy.wait(1000);
    cy.clickOptionWithText("Create group");
    cy.waitTextVisible("Create new group");
    cy.get("#name").type(group_name);
    cy.get("#description").type("Test group description");
    cy.contains("Advanced").click();
    cy.waitTextVisible("Group Id");
    cy.get("#groupId").type(test_id);
    cy.get("#createGroupButton").click();
    // cy.waitTextVisible("Created group!");
    cy.waitTextVisible(group_name);
  });

  it("add test user to a group", () => {
    cy.visitWithLogin("/settings/identities/users");
    cy.waitTextVisible("Settings");
    cy.wait(1000);
    cy.get(".ant-tabs-tab-btn").contains("Groups").click();
    cy.clickOptionWithText(group_name);
    cy.clickTextOptionWithClass(".ant-typography", group_name);
    // cy.get(".ant-typography").contains(group_name).should("be.visible");
    cy.clickTextOptionWithClass(".ant-tabs-tab", "Members");
    // cy.get(".ant-tabs-tab").contains("Members").click();
    cy.waitTextVisible("No members in this group yet.");
    cy.clickOptionWithText("Add Member");
    // cy.clickOptionWithText('Search for users...')
    cy.contains("Search for users...").click({ force: true });
    cy.focused().type(username);
    // cy.clickOptionWithText('Add group members')
    // cy.clickTextOptionWithClass(".ant-select-item-option", username)
    cy.get(".ant-select-item-option").contains(username).click();
    cy.focused().blur();
    cy.contains(username).should("have.length", 1);
    cy.get('[role="dialog"] button').contains("Add").click({ force: true });
    cy.waitTextVisible("Group members added!");
    cy.contains(username, { timeout: 10000 }).should("be.visible");
  });

  it("update group info", () => {
    cy.visitWithLogin("/settings/identities/groups");
    cy.waitTextVisible("Settings");
    cy.wait(1000);
    cy.clickOptionWithText(group_name);
    cy.clickOptionWithSpecificClass(".ant-typography", 0);
    cy.clickOptionWithTestId("EditOutlinedIcon");
    cy.get("#email").type(`${test_id}@testemail.com`);
    cy.get("#slack").type(`#${test_id}`);
    cy.get(".ant-form-item-control-input-content")
      .eq(0)
      .clear()
      .type(`Test group EDITED ${test_id}{enter}`);
    cy.waitTextVisible("Name Updated");
    cy.waitTextVisible(`${test_id}@testemail.com`);
    cy.waitTextVisible(`#${test_id}`);
    cy.contains("Changes saved.").should("not.exist");
  });

  it("user verify to edit the discription", () => {
    cy.visitWithLogin("/settings/identities/groups");
    cy.waitTextVisible("Settings");
    cy.wait(1000);
    cy.contains(`Test group EDITED ${test_id}`).should("be.visible").click();
    cy.get('[data-testid="EditOutlinedIcon"]').eq(1).click();
    cy.contains("Test group description").should("be.visible").type(" EDITED");
    cy.clickOptionWithText("Test group");
    cy.get("body").click();
    cy.waitTextVisible("Changes saved.");
    cy.contains("Changes saved.").should("not.exist");
    cy.contains("Test group description EDITED").should("be.visible");
  });

  it("user verify to add the owner", () => {
    cy.visitWithLogin("/settings/identities/groups");
    cy.waitTextVisible("Settings");
    cy.wait(1000);
    cy.contains(`Test group EDITED ${test_id}`).should("be.visible").click();
    cy.get(".anticon.anticon-plus").click();
    cy.get('[aria-label="Close"]').should("be.visible");
    cy.get('[id="owner"]').click();
    cy.contains("Add Owners").click();
    cy.get('[id="owner"]').click();
    cy.focused().type(username);
    cy.get(`[data-testid="owner-${username}"]`).click();
    cy.focused().blur();
    cy.contains(username, { matchCase: false }).should("have.length", 1);
    cy.get('[role="dialog"] button').contains("Add").click();
    cy.waitTextVisible("Owners Added");
    cy.contains(username, { matchCase: false }).should("be.visible");
  });

  it("test User verify group participation", () => {
    cy.visitWithLogin("/settings/identities/groups");
    cy.waitTextVisible("Settings");
    cy.wait(1000);
    cy.hideOnboardingTour();
    cy.clickOptionWithText(`Test group EDITED ${test_id}`);
    cy.get(".ant-tabs-tab").contains("Members").click();
    cy.waitTextVisible(username);
  });

  it("remove group", () => {
    cy.visitWithLogin("/settings/identities/groups");
    cy.waitTextVisible("Settings");
    cy.wait(1000);
    cy.get(
      `[href="/group/urn:li:corpGroup:${test_id}"]`,
    ).openThreeDotDropdown();
    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    cy.waitTextVisible("Deleted Group!");
    cy.ensureTextNotPresent(`Test group EDITED ${test_id}`);
  });
});
