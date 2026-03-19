const test_id = Math.floor(Math.random() * 100000);
const username = `Example Name ${test_id}`;
const email = `example${test_id}@example.com`;
const password = "Example password";
const group_name = `Test group ${test_id}`;

describe("create and manage group", () => {
  beforeEach(() => {
    cy.skipIntroducePage();
    cy.on("uncaught:exception", (err, runnable) => false);
  });

  it("add test user", () => {
    cy.visitWithLogin("/settings/identities/users");
    cy.get('[data-testid="manage-users-groups-v2"]', { timeout: 10000 }).should(
      "be.visible",
    );
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
      cy.get('[data-testid="sign-up"]').click();
      cy.contains("Accepted invite!").should("not.exist");
      cy.wait(5000);
      cy.waitTextVisible(username);
    });
  });

  it("create a group", () => {
    cy.visitWithLogin("/settings/identities/groups");
    cy.get('[data-testid="manage-users-groups-v2"]', { timeout: 10000 }).should(
      "be.visible",
    );
    cy.clickOptionWithText("Create Group");
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
    cy.visitWithLogin("/settings/identities/groups");
    cy.get('[data-testid="manage-users-groups-v2"]', { timeout: 10000 }).should(
      "be.visible",
    );
    // Navigate to the group profile via its link in the Alchemy Table
    cy.get(`[href="/group/urn:li:corpGroup:${test_id}"]`, {
      timeout: 10000,
    })
      .should("be.visible")
      .click();
    cy.url().should("include", `/group/urn:li:corpGroup:${test_id}`);
    // Click on the Members tab and wait for it to load
    cy.get('[role="tab"]').contains("Members").click();

    // Wait for tab content to be fully loaded by looking for specific content
    cy.get("body").then(($body) => {
      if ($body.text().includes(Cypress.env("ADMIN_DISPLAYNAME"))) {
        cy.contains(Cypress.env("ADMIN_DISPLAYNAME"), {
          timeout: 10000,
        }).should("be.visible");
      } else {
        // Tab is loaded, continue
        cy.wait(1000); // Give it a moment to settle
      }
    });

    cy.get("button", { timeout: 10000 })
      .contains("Add Member")
      .should("be.visible")
      .click();
    cy.get('[role="dialog"]', { timeout: 10000 }).should("be.visible");
    cy.get('[data-testid="add-members-select"]', { timeout: 10000 }).should(
      "be.visible",
    );
    cy.get('[data-testid="add-members-select-base"]', { timeout: 10000 })
      .should("exist")
      .click({ force: true });
    cy.get('[data-testid="dropdown-search-input"]', { timeout: 10000 })
      .should("be.visible")
      .type(username);
    cy.get('[data-testid="add-members-select-dropdown"]', { timeout: 10000 })
      .contains(username)
      .click({ force: true });
    cy.get('[role="dialog"]').within(() => {
      cy.get("button").contains("Add").click({ force: true });
    });
    cy.waitTextVisible("Group members added!");
    cy.contains(username, { timeout: 10000 }).should("be.visible");
  });

  it("update group info", () => {
    cy.visitWithLogin("/settings/identities/groups");
    cy.get('[data-testid="manage-users-groups-v2"]', { timeout: 10000 }).should(
      "be.visible",
    );
    // Navigate to the group profile via its link in the Alchemy Table
    cy.get(`[href="/group/urn:li:corpGroup:${test_id}"]`, {
      timeout: 10000,
    })
      .should("be.visible")
      .click();
    cy.url().should("include", `/group/urn:li:corpGroup:${test_id}`);
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
    cy.get('[data-testid="manage-users-groups-v2"]', { timeout: 10000 }).should(
      "be.visible",
    );
    // Navigate to the group profile via its link in the Alchemy Table
    cy.get(`[href="/group/urn:li:corpGroup:${test_id}"]`, {
      timeout: 10000,
    })
      .should("be.visible")
      .click();
    cy.url().should("include", `/group/urn:li:corpGroup:${test_id}`);
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
    cy.get('[data-testid="manage-users-groups-v2"]', { timeout: 10000 }).should(
      "be.visible",
    );
    // Navigate to the group profile via its link in the Alchemy Table
    cy.get(`[href="/group/urn:li:corpGroup:${test_id}"]`, {
      timeout: 10000,
    })
      .should("be.visible")
      .click();
    cy.url().should("include", `/group/urn:li:corpGroup:${test_id}`);
    cy.get('[data-testid="add-owners-sidebar-button"]', { timeout: 10000 })
      .should("be.visible")
      .click();
    cy.get('[role="dialog"]', { timeout: 10000 }).should("be.visible");
    cy.get('[data-testid="add-owners-select"]', { timeout: 10000 }).should(
      "be.visible",
    );
    cy.get('[data-testid="add-owners-select-base"]', { timeout: 10000 })
      .should("exist")
      .click({ force: true });
    cy.get('[data-testid="dropdown-search-input"]', { timeout: 10000 })
      .should("be.visible")
      .type(username);
    cy.get('[data-testid="add-owners-select-dropdown"]', { timeout: 10000 })
      .contains(username)
      .click({ force: true });
    cy.get('[role="dialog"] button').contains("Add").click({ force: true });
    cy.waitTextVisible("Owners Added");
    cy.contains(username, { matchCase: false }).should("be.visible");
  });

  it("test User verify group participation", () => {
    cy.visitWithLogin("/settings/identities/groups");
    cy.get('[data-testid="manage-users-groups-v2"]', { timeout: 10000 }).should(
      "be.visible",
    );
    cy.hideOnboardingTour();

    // Navigate to the group profile via its link in the Alchemy Table
    cy.get(`[href="/group/urn:li:corpGroup:${test_id}"]`, {
      timeout: 10000,
    })
      .should("be.visible")
      .click();
    cy.url().should("include", `/group/urn:li:corpGroup:${test_id}`);

    // Wait for Members tab to be visible before clicking
    cy.get('[role="tab"]', { timeout: 10000 })
      .contains("Members")
      .should("be.visible")
      .click();

    // Verify the user is in the group with explicit timeout
    cy.contains(username, { timeout: 10000 }).should("be.visible");
  });

  it("remove group", () => {
    cy.visitWithLogin("/settings/identities/groups");
    cy.get('[data-testid="manage-users-groups-v2"]', { timeout: 10000 }).should(
      "be.visible",
    );

    // Wait for the group to be present, then open its actions menu
    cy.get(`[href="/group/urn:li:corpGroup:${test_id}"]`, {
      timeout: 10000,
    }).should("be.visible");
    cy.get(`[data-testid="group-menu-Test group EDITED ${test_id}"]`).click();
    cy.clickOptionWithText("Delete");
    cy.get('[role="dialog"] button').contains("Delete").click();
    cy.waitTextVisible(`Deleted Test group EDITED ${test_id}!`);
    cy.ensureTextNotPresent(`Test group EDITED ${test_id}`);
  });
});
