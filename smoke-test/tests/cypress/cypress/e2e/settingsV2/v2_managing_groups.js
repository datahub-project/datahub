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
    cy.get('[data-testid="manage-users-groups-v2"]', { timeout: 10000 }).should(
      "be.visible",
    );
    cy.get(".ant-tabs-tab-btn").contains("Groups").click();
    // Wait for groups tab to load and group to be visible
    cy.contains(group_name, { timeout: 10000 }).should("be.visible");
    cy.clickOptionWithText(group_name);
    cy.clickTextOptionWithClass(".ant-typography", group_name);
    // cy.get(".ant-typography").contains(group_name).should("be.visible");
    // Click on the Members tab and wait for it to load
    cy.get(".ant-tabs-tab").contains("Members").click();

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

    // Look for the Add Member button (it should be visible regardless of existing members)
    cy.get("button", { timeout: 10000 })
      .contains("Add Member")
      .should("be.visible")
      .click();
    // Wait for the modal to appear and ensure it's fully loaded
    cy.get('[role="dialog"]', { timeout: 10000 }).should("be.visible");

    // Try multiple approaches to interact with the user search dropdown
    cy.get("body").then(($body) => {
      // First try to find the ant-select component directly
      if ($body.find(".ant-select:not(.ant-select-disabled)").length > 0) {
        cy.get(".ant-select:not(.ant-select-disabled)")
          .first()
          .click({ force: true });
      } else {
        // Fallback: try to find the search input or placeholder
        cy.get('[role="dialog"]').within(() => {
          cy.get(
            '.ant-select-selector, .ant-select-selection-search-input, [placeholder*="user"]',
          )
            .first()
            .click({ force: true });
        });
      }
    });

    // Type the username into the search field (ensure we get only the visible one in the dialog)
    cy.get('[role="dialog"]').within(() => {
      cy.get('.ant-select-selection-search-input, input[role="combobox"]')
        .first()
        .should("be.visible")
        .type(username, { force: true });
    });

    // Wait for search results and select the user
    cy.get(".ant-select-item-option", { timeout: 10000 })
      .contains(username)
      .should("be.visible")
      .click();

    // Wait for dropdown to close and verify the user was selected
    cy.get('[role="dialog"]').within(() => {
      // Wait for the dropdown to close
      cy.get(".ant-select-dropdown", { timeout: 5000 }).should("not.exist");
      // Verify the user was selected
      cy.contains(username).should("be.visible");
      // Click the Add button with force to handle any lingering overlay issues
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
    cy.contains(group_name, { timeout: 10000 }).should("be.visible");
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
    cy.get('[data-testid="manage-users-groups-v2"]', { timeout: 10000 }).should(
      "be.visible",
    );
    cy.contains(`Test group EDITED ${test_id}`, { timeout: 10000 })
      .should("be.visible")
      .first()
      .click();
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
    cy.contains(`Test group EDITED ${test_id}`, { timeout: 10000 })
      .should("be.visible")
      .first()
      .click();
    cy.get('[data-testid="add-owners-sidebar-button"]', { timeout: 10000 })
      .should("be.visible")
      .click();
    cy.get('[aria-label="Close"]').should("be.visible");
    cy.get('[id="owner"]').click();
    cy.contains("Add Owners").click();
    cy.get('[id="owner"]').click();
    cy.focused().type(username);
    cy.get(`[data-testid="owner-option-${username}"]`, { timeout: 10000 })
      .should("be.visible")
      .click();
    cy.focused().blur();
    cy.contains(username, { matchCase: false }).should("have.length", 1);
    cy.get('[role="dialog"] button').contains("Add").click();
    cy.waitTextVisible("Owners Added");
    cy.contains(username, { matchCase: false }).should("be.visible");
  });

  it("test User verify group participation", () => {
    cy.visitWithLogin("/settings/identities/groups");
    cy.get('[data-testid="manage-users-groups-v2"]', { timeout: 10000 }).should(
      "be.visible",
    );
    cy.hideOnboardingTour();

    // Add debugging and robust existence check
    cy.get("body").then(($body) => {
      if ($body.text().includes(`Test group EDITED ${test_id}`)) {
        cy.log("Found group:", `Test group EDITED ${test_id}`);
      } else {
        cy.log(
          "Group not found in page, available text:",
          $body.text().substring(0, 500),
        );
      }
    });

    // Wait for and verify the group exists before clicking
    // Use first() to ensure we only click on one element if multiple exist
    cy.contains(`Test group EDITED ${test_id}`, { timeout: 10000 })
      .should("be.visible")
      .first()
      .click();

    // Wait for Members tab to be visible before clicking
    cy.get(".ant-tabs-tab", { timeout: 10000 })
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

    // Wait for the group link to be present before trying to interact
    cy.get(`[href="/group/urn:li:corpGroup:${test_id}"]`, { timeout: 10000 })
      .should("be.visible")
      .openThreeDotDropdown();

    cy.clickOptionWithText("Delete");
    cy.clickOptionWithText("Yes");
    cy.waitTextVisible("Deleted Group!");
    cy.ensureTextNotPresent(`Test group EDITED ${test_id}`);
  });
});
