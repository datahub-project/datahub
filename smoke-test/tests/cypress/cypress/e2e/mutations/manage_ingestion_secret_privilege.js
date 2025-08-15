const test_id = Math.floor(Math.random() * 100000);
const platform_policy_name = `Platform test policy ${test_id}`;
const number = Math.floor(Math.random() * 100000);
const name = `Example Name ${number}`;
const email = `example${number}@example.com`;

const tryToSignUp = () => {
  cy.enterTextInTestId("email", email);
  cy.enterTextInTestId("name", name);
  cy.enterTextInTestId("password", "Example password");
  cy.enterTextInTestId("confirmPassword", "Example password");
  cy.mouseover("#title").click();
  cy.waitTextVisible("Other").click();
  cy.clickOptionWithId("[type=submit]");
  return { name, email };
};

const signIn = () => {
  cy.visit("/login");
  cy.enterTextInTestId("username", email);
  cy.enterTextInTestId("password", "Example password");
  cy.clickOptionWithId("[type=submit]");
};

const updateAndSave = (Id, groupName, text) => {
  cy.clickOptionWithTestId(Id).type(groupName);
  cy.get(".rc-virtual-list").contains(text).click({ force: true });
  cy.focused().blur();
};

const clickFocusAndType = (Id, text) => {
  cy.clickOptionWithTestId(Id).focused().clear().type(text);
};

const clickOnButton = (saveButton) => {
  cy.clickOptionWithId(`#${saveButton}`);
};

const createPolicy = (description, policyName) => {
  clickFocusAndType("policy-description", description);
  clickOnButton("nextButton");
  updateAndSave("privileges", "Ingestion", "Manage Metadata Ingestion");
  cy.wait(1000);
  clickOnButton("nextButton");
  updateAndSave("users", "All", "All Users");
  clickOnButton("saveButton");
  cy.waitTextVisible("Successfully saved policy.");
  cy.ensureTextNotPresent("Successfully saved policy.");
  cy.reload();
  searchAndToggleMetadataPolicyStatus(policyName);
  cy.get(".ant-table-row-level-0").contains(policyName);
};

const searchAndToggleMetadataPolicyStatus = (metadataPolicyName) => {
  cy.get('[data-testid="search-input"]').should("be.visible");
  cy.get('[data-testid="search-input"]').last().type(metadataPolicyName);
};

const editPolicy = (policyName, type, select) => {
  searchAndToggleMetadataPolicyStatus(policyName);
  cy.contains("tr", policyName).as("metadataPolicyRow");
  cy.contains("EDIT").click();
  clickOnButton("nextButton");
  cy.clickOptionWithId(".ant-tag-close-icon");
  updateAndSave("privileges", type, select);
  clickOnButton("nextButton");
  cy.clickOptionWithId(".ant-tag-close-icon");
  updateAndSave("users", name, name);
  clickOnButton("saveButton");
  cy.waitTextVisible("Successfully saved policy.");
};

const deactivateExistingAllUserPolicies = () => {
  cy.get(".ant-pagination li")
    .its("length")
    .then((len) => {
      const pageCount = len - 2;
      for (let page = 1; page <= pageCount; page++) {
        cy.get("tbody tr td").should("be.visible");
        cy.get("tbody tr").each(($row) => {
          cy.wrap($row)
            .find("td")
            .eq(3)
            .invoke("text")
            .then((role) => {
              if (role === "All Users") {
                cy.wrap($row)
                  .find("td")
                  .eq(5)
                  .find("div button")
                  .eq(1)
                  .invoke("text")
                  .then((buttonText) => {
                    if (buttonText === "DEACTIVATE") {
                      cy.wrap($row)
                        .find("td")
                        .eq(5)
                        .find("div button")
                        .eq(1)
                        .click();
                      cy.waitTextVisible("Successfully deactivated policy.");
                    }
                  });
              }
            });
        });
        if (page < pageCount) {
          cy.contains("li", `${page + 1}`).click();
          cy.ensureTextNotPresent("No Policies");
        }
      }
    });
};

describe("Manage Ingestion and Secret Privileges", () => {
  let registeredEmail = "";
  it("create Metadata Ingestion platform policy and assign privileges to all users", () => {
    cy.loginWithCredentials();
    cy.visit("/settings/permissions/policies");
    cy.waitTextVisible("Manage Permissions");
    cy.get(".ant-select-selection-item").should("be.visible").click();
    cy.get(".ant-select-item-option-content").contains("All").click();
    cy.get('[data-icon="delete"]').should("be.visible");
    deactivateExistingAllUserPolicies();
    cy.reload();
    cy.clickOptionWithText("Create new policy");
    clickFocusAndType("policy-name", platform_policy_name);
    cy.clickOptionWithId('[data-testid="policy-type"] [title="Metadata"]');
    cy.clickOptionWithTestId("platform");
    createPolicy(
      `Platform policy description ${test_id}`,
      platform_policy_name,
    );
    cy.logout();
  });

  it("Create user and verify ingestion tab not present", () => {
    cy.loginWithCredentials();
    cy.visit("/settings/identities/users");
    cy.waitTextVisible("Invite Users");
    cy.clickOptionWithText("Invite Users");
    cy.waitTextVisible(/signup\?invite_token=\w{32}/).then(($elem) => {
      const inviteLink = $elem.text();
      cy.log(inviteLink);
      cy.visit("/settings/identities/users");
      cy.logout();
      cy.visit(inviteLink);
      const { name, email } = tryToSignUp();
      registeredEmail = email;
      cy.waitTextVisible("Welcome back");
      cy.hideOnboardingTour();
      cy.waitTextVisible(name);
    });
  });

  it("Edit Metadata Ingestion platform policy and assign privileges to the user", () => {
    cy.loginWithCredentials();
    cy.visit("/settings/permissions/policies");
    cy.waitTextVisible("Manage Permissions");
    editPolicy(platform_policy_name, "Ingestion", "Manage Metadata Ingestion");
  });

  it("Verify new user can see ingestion and access Manage Ingestion tab", () => {
    cy.clearCookies();
    cy.clearLocalStorage();
    signIn();
    cy.waitTextVisible("Welcome back");
    cy.hideOnboardingTour();
    cy.waitTextVisible(name);
    cy.clickOptionWithText("Ingestion");
    cy.wait(1000);
    cy.get("body").click();
    cy.waitTextVisible("Manage Data Sources");
    cy.waitTextVisible("Sources");
    cy.get(".ant-tabs-nav-list").contains("Source").should("be.visible");
    cy.get(".ant-tabs-tab").should("have.length", 1);
  });

  it("Verify new user can see ingestion and access Manage Secret tab", () => {
    cy.clearCookies();
    cy.clearLocalStorage();
    cy.loginWithCredentials();
    cy.visit("/settings/permissions/policies");
    cy.waitTextVisible("Manage Permissions");
    editPolicy(platform_policy_name, "Secret", "Manage Secrets");
    cy.logout();
    signIn();
    cy.waitTextVisible("Welcome back");
    cy.hideOnboardingTour();
    cy.waitTextVisible(name);
    cy.clickOptionWithText("Ingestion");
    cy.wait(1000);
    cy.clickOptionWithId("body");
    cy.waitTextVisible("Manage Data Sources");
    cy.waitTextVisible("Secrets");
    cy.get(".ant-tabs-nav-list").contains("Secrets").should("be.visible");
    cy.get(".ant-tabs-tab").should("have.length", 1);
  });
});
