import { hasOperationName } from "../utils";

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
  cy.get('[data-testid="sign-up"]').click();
  return { name, email };
};

const signIn = () => {
  cy.visit("/login");
  cy.enterTextInTestId("username", email);
  cy.enterTextInTestId("password", "Example password");
  cy.get('[data-testid="sign-in"]').click();
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

const searchForPolicy = (policyName) => {
  cy.get('[data-testid="search-bar-input"]').should("be.visible");
  cy.get('[data-testid="search-bar-input"]').clear().type(policyName);
  cy.wait(500);
};

const openRowMenu = (policyName) => {
  cy.contains("tr", policyName).find("button").last().click({ force: true });
  cy.wait(300);
};

const clickMenuAction = (actionText) => {
  cy.get('[data-testid^="menu-item-"]').contains(actionText).click();
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
  searchForPolicy(policyName);
  cy.get("tbody").contains(policyName);
};

const editPolicy = (policyName, type, select) => {
  searchForPolicy(policyName);
  openRowMenu(policyName);
  clickMenuAction("Edit");
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
  cy.get("tbody tr td").should("be.visible");
  cy.get("tbody tr").each(($row) => {
    cy.wrap($row)
      .find("td")
      .eq(3)
      .invoke("text")
      .then((role) => {
        if (role.includes("All Users")) {
          cy.wrap($row).find("button").last().click({ force: true });
          cy.wait(300);
          cy.get("body").then(($body) => {
            if ($body.find('[data-testid^="menu-item-"]').length > 0) {
              cy.get('[data-testid^="menu-item-"]')
                .contains("Deactivate")
                .then(($el) => {
                  if ($el.length) {
                    cy.wrap($el).click();
                    cy.waitTextVisible("Successfully deactivated policy.");
                  }
                });
            }
          });
        }
      });
  });
};

describe("Manage Ingestion and Secret Privileges", () => {
  let registeredEmail = "";

  beforeEach(() => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.on("response", (res) => {
          res.body.data.appConfig.featureFlags.showIngestionPageRedesign = false;
        });
      }
    });
  });

  it("create Metadata Ingestion platform policy and assign privileges to all users", () => {
    cy.login();
    cy.visit("/settings/permissions/policies");
    cy.waitTextVisible("Manage Permissions");
    cy.get('[data-testid="policy-filter"]').click();
    cy.get('[data-testid="option-ALL"]').click();
    cy.wait(500);
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
    cy.login();
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

  // TODO: To be added back. Need to modify editPolicy
  // it("Edit Metadata Ingestion platform policy and assign privileges to the user", () => {
  //   cy.loginWithCredentials();
  //   cy.visit("/settings/permissions/policies");
  //   cy.waitTextVisible("Manage Permissions");
  //   editPolicy(platform_policy_name, "Ingestion", "Manage Metadata Ingestion");
  // });

  it("Verify new user can see ingestion and access Manage Ingestion tab", () => {
    cy.clearCookies();
    cy.clearLocalStorage();
    signIn();
    cy.waitTextVisible("Welcome back");
    cy.hideOnboardingTour();
    cy.waitTextVisible(name);
    cy.get('[id="home-page-ingestion"]').scrollIntoView().click();
    cy.wait(1000);
    cy.get("body").click();
    cy.waitTextVisible("Manage Data Sources");
    cy.waitTextVisible("Sources");
    cy.get(".ant-tabs-nav-list").contains("Source").should("be.visible");
    cy.get(".ant-tabs-tab").should("have.length", 1);
  });

  // TODO: To be added back. Need to modify editPolicy
  // it("Verify new user can see ingestion and access Manage Secret tab", () => {
  //   cy.clearCookies();
  //   cy.clearLocalStorage();
  //   cy.loginWithCredentials();
  //   cy.visit("/settings/permissions/policies");
  //   cy.waitTextVisible("Manage Permissions");
  //   editPolicy(platform_policy_name, "Secret", "Manage Secrets");
  //   cy.logout();
  //   signIn();
  //   cy.waitTextVisible("Welcome back");
  //   cy.hideOnboardingTour();
  //   cy.waitTextVisible(name);
  //   cy.clickOptionWithText("Ingestion");
  //   cy.wait(1000);
  //   cy.clickOptionWithId("body");
  //   cy.waitTextVisible("Manage Data Sources");
  //   cy.waitTextVisible("Secrets");
  //   cy.get(".ant-tabs-nav-list").contains("Secrets").should("be.visible");
  //   cy.get(".ant-tabs-tab").should("have.length", 1);
  // });
});
