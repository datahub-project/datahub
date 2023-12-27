const test_id = Math.floor(Math.random() * 100000);
const email = `example${test_id}@example.com`;
const password = "Example password Reset";

const tryToSignUp = () => {
  let name = `Example Name ${test_id}`;
  cy.enterTextInTestId("email", email);
  cy.enterTextInTestId("name", name);
  cy.enterTextInTestId("password", "Example password");
  cy.enterTextInTestId("confirmPassword", "Example password");
  cy.mouseover("#title").click();
  cy.waitTextVisible("Other").click();
  cy.get("[type=submit]").click();
  return name;
};

describe("add_user", () => {
  it("go to user link and invite a user", () => {
    cy.login();
    cy.visit("/settings/identities/users");
    cy.waitTextVisible("Invite Users");
    cy.clickOptionWithText("Invite Users");
    cy.waitTextVisible(/signup\?invite_token=\w{32}/)
      .then(($elem) => {
        const inviteLink = $elem.text();
        cy.visit("/settings/identities/users");
        cy.logout();
        cy.visit(inviteLink);
        let name = tryToSignUp();
        cy.waitTextVisible("Welcome to DataHub");
        cy.hideOnboardingTour();
        cy.waitTextVisible(name);
      })
      .then(() => {
        cy.logout();
        cy.visit("/signup?invite_token=bad_token");
        tryToSignUp();
        cy.waitTextVisible("Failed to log in! An unexpected error occurred.");
      });
  });
});

describe("Reset Password", () => {
  it("Verify you can’t generate a reset password link for a non-native user", () => {
    cy.login();
    cy.visit("/settings/identities/users");
    cy.wait(2000);
    cy.get("[data-testid=userItem-non-native]").first().click();
    cy.get("[data-testid=resetButton]").should(($button) => {
      const isDisabled = $button.prop("disabled");
      if (Cypress.env("ADMIN_USERNAME") !== "datahub") {
        expect(isDisabled).to.eq(true);
      }
    });
  });

  it("Generate a reset password link for a native user", () => {
    cy.login();
    cy.visit("/settings/identities/users");
    cy.wait(2000);
    cy.get("[data-testid=userItem-native]").first().click();
    cy.get("[data-testid=resetButton]").first().click();
    cy.get("[data-testid=refreshButton]").click();
    cy.waitTextVisible("Generated new link to reset credentials");
    cy.wait(2000);

    cy.window().then((win) => {
      cy.stub(win, "prompt");
    });
    cy.get(".ant-typography-copy").should("be.visible").click();
    cy.get(".ant-modal-close").should("be.visible").click();

    cy.waitTextVisible(/reset\?reset_token=\w{32}/)
      .then(($elem) => {
        const inviteLink = $elem.text();
        cy.logout();
        cy.visit(inviteLink);
        cy.enterTextInTestId("email", email);
        cy.enterTextInTestId("password", password);
        cy.enterTextInTestId("confirmPassword", password);
        cy.get("[type=submit]").click();
        cy.wait(2000);
        cy.hideOnboardingTour();
        cy.waitTextVisible("Welcome back");
      })
      .then(() => {
        cy.logout();
        cy.visit("/reset?reset_token=bad_token");
        cy.enterTextInTestId("email", email);
        cy.enterTextInTestId("password", password);
        cy.enterTextInTestId("confirmPassword", password);
        cy.get("[type=submit]").click();
        cy.waitTextVisible("Failed to log in!");
      });
  });
});
// Verify you can’t generate a reset password link for a non-native user (root, for example)
// Generate a reset password link for a native user
// Log out, then verify that using a bad reset token in the URL doesn’t allow you to reset password
// Use the correct reset link to reset native user credentials
