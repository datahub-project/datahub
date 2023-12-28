const tryToSignUp = () => {
  let number = Math.floor(Math.random() * 100000);
  let name = `Example Name ${number}`;
  cy.enterTextInTestId("email", `example${number}@example.com`);
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
        cy.log(inviteLink);
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
    cy.waitTextVisible("Invite Users");
    cy.get("[data-testid=userItem-non-native]").first().click();
    cy.get('[data-testid="reset-menu-item"]').should(
      "have.attr",
      "aria-disabled",
      "true"
    );
  });

  it("Generate a reset password link for a native user", () => {
    cy.login();
    cy.visit("/settings/identities/users");
    cy.waitTextVisible("Invite Users");
    cy.get("[data-testid=userItem-native]").first().click();
    // Capture the email of the selected native user required for reset password
    cy.get("[data-testid=email-native]")
      .first()
      .invoke("text")
      .then((email) => {
        cy.wrap(email).as("capturedEmail");
      });

    cy.get("[data-testid=resetButton]").first().click();
    cy.get("[data-testid=refreshButton]").click();
    cy.waitTextVisible("Generated new link to reset credentials");

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
        // Enter the captured email and set a new password for the user
        cy.get("@capturedEmail").then((capturedEmail) => {
          cy.enterTextInTestId("email", capturedEmail);
          cy.enterTextInTestId("password", "Example Reset Password");
          cy.enterTextInTestId("confirmPassword", "Example Reset Password");
          cy.get("[type=submit]").click();
          cy.waitTextVisible("Welcome back");
          cy.hideOnboardingTour();
        });
      })
      .then(() => {
        cy.logout();
        cy.visit("/reset?reset_token=bad_token");
        cy.get("@capturedEmail").then((capturedEmail) => {
          cy.enterTextInTestId("email", capturedEmail);
          cy.enterTextInTestId("password", "Example Reset Password");
          cy.enterTextInTestId("confirmPassword", "Example Reset Password");
          cy.get("[type=submit]").click();
          cy.waitTextVisible("Failed to log in!");
        });
      });
  });
});
