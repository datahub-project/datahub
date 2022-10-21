const tryToSignUp = () => {
    cy.enterTextInTestId("email", "example@example.com")
    cy.enterTextInTestId("name", "Example Name")
    cy.enterTextInTestId("password", "Example password")
    cy.enterTextInTestId("confirmPassword", "Example password")

    cy.mouseover("#title").click()
    cy.waitTextVisible("Other").click()

    cy.get("[type=submit]").click()
};

describe("add_user", () => {
    it("go to user link and invite a user", () => {
        cy.login()

        cy.visit("/settings/identities/users");
        cy.waitTextVisible("Invite Users");

        cy.clickOptionWithText("Invite Users")

        cy.waitTextVisible('signup?invite_token').then(($elem) => {
            const inviteLink = $elem.text();
            cy.logout();
            cy.visit(inviteLink);
            tryToSignUp();
            cy.waitTextVisible("Accepted invite!")
        }).then(() => {
            cy.logout();
            cy.visit("/signup?invite_token=bad_token");
            tryToSignUp()
            cy.waitTextVisible("Failed to log in! An unexpected error occurred.")
        });
    });
});