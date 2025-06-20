describe("manage applications", () => {
  it("Manage Applications Page - Verify search bar placeholder", () => {
    cy.login();
    cy.visit("/applications");
    cy.get('[data-testid="application-search-input"]').should(
      "have.attr",
      "placeholder",
      "Search applications...",
    );
  });

  it("Manage Applications Page - Verify search not exists", () => {
    cy.login();
    cy.visit("/applications");
    cy.get('[data-testid="page-title"]').should(
      "contain.text",
      "Manage Applications",
    );

    cy.get('[data-testid="application-search-input"]', {
      timeout: 10000,
    }).should("be.visible");

    // wait for the text "loading data" to disappear
    cy.get("body").should("not.contain", "loading data");

    cy.wait(1000);

    cy.get('[id="application-search-input"]').type("testtestnomatch", {
      force: true,
    });

    cy.wait(1000);

    cy.get('[data-testid="applications-not-found"]').should(
      "contain.text",
      "No applications found for your search query",
    );
  });

  it("Manage Applications Page - Verify create and delete", () => {
    cy.login();
    cy.visit("/applications");
    cy.get('[data-testid="page-title"]').should(
      "contain.text",
      "Manage Applications",
    );

    // cy.get('Create Application', { timeout: 10000 }).should('be.visible');

    // wait for the text "loading data" to disappear
    cy.get("body").should("not.contain", "loading data");

    cy.wait(500);

    cy.contains("button", "Create Application").click();

    cy.wait(500);

    cy.get('[data-testid="application-name-input"]').type("test-new-name");

    cy.get('[data-testid="application-description-input"]').type(
      "test new description",
    );

    cy.get('[id="createNewApplicationButton"]').click();

    // refresh the page
    cy.reload();

    cy.get("body").should("contain", "test-new-name");

    // Find the application row by name and delete it
    // First, wait for the page to load completely
    cy.get("body").should("not.contain", "loading data");

    // Find the table row containing the application name and click its actions dropdown
    cy.contains("tr", "test-new-name").within(() => {
      cy.get('[data-testid$="MoreVertOutlinedIcon"]').click();
    });

    // Click the delete option from the dropdown menu
    cy.get('[data-testid="action-delete"]').click();

    // Verify the confirmation modal appears
    cy.contains("Delete application test-new-name").should("be.visible");

    // Handle the confirmation modal
    cy.contains("button", "Delete").click();

    cy.wait(500);

    cy.reload();

    cy.get("body").should("not.contain", "test-new-name");
  });
});
