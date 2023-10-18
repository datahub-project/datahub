const domainName = "CypressNestedDomain";
const domainDescription = "CypressNestedDomainDescription";

describe("nested domains test", () => {

    it("create a domain, move under parent, remove domain", () => {
        // Create a new domain without a parent
        cy.loginWithCredentials();
        cy.goToDomainList();
        cy.clickOptionWithTestId("domains-new-domain-button");
        cy.get('[data-testid="create-domain-name"]').click().type(domainName);
        cy.get('[data-testid="create-domain-description"]').click().type(domainDescription);
        cy.clickOptionWithTestId("create-domain-button");
        cy.waitTextVisible(domainName);

        // Ensure the new domain has no parent in the navigation sidebar
        cy.waitTextVisible(domainDescription);

        // Move a domain from the root level to be under a parent domain
        cy.clickOptionWithText(domainName);
        cy.openThreeDotDropdown();
        cy.clickOptionWithTestId("entity-menu-move-button");
        cy.get('[data-testid="move-domain-modal"]').contains("Marketing").click({force: true});
        cy.get('[data-testid="move-domain-modal"]').contains("Marketing").should("be.visible");
        cy.clickOptionWithTestId("move-domain-modal-move-button").wait(5000);

        // Wnsure domain is no longer on the sidebar navigator at the top level but shows up under the parent
        cy.goToDomainList();
        cy.ensureTextNotPresent(domainName);
        cy.ensureTextNotPresent(domainDescription);
        cy.waitTextVisible("1 sub-domain");

        // Move a domain from under a parent domain to the root level
        cy.get('[data-testid="domain-list-item"]').contains("Marketing").prev().click();
        cy.clickOptionWithText(domainName);
        cy.openThreeDotDropdown();
        cy.clickOptionWithTestId("entity-menu-move-button");
        cy.clickOptionWithTestId("move-domain-modal-move-button").wait(5000);
        cy.goToDomainList();
        cy.waitTextVisible(domainName);
        cy.waitTextVisible(domainDescription);

        // Delete a domain
        cy.clickOptionWithText(domainName).wait(3000);
        cy.openThreeDotDropdown();
        cy.clickOptionWithTestId("entity-menu-delete-button");
        cy.waitTextVisible("Are you sure you want to remove this Domain?");
        cy.clickOptionWithText("Yes");
        cy.waitTextVisible("Deleted Domain!");
        cy.ensureTextNotPresent(domainName);
        cy.ensureTextNotPresent(domainDescription);
    });
});