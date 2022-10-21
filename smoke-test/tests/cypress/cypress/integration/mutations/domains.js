const test_domain = "CypressDomainTest";

describe("add remove domain", () => {
    it("create domain", () => {
        cy.login();
        cy.goToDomainList();
        cy.clickOptionWithText("New Domain");
        cy.addViaModel(test_domain)
        cy.waitTextVisible("Created domain!")
        cy.waitTextVisible(test_domain)    
    })

    // add asset to domain
    // Search filter by domain
    // Remove entity from domain
    // Delete a domain - ensure that the dangling reference is deleted on the asset
});