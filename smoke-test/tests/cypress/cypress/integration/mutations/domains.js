const test_domain = "CypressDomainTest";
let domain_created_urn = ""

describe("add remove domain", () => {
    it("create domain", () => {
        cy.login();
        cy.goToDomainList();
        cy.clickOptionWithText("New Domain");
        cy.addViaModel(test_domain, "Create new Domain")
        cy.waitTextVisible("Created domain!")
        
        cy.waitTextVisible(test_domain)

        cy.waitTextVisible(test_domain)
            .parents("[data-testid^='urn:li:domain:']")
            .invoke('attr', 'data-testid')
            .then((data_test_id) => {
                domain_created_urn = data_test_id;
            })
    })

    it("add entities to domain", () => {
        cy.login();
        cy.goToDomainList();
        cy.clickOptionWithText(test_domain);
        cy.waitTextVisible("Add assets")
        cy.clickOptionWithText("Add assets")     
        cy.get(".ant-modal-content").within(() => {
            cy.get('[data-testid="search-input"]').click().type("jaffle")
            cy.waitTextVisible("jaffle_shop")
            cy.get(".ant-checkbox-input").first().click()
            cy.get("#continueButton").click()
        })
        cy.waitTextVisible("Added assets to Domain!")
    })

    it("search filter by domain", () => {
        cy.login();
        cy.goToStarSearchList()
        cy.waitTextVisible(test_domain)
        cy.get('[data-testid="facet-domains-' + domain_created_urn + '"]').click()
        cy.waitTextVisible("jaffle_shop")
    })

    it("remove entity from domain", () => {
        cy.login();
        cy.goToDomainList();
        cy.removeDomainFromDataset(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)",
            "customers",
            domain_created_urn
        )
    })

    it("delete a domain and ensure dangling reference is deleted on entities", () => {
        cy.login();
        cy.goToDomainList();
        cy.get('[data-testid="' + domain_created_urn + '"]').within(() => {
            cy.get(".ant-dropdown-trigger").click();
        });
        cy.clickOptionWithText("Delete");
        cy.clickOptionWithText("Yes");
        cy.ensureTextNotPresent(test_domain)
        
        cy.goToContainer("urn:li:container:348c96555971d3f5c1ffd7dd2e7446cb")
        cy.waitTextVisible("jaffle_shop")
        cy.ensureTextNotPresent(test_domain)
    })
});