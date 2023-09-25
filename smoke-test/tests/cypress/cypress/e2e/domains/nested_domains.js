import { aliasQuery, hasOperationName } from "../utils";

const domainName = "CypressNestedDomain";
const domainDescription = "CypressNestedDomainDescription";

describe("nested domains test", () => {

    it("create a domain, move under parent, remove domain", () => {
        setDomainsFeatureFlag(true);
        //create a new domain without a parent
        cy.loginWithCredentials();
        cy.goToDomainList();
        cy.clickOptionWithText("New Domain");
        cy.waitTextVisible("Create New Domain");
        cy.get('[data-testid="create-domain-name"]').click().type(domainName);
        cy.get('#description').click().type(domainDescription);
        cy.get('[data-testid="create-domain-button"]').click()
        cy.waitTextVisible(domainName);
        //ensure the new domain has no parent in the navigation sidebar
        cy.waitTextVisible(domainDescription);
        //move a domain from the root level to be under a parent domain
        cy.clickOptionWithText(domainName);
        cy.openThreeDotDropdown();
        cy.clickOptionWithText("Move");
        cy.get('[role="dialog"]').contains("Marketing").click({force: true});
        cy.get('[role="dialog"]').contains("Marketing").should("be.visible");
        cy.get("button").contains("Move").click().wait(5000);
        //ensure domain is no longer on the sidebar navigator at the top level but shows up under the parent
        cy.goToDomainList();
        cy.ensureTextNotPresent(domainName);
        cy.ensureTextNotPresent(domainDescription);
        cy.waitTextVisible("1 sub-domain");
        //move a domain from under a parent domain to the root level
        cy.contains("Marketing").prev().click();
        cy.clickOptionWithText(domainName);
        cy.openThreeDotDropdown();
        cy.clickOptionWithText("Move");
        cy.get("button").contains("Move").click().wait(5000);
        cy.goToDomainList();
        cy.waitTextVisible(domainName);
        cy.waitTextVisible(domainDescription);
        //delete a domain
        cy.clickOptionWithText(domainName).wait(3000);
        cy.openThreeDotDropdown();
        cy.clickOptionWithText("Delete");
        cy.waitTextVisible("Are you sure you want to remove this Domain?");
        cy.get("button").contains("Yes").click();
        cy.waitTextVisible("Deleted Domain!");
        cy.ensureTextNotPresent(domainName);
        cy.ensureTextNotPresent(domainDescription);
    });
});