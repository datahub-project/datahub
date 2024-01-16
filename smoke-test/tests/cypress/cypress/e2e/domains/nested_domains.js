const domainName = "CypressNestedDomain";
const domainDescription = "CypressNestedDomainDescription";

const createDomain = () => {
    cy.clickOptionWithTestId("domains-new-domain-button");
    cy.get('[data-testid="create-domain-name"]').click().type(domainName);
    cy.get('[data-testid="create-domain-description"]').click().type(domainDescription);
    cy.clickOptionWithTestId("create-domain-button");
    cy.waitTextVisible("Created domain!");
    }

const moveDomaintoRootLevel = () => {
    cy.clickOptionWithText(domainName);
    cy.openThreeDotDropdown();
    cy.clickOptionWithTestId("entity-menu-move-button");
    cy.get('[data-testid="move-domain-modal"]').contains("Marketing").click({force: true});
    cy.waitTextVisible('Marketing')
    cy.clickOptionWithTestId("move-domain-modal-move-button")
 }

const moveDomaintoParent = () => {
    cy.get('[data-testid="domain-list-item"]').contains("Marketing").prev().click();
    cy.clickOptionWithText(domainName);
    cy.waitTextVisible(domainName)
    cy.openThreeDotDropdown();
    cy.clickOptionWithTestId("entity-menu-move-button");
    cy.clickOptionWithTestId("move-domain-modal-move-button")
        }

 const deleteFromDomainDropdown = () => {
    cy.clickOptionWithText('Filters')
    cy.openThreeDotDropdown();
    cy.clickOptionWithTestId("entity-menu-delete-button");
    cy.waitTextVisible("Are you sure you want to remove this Domain?");
    cy.clickOptionWithText("Yes");
 }       

const deleteDomain = () => {
    cy.clickOptionWithText(domainName).waitTextVisible('Domains');
    deleteFromDomainDropdown()
  }

//Delete Unecessary Existing Domains
 const deleteExisitingDomain = () => {
    cy.get('a[href*="urn:li"] span[class^="ant-typography"]')
          .should('be.visible')
          .its('length')
          .then((length) => {
        for (let i = 0; i < length - 1; i++) {
            cy.get('a[href*="urn:li"] span[class^="ant-typography"]')
                .should('be.visible')
                .first()
                .click({ force: true });
            deleteFromDomainDropdown();
        }
    });   
    cy.waitTextVisible('My custom domain'); 
 }   

describe("nested domains test", () => {
    beforeEach (() => {
    cy.loginWithCredentials();
    cy.goToDomainList();
  });
  
    it("Create a new domain", () => {
        deleteExisitingDomain()      
        createDomain();
        cy.waitTextVisible("Domains");
    });
        
    it("Move domain root level to parent level", () => {
        cy.waitTextVisible(domainName)
        moveDomaintoRootLevel();
        cy.waitTextVisible("Moved Domain!")
        cy.goToDomainList();
        cy.waitTextVisible("1 sub-domain");
    });

    it("Move domain parent level to root level", () => {
        moveDomaintoParent();
        cy.waitTextVisible("Moved Domain!")
        cy.goToDomainList();
        cy.waitTextVisible(domainName);
        cy.waitTextVisible(domainDescription);
    });

    it("Remove the domain", () => {
        deleteDomain();
        cy.goToDomainList();
        cy.ensureTextNotPresent(domainName);
        cy.ensureTextNotPresent(domainDescription);
    });
});
