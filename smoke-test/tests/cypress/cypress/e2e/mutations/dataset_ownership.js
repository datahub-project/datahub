const test_id = Math.floor(Math.random() * 100000);
const username = `Example Name ${test_id}`;
const email = `example${test_id}@example.com`;
const password = "Example password";
const group_name = `Test group ${test_id}`;

const addOwner = (owner, type, elementId) => {
    cy.clickOptionWithTestId("add-owners-button");
    cy.get('[id="owner-search-input"]').type(owner);
    cy.clickOptionWithText(owner);
    cy.get('[role="dialog"]').contains("Technical Owner").click();
    cy.get('[role="listbox"]').parent().contains(type).click();
    cy.get('[role="dialog"]').contains(type).should("be.visible");
    cy.clickOptionWithText("Done");
    cy.waitTextVisible("Owners Added");
    cy.waitTextVisible(type);
    cy.waitTextVisible(owner).wait(3000);
    cy.clickOptionWithText(owner);
    cy.waitTextVisible("SampleCypressHiveDataset");
    cy.goToDataset("urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)", "SampleCypressHiveDataset");
    cy.get(elementId).next().click({ focus: true });
    cy.clickOptionWithText("Yes");
    cy.waitTextVisible("Owner Removed");
    cy.ensureTextNotPresent(owner);
    cy.ensureTextNotPresent(type);
}

describe("add, remove ownership for dataset", () => {
    beforeEach(() => {
        cy.on('uncaught:exception', (err, runnable) => { return false; });
      });


    it("open test dataset page, add and remove user ownership(test every type)", () => {
        cy.loginWithCredentials();
        cy.goToDataset("urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)", "SampleCypressHiveDataset");
        //business owner
        addOwner(username, "Business Owner", `[href="/user/urn:li:corpuser:example${test_id}@example.com"]`);
        //data steward
        addOwner(username, "Data Steward", `[href="/user/urn:li:corpuser:example${test_id}@example.com"]`);
        //none
        addOwner(username, "None", `[href="/user/urn:li:corpuser:example${test_id}@example.com"]`);
        //technical owner
        addOwner(username, "Technical Owner", `[href="/user/urn:li:corpuser:example${test_id}@example.com"]`);
    });

    it("open test dataset page, add and remove group ownership(test every type)", () => {
        cy.loginWithCredentials();
        cy.goToDataset("urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)", "SampleCypressHiveDataset");
        //business owner
        addOwner(group_name, "Business Owner", `[href="/group/urn:li:corpGroup:${test_id}"]`);
        //data steward
        addOwner(group_name, "Data Steward", `[href="/group/urn:li:corpGroup:${test_id}"]`);
        //none
        addOwner(group_name, "None", `[href="/group/urn:li:corpGroup:${test_id}"]`);
        //technical owner
        addOwner(group_name, "Technical Owner", `[href="/group/urn:li:corpGroup:${test_id}"]`);
    });
});