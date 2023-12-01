const urn = "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)";
const datasetName = "SampleCypressHdfsDataset";

describe("dataset health test", () => {
    it("go to dataset with failing assertions and verify health of dataset", () => {
        cy.login();
        cy.goToDataset(urn, datasetName);
        //Ensure that the “Health” badge is present and there is an active incident warning
        cy.get(`[href="/dataset/${urn}/Validation"]`).should("be.visible");
        cy.get(`[href="/dataset/${urn}/Validation"] span`).trigger("mouseover", { force: true });
        cy.waitTextVisible("This asset may be unhealthy");
        cy.waitTextVisible("Assertions 1 of 1 assertions are failing");
        cy.get('[data-testid="assertions-details"]').click();
        // cy.clickOptionWithText("details");
        cy.waitTextVisible("All assertions are failing");
        cy.clickOptionWithText("External");
        cy.waitTextVisible("Failed");
        });
});