const urn1 = "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)";
const datasetName1 = "SampleCypressHdfsDataset";

const urn2 = "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_health_test,PROD)";
const datasetName2 = "cypress_health_test";

describe("dataset health test", () => {
    it("go to dataset with failing assertions and verify health of dataset", () => {
        cy.login();
        cy.goToDataset(urn1, datasetName1);
        // Ensure that the “Health” badge is present and there is an active incident warning
        cy.get(`[href="/dataset/${urn1}/Validation"]`).should("be.visible");
        cy.get(`[href="/dataset/${urn1}/Validation"] span`).trigger("mouseover", { force: true });
        cy.waitTextVisible("This asset may be unhealthy");
        cy.waitTextVisible("Assertions 1 of 1 assertions are failing");
        cy.get('[data-testid="assertions-details"]').click();
        // cy.clickOptionWithText("details");
        cy.waitTextVisible("1 failed assertions");
    });
    it("go to dataset with active incidents and verify health of dataset", () => {
        cy.login();
        cy.goToDataset(urn2, datasetName2);
        // Ensure that the “Health” badge is present and there is an active incident warning
        cy.get(`[href="/dataset/${urn2}/Validation"]`).should("be.visible");
        cy.get(`[href="/dataset/${urn2}/Validation"] span`).trigger("mouseover", { force: true });
        cy.waitTextVisible("This asset may be unhealthy");
        cy.waitTextVisible("1 active incident");
    });
});