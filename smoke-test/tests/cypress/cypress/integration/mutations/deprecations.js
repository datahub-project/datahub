describe("deprecation", () => {
    it("go to dataset and check deprecation works", () => {
        const urn = "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)";
        const datasetName = "cypress_logging_events";
        cy.login();

        cy.goToDataset(urn, datasetName);
        cy.openThreeDotDropdown();
        cy.clickOptionWithText("Mark as deprecated");
        cy.addViaModel("test deprecation", "Add Deprecation Details");

        cy.goToDataset(urn, datasetName);
        cy.contains("Deprecated");

        cy.openThreeDotDropdown();
        cy.clickOptionWithText("Mark as un-deprecated");
        cy.ensureTextNotPresent("Deprecated");
    });
});
