describe("deprecation", () => {
    it("go to dataset and check deprecation works", () => {
        const urn = "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)";
        const dataset_name = "cypress_logging_events";
        cy.login();

        cy.goToDataset(urn, dataset_name);
        cy.openThreeDotDropdown();
        cy.clickOptionWithText("Mark as deprecated");
        cy.addViaModel("test deprecation", "Add Deprecation Details");

        cy.goToDataset(urn, dataset_name);
        cy.contains("Deprecated");

        cy.openThreeDotDropdown();
        cy.clickOptionWithText("Mark as un-deprecated");
        cy.ensureTextNotPresent("Deprecated");
    });
});
