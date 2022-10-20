describe("deprecation", () => {
    it("go to dataset and check deprecation works", () => {
        const urn = "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)";
        cy.login();

        cy.goToDataset(urn);
        cy.openThreeDotDropdown();
        cy.clickOptionWithText("Mark as deprecated");
        cy.addViaModel("test deprecation");

        cy.goToDataset(urn);
        cy.contains("Deprecated");

        cy.openThreeDotDropdown();
        cy.clickOptionWithText("Mark as un-deprecated");
        cy.ensureTextNotPresent("Deprecated");
    });
});
