describe("glossary", () => {
    it("go to glossary page, create terms, term group", () => {

        const urn = "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)";
        const datasetName = "cypress_logging_events";
        const glossaryTerm = "CypressGlosssaryTerm";
        const glossaryTermGroup = "CypressGlosssaryGroup";
        cy.login();
        cy.goToGlossaryList();

        cy.clickOptionWithText("Add Term");
        cy.addViaModel(glossaryTerm, "Create Glossary Term");

        cy.clickOptionWithText("Add Term Group");
        cy.addViaModel(glossaryTermGroup, "Create Term Group");

        cy.addTermToDataset(urn, datasetName, glossaryTerm);

        cy.goToGlossaryList();
        cy.clickOptionWithText(glossaryTerm);
        cy.deleteFromDropdown();

        cy.goToDataset(urn, datasetName);
        cy.ensureTextNotPresent(glossaryTerm);

        cy.goToGlossaryList();
        cy.clickOptionWithText(glossaryTermGroup);
        cy.deleteFromDropdown();

        cy.goToGlossaryList();
        cy.ensureTextNotPresent(glossaryTermGroup);
    });
});
