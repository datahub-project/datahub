describe("glossary", () => {
    it("go to glossary page, create terms, term group", () => {

        const urn = "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)";
        const dataset_name = "cypress_logging_events";
        const glossaryTerm = "CypressGlosssaryTerm";
        const glossaryTermGroup = "CypressGlosssaryGroup";
        cy.login();
        cy.goToGlossaryList();

        cy.clickOptionWithText("Add Term");
        cy.addViaModel(glossaryTerm, "Create Glossary Term");

        cy.clickOptionWithText("Add Term Group");
        cy.addViaModel(glossaryTermGroup, "Create Term Group");

        cy.addTermToDataset(urn, dataset_name, glossaryTerm);

        cy.goToGlossaryList();
        cy.clickOptionWithText(glossaryTerm);
        cy.deleteFromDropdown();

        cy.goToDataset(urn, dataset_name);
        cy.ensureTextNotPresent(glossaryTerm);

        cy.goToGlossaryList();
        cy.clickOptionWithText(glossaryTermGroup);
        cy.deleteFromDropdown();

        cy.goToGlossaryList();
        cy.ensureTextNotPresent(glossaryTermGroup);
    });
});
