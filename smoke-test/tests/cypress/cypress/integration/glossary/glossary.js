describe("glossary", () => {
    it("go to glossary page, create terms, term group", () => {

        const urn = "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)";
        const glossaryTerm = "CypressGlosssaryTerm";
        const glossaryTermGroup = "CypressGlosssaryGroup";
        cy.login();
        cy.goToGlossaryList();

        cy.clickOptionWithText("Add Term");
        cy.addViaModel(glossaryTerm);

        cy.contains("Add Term Group").click();
        cy.addViaModel(glossaryTermGroup);

        cy.addTermToDataset(urn, glossaryTerm);

        cy.goToGlossaryList();
        cy.clickOptionWithText(glossaryTerm);
        cy.deleteFromDropdown();

        cy.goToDataset(urn);
        cy.ensureTextNotPresent(glossaryTerm);

        cy.goToGlossaryList();
        cy.clickOptionWithText(glossaryTermGroup);
        cy.deleteFromDropdown();

        cy.goToGlossaryList();
        cy.ensureTextNotPresent(glossaryTermGroup);
    });
});
