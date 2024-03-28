describe("businessAttribute", () => {
    it('go to business attribute page, create attribute ', function () {
        const urn="urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)";
        const businessAttribute="CypressBusinessAttribute";
        const datasetName = "cypress_logging_events";
        cy.login();
        cy.goToBusinessAttributeList();

        cy.clickOptionWithText("Create Business Attribute");
        cy.addViaModal(businessAttribute, "Create Business Attribute", businessAttribute, "create-business-attribute-button");

        cy.wait(3000);
        cy.goToBusinessAttributeList().contains(businessAttribute).should("be.visible");

        cy.addAttributeToDataset(urn, datasetName, businessAttribute);

        cy.get('[data-testid="schema-field-event_name-businessAttribute"]').within(() =>
            cy
                .get("span[aria-label=close]")
                .trigger("mouseover", { force: true })
                .click({ force: true })
        );
        cy.contains("Yes").click({ force: true });

        cy.get('[data-testid="schema-field-event_name-businessAttribute"]').contains("CypressBusinessAttribute").should("not.exist");

        cy.goToBusinessAttributeList();
        cy.clickOptionWithText(businessAttribute);
        cy.deleteFromDropdown();

        cy.goToBusinessAttributeList();
        cy.ensureTextNotPresent(businessAttribute);
    });

    it('Inheriting tags and terms from business attribute to dataset ', function () {
        const urn="urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)";
        const businessAttribute="CypressAttribute";
        const datasetName = "cypress_logging_events";
        const term="CypressTerm";
        const tag="Cypress";

        cy.login();

        cy.addAttributeToDataset(urn, datasetName, businessAttribute);
        cy.contains(term);
        cy.contains(tag);

    });

    it("can visit related entities", () => {
        const businessAttribute="CypressAttribute";
        cy.login();
        cy.goToBusinessAttributeList();
        cy.clickOptionWithText(businessAttribute);
        cy.clickOptionWithText("Related Entities");
        //cy.visit("/business-attribute/urn:li:businessAttribute:37c81832-06e0-40b1-a682-858e1dd0d449/Related%20Entities");
        //cy.wait(5000);
        cy.contains("of 0").should("not.exist");
        cy.contains(/of [0-9]+/);
    });


    it("can search related entities by query", () => {
        cy.login();
        cy.visit("/business-attribute/urn:li:businessAttribute:37c81832-06e0-40b1-a682-858e1dd0d449/Related%20Entities");
        cy.get('[placeholder="Filter entities..."]').click().type(
            "logging{enter}"
        );
        cy.wait(5000);
        cy.contains("of 0").should("not.exist");
        cy.contains(/of 1/);
        cy.contains("cypress_logging_events");
    });

    it("remove business attribute from dataset", () => {
        const urn="urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)";
        const datasetName = "cypress_logging_events";
        cy.login();
        cy.goToDataset(urn, datasetName);

        cy.wait(3000);

        cy.clickOptionWithText("event_name");
        cy.get('[data-testid="schema-field-event_name-businessAttribute"]').within(() =>
            cy
                .get("span[aria-label=close]")
                .trigger("mouseover", { force: true })
                .click({ force: true })
        );
        cy.contains("Yes").click({ force: true });

        cy.get('[data-testid="schema-field-event_name-businessAttribute"]').contains("CypressAttribute").should("not.exist");
    });

    it("update the data type of a business attribute", () => {
        const businessAttribute="cypressTestAttribute";
        cy.login();
        cy.goToBusinessAttributeList();

        cy.clickOptionWithText(businessAttribute);

        cy.get('[data-testid="edit-data-type-button"]').within(() =>
            cy
                .get("span[aria-label=edit]")
                .trigger("mouseover", { force: true })
                .click({ force: true })
        );

        cy.get('[data-testid="add-data-type-option"]').get('.ant-select-selection-search-input').click({multiple: true});

        cy.get('.ant-select-item-option-content')
            .contains('STRING')
            .click();

        cy.contains("STRING");

    });
});
