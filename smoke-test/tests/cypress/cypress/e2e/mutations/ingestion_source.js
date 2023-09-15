
const number = Math.floor(Math.random() * 100000);
const accound_id = `account${number}`;
const warehouse_id = `warehouse${number}`;
const username = `user${number}`;
const password = `password${number}`;
const role = `role${number}`;
const ingestion_source_name = `ingestion source ${number}`;

describe("ingestion source creation flow", () => {
    it("create a ingestion source using ui, verify ingestion source details saved correctly, remove ingestion source", () => {
      cy.loginWithCredentials();
      //go to ingestion page, create a snowflake source
      cy.goToIngestionPage();
      cy.clickOptionWithText("Create new source");
      cy.clickOptionWithText("Snowflake");
      cy.waitTextVisible("Snowflake Recipe");
      cy.get("#account_id").type(accound_id);
      cy.get("#warehouse").type(warehouse_id);
      cy.get("#username").type(username);
      cy.get("#password").type(password);
      cy.focused().blur();
      cy.get("#role").type(role);
      //verify yaml recipe is generated correctly
      cy.clickOptionWithText("YAML");
      cy.waitTextVisible("account_id");
      cy.waitTextVisible(accound_id);
      cy.waitTextVisible(warehouse_id);
      cy.waitTextVisible(username);
      cy.waitTextVisible(password);
      cy.waitTextVisible(role);
      //finish creating source
      cy.get("button").contains("Next").click();
      cy.waitTextVisible("Configure an Ingestion Schedule");
      cy.get("button").contains("Next").click();
      cy.waitTextVisible("Give this ingestion source a name."); 
      cy.get('[data-testid="source-name-input"]').type(ingestion_source_name);
      cy.get("button").contains("Save").click();
      cy.waitTextVisible("Successfully created ingestion source!").wait(5000)//prevent issue with missing form data
      cy.waitTextVisible(ingestion_source_name);
      cy.get("button").contains("Pending...").should("be.visible");
      //verify ingestion source details are saved correctly
      cy.get("button").contains("EDIT").first().click();
      cy.waitTextVisible("Edit Ingestion Source");
      cy.get("#account_id").should("have.value", accound_id);
      cy.get("#warehouse").should("have.value", warehouse_id);
      cy.get("#username").should("have.value", username);
      cy.get("#password").should("have.value", password);
      cy.get("#role").should("have.value", role);
      cy.get("button").contains("Next").click();
      cy.waitTextVisible("Configure an Ingestion Schedule");
      cy.get("button").contains("Next").click();
      cy.get('[data-testid="source-name-input"]').clear().type(ingestion_source_name + " EDITED");
      cy.get("button").contains("Save").click();
      cy.waitTextVisible("Successfully updated ingestion source!");
      cy.waitTextVisible(ingestion_source_name + " EDITED");
      //remove ingestion source
      cy.get('[data-testid="delete-button"]').first().click();
      cy.waitTextVisible("Confirm Ingestion Source Removal");
      cy.get("button").contains("Yes").click();
      cy.waitTextVisible("Removed ingestion source.");
      cy.ensureTextNotPresent(ingestion_source_name + " EDITED")
    })
});