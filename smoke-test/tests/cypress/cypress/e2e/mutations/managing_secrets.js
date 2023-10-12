const number = Math.floor(Math.random() * 100000);
const accound_id = `account${number}`;
const warehouse_id = `warehouse${number}`;
const username = `user${number}`;
const password = `password${number}`;
const role = `role${number}`;
const ingestion_source_name = `ingestion source ${number}`;

describe("managing secrets for ingestion creation", () => {
    it("create a secret, create ingestion source using a secret, remove a secret", () => {
      cy.loginWithCredentials();
      //navigate to the manage ingestion page → secrets
      cy.goToIngestionPage();
      cy.clickOptionWithText("Secrets");
      //create a new secret
      cy.clickOptionWithText("Create new secret");
      cy.get('[role="dialog"]').contains("Create a new Secret").should("be.visible");
      cy.get('[role="dialog"] #name').type(`secretname${number}`);
      cy.get('[role="dialog"] #value').type(`secretvalue${number}`);
      cy.get('[role="dialog"] #description').type(`secretdescription${number}`);
      cy.get('#createSecretButton').click();
      cy.waitTextVisible("Successfully created Secret!");
      cy.waitTextVisible(`secretname${number}`);
      cy.waitTextVisible(`secretdescription${number}`).wait(5000)//prevent issue with missing secret
      //create an ingestion source using a secret
      cy.goToIngestionPage();
      cy.clickOptionWithText("Create new source");
      cy.clickOptionWithText("Snowflake");
      cy.waitTextVisible("Snowflake Recipe");
      cy.get("#account_id").type(accound_id);
      cy.get("#warehouse").type(warehouse_id);
      cy.get("#username").type(username);
      cy.get("#password").click().wait(1000);
      cy.contains(`secretname${number}`).click({force: true});
      cy.focused().blur();
      cy.get("#role").type(role);
      cy.get("button").contains("Next").click();
      cy.waitTextVisible("Configure an Ingestion Schedule");
      cy.get("button").contains("Next").click();
      cy.waitTextVisible("Give this ingestion source a name."); 
      cy.get('[data-testid="source-name-input"]').type(ingestion_source_name);
      cy.get("button").contains("Save").click();
      cy.waitTextVisible("Successfully created ingestion source!").wait(5000)//prevent issue with missing form data
      cy.waitTextVisible(ingestion_source_name);
      cy.get("button").contains("Pending...").should("be.visible");
      //remove a secret
      cy.clickOptionWithText("Secrets");
      cy.waitTextVisible(`secretname${number}`);
      cy.get('[data-icon="delete"]').first().click();
      cy.waitTextVisible("Confirm Secret Removal");
      cy.get("button").contains("Yes").click();
      cy.waitTextVisible("Removed secret.");
      cy.ensureTextNotPresent(`secretname${number}`);
      cy.ensureTextNotPresent(`secretdescription${number}`);
      //remove ingestion source
      cy.goToIngestionPage();
      cy.get('[data-testid="delete-button"]').first().click();
      cy.waitTextVisible("Confirm Ingestion Source Removal");
      cy.get("button").contains("Yes").click();
      cy.waitTextVisible("Removed ingestion source.");
      cy.ensureTextNotPresent(ingestion_source_name)
      //verify secret is not present during ingestion source creation for password dropdown
      cy.clickOptionWithText("Create new source");
      cy.clickOptionWithText("Snowflake");
      cy.waitTextVisible("Snowflake Recipe");
      cy.get("#account_id").type(accound_id);
      cy.get("#warehouse").type(warehouse_id);
      cy.get("#username").type(username);
      cy.get("#password").click().wait(1000);
      cy.ensureTextNotPresent(`secretname${number}`);
      //verify secret can be added during ingestion source creation and used successfully
      cy.clickOptionWithText("Create Secret");    
      cy.get('[role="dialog"]').contains("Create a new Secret").should("be.visible");
      cy.get('[role="dialog"] #name').type(`secretname${number}`);
      cy.get('[role="dialog"] #value').type(`secretvalue${number}`);
      cy.get('[role="dialog"] #description').type(`secretdescription${number}`);
      cy.get('#createSecretButton').click();
      cy.waitTextVisible("Created secret!");
      cy.get("#role").type(role);
      cy.get("button").contains("Next").click();
      cy.waitTextVisible("Configure an Ingestion Schedule");
      cy.get("button").contains("Next").click();
      cy.waitTextVisible("Give this ingestion source a name."); 
      cy.get('[data-testid="source-name-input"]').type(ingestion_source_name);
      cy.get("button").contains("Save").click();
      cy.waitTextVisible("Successfully created ingestion source!").wait(5000)//prevent issue with missing form data
      cy.waitTextVisible(ingestion_source_name);
      cy.get("button").contains("Pending...").should("be.visible");
      //Remove ingestion source and secret
      cy.goToIngestionPage();
      cy.get('[data-testid="delete-button"]').first().click();
      cy.waitTextVisible("Confirm Ingestion Source Removal");
      cy.get("button").contains("Yes").click();
      cy.waitTextVisible("Removed ingestion source.");
      cy.ensureTextNotPresent(ingestion_source_name)
      cy.clickOptionWithText("Secrets");
      cy.waitTextVisible(`secretname${number}`);
      cy.get('[data-icon="delete"]').first().click();
      cy.waitTextVisible("Confirm Secret Removal");
      cy.get("button").contains("Yes").click();
      cy.waitTextVisible("Removed secret.");
      cy.ensureTextNotPresent(`secretname${number}`);
      cy.ensureTextNotPresent(`secretdescription${number}`);    
    })
});