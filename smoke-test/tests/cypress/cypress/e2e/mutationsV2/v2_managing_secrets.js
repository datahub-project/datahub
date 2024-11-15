const number = Math.floor(Math.random() * 100000);
const accound_id = `account${number}`;
const warehouse_id = `warehouse${number}`;
const username = `user${number}`;
const password = `password${number}`;
const role = `role${number}`;
const ingestion_source_name = `ingestion source ${number}`;

describe("managing secrets for ingestion creation", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });
  it("create a secret, create ingestion source using a secret, remove a secret", () => {
    // Navigate to the manage ingestion page → secrets
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.goToIngestionPage();
    // cy.openEntityTab("Secrets");

    // Create a new secret
    cy.get('[data-node-key="Secrets"]').click();
    cy.clickOptionWithTestId("create-secret-button");
    cy.enterTextInTestId("secret-modal-name-input", `secretname${number}`);
    cy.enterTextInTestId("secret-modal-value-input", `secretvalue${number}`);
    cy.enterTextInTestId(
      "secret-modal-description-input",
      `secretdescription${number}`,
    );
    cy.clickOptionWithTestId("secret-modal-create-button");
    cy.waitTextVisible("Successfully created Secret!");
    cy.waitTextVisible(`secretname${number}`);
    cy.waitTextVisible(`secretdescription${number}`).wait(5000);

    // Create an ingestion source using a secret
    cy.goToIngestionPage();
    cy.get('[data-node-key="Sources"]').click();
    cy.get("#ingestion-create-source").click();
    cy.get('[placeholder="Search data sources..."]').type("snowflake");
    cy.clickOptionWithText("Snowflake");
    cy.waitTextVisible("Account");
    cy.get("#account_id").type(accound_id);
    cy.get("#warehouse").type(warehouse_id);
    cy.get("#username").type(username);
    cy.get("#password").click().wait(1000);
    cy.contains(`secretname${number}`).click({ force: true });
    cy.focused().blur();
    cy.get("#role").type(role);
    cy.get("button").contains("Next").click();
    cy.waitTextVisible("Configure an Ingestion Schedule");
    cy.get("button").contains("Next").click();
    cy.get(".ant-collapse-item").should("be.visible");
    cy.get('[data-testid="source-name-input"]').type(ingestion_source_name);
    cy.get("button").contains("Save").click();
    cy.waitTextVisible("Successfully created ingestion source!").wait(5000);
    cy.waitTextVisible(ingestion_source_name);
    cy.get("button").contains("Pending...").should("be.visible");

    // Remove a secret
    cy.openEntityTab("Secrets");
    cy.waitTextVisible(`secretname${number}`);
    cy.get('[data-icon="delete"]').first().click();
    cy.waitTextVisible("Confirm Secret Removal");
    cy.get("button").contains("Yes").click();
    cy.waitTextVisible("Removed secret.");
    cy.ensureTextNotPresent(`secretname${number}`);
    cy.ensureTextNotPresent(`secretdescription${number}`);

    // Remove ingestion source
    cy.goToIngestionPage();
    cy.get('[data-node-key="Sources"]').click();
    cy.get(
      `[data-testid="delete-ingestion-source-${ingestion_source_name}"]`,
    ).click();
    cy.waitTextVisible("Confirm Ingestion Source Removal");
    cy.get("button").contains("Yes").click();
    cy.waitTextVisible("Removed ingestion source.");
    cy.ensureTextNotPresent(ingestion_source_name);

    // Verify secret is not present during ingestion source creation for password dropdown
    cy.clickOptionWithText("Create new source");
    cy.get('[placeholder="Search data sources..."]').type("snowflake");
    cy.clickOptionWithText("Snowflake");
    cy.waitTextVisible("Account");
    cy.get("#account_id").type(accound_id);
    cy.get("#warehouse").type(warehouse_id);
    cy.get("#username").type(username);
    cy.get("#password").click().wait(1000);
    cy.ensureTextNotPresent(`secretname${number}`);

    // Verify secret can be added during ingestion source creation and used successfully
    cy.clickOptionWithText("Create Secret");
    cy.enterTextInTestId("secret-modal-name-input", `secretname${number}`);
    cy.enterTextInTestId("secret-modal-value-input", `secretvalue${number}`);
    cy.enterTextInTestId(
      "secret-modal-description-input",
      `secretdescription${number}`,
    );
    cy.clickOptionWithTestId("secret-modal-create-button");
    cy.waitTextVisible("Created secret!");
    cy.get("#role").type(role);
    cy.get("button").contains("Next").click();
    cy.waitTextVisible("Configure an Ingestion Schedule");
    cy.get("button").contains("Next").click();
    cy.get(".ant-collapse-item").should("be.visible");
    cy.get('[data-testid="source-name-input"]').type(ingestion_source_name);
    cy.get("button").contains("Save").click();
    cy.waitTextVisible("Successfully created ingestion source!").wait(5000); // prevent issue with missing form data
    cy.waitTextVisible(ingestion_source_name);
    cy.get("button").contains("Pending...").should("be.visible");

    // Remove ingestion source and secret
    cy.goToIngestionPage();
    cy.get('[data-node-key="Sources"]').click();
    cy.get(
      `[data-testid="delete-ingestion-source-${ingestion_source_name}"]`,
    ).click();
    cy.waitTextVisible("Confirm Ingestion Source Removal");
    cy.get("button").contains("Yes").click();
    cy.waitTextVisible("Removed ingestion source.");
    cy.ensureTextNotPresent(ingestion_source_name);
    cy.clickOptionWithText("Secrets");
    cy.waitTextVisible(`secretname${number}`);
    cy.get('[data-icon="delete"]').first().click();
    cy.waitTextVisible("Confirm Secret Removal");
    cy.get("button").contains("Yes").click();
    cy.waitTextVisible("Removed secret.");
    cy.ensureTextNotPresent(`secretname${number}`);
    cy.ensureTextNotPresent(`secretdescription${number}`);
  });
});
