const number = Math.floor(Math.random() * 100000);
const accound_id = `account${number}`;
const warehouse_id = `warehouse${number}`;
const username = `user${number}`;
const password = `password${number}`;
const role = `role${number}`;
const ingestion_source_name = `ingestion source ${number}`;

describe("ingestion source creation flow", () => {
  beforeEach(() => {
    cy.setFeatureFlags(true, (res) => {
      res.body.data.appConfig.featureFlags.showIngestionPageRedesign = false;
    });
  });
  it("create a ingestion source using ui, verify ingestion source details saved correctly, remove ingestion source", () => {
    // Go to ingestion page, create a snowflake source
    cy.login();
    cy.skipIntroducePage();
    cy.goToIngestionPage();
    cy.get('[data-node-key="Sources"]').click();
    cy.contains("Loading ingestion sources...").should("not.exist");
    cy.clickOptionWithTestId("create-ingestion-source-button");
    cy.get('[placeholder="Search data sources..."]').type("snowflake");
    cy.clickOptionWithText("Snowflake");
    cy.waitTextVisible("Account");
    cy.get("#account_id").type(accound_id);
    cy.get("#warehouse").type(warehouse_id);
    cy.get("#username").type(username);
    // Select Username & Password authentication to make password field visible
    cy.get("#authentication_type").click({ force: true });
    cy.get('.ant-select-dropdown [title="Username & Password"]').click();
    cy.get("#password").type(password);
    cy.focused().blur();
    cy.get("#role").type(role);

    // Verify yaml recipe is generated correctly
    cy.clickOptionWithTestId("recipe-builder-yaml-button");
    cy.waitTextVisible("account_id");
    cy.waitTextVisible(accound_id);
    cy.waitTextVisible(warehouse_id);
    cy.waitTextVisible(username);
    cy.waitTextVisible(password);
    cy.waitTextVisible(role);

    // Finish creating source
    cy.clickOptionWithTestId("recipe-builder-next-button");
    cy.waitTextVisible("Configure an Ingestion Schedule");
    cy.clickOptionWithTestId("ingestion-schedule-next-button");
    cy.get(".ant-collapse-item").should("be.visible");
    cy.get('[data-testid="source-name-input"]').type(ingestion_source_name);
    cy.clickOptionWithTestId("ingestion-source-save-button");
    cy.waitTextVisible("Successfully created ingestion source!").wait(5000);
    cy.waitTextVisible(ingestion_source_name);
    cy.get('[data-testid="ingestion-source-table-status"]')
      .contains("Pending")
      .should("be.visible");

    // Verify ingestion source details are saved correctly
    // Find the row containing our ingestion source by name, then click its edit button
    cy.contains("tr", ingestion_source_name)
      .find("button")
      .contains("EDIT")
      .click();

    cy.waitTextVisible("Account");
    cy.get("#account_id").should("have.value", accound_id);
    cy.get("#warehouse").should("have.value", warehouse_id);
    cy.get("#username").should("have.value", username);
    // Verify authentication type is set correctly (should infer from password presence)
    cy.get("#authentication_type")
      .parents(".ant-form-item")
      .should("contain", "Username & Password");
    cy.get("#password").should("have.value", password);
    cy.get("#role").should("have.value", role);
    cy.get("button").contains("Next").click();
    cy.waitTextVisible("Configure an Ingestion Schedule");
    cy.clickOptionWithTestId("ingestion-schedule-next-button");
    cy.get('[data-testid="source-name-input"]')
      .clear()
      .type(`${ingestion_source_name} EDITED`);
    cy.clickOptionWithTestId("ingestion-source-save-button");
    cy.waitTextVisible("Successfully updated ingestion source!");
    cy.waitTextVisible(`${ingestion_source_name} EDITED`);

    // Remove ingestion source
    cy.contains("tr", `${ingestion_source_name} EDITED`)
      .find('[data-icon="delete"]')
      .first()
      .click();
    cy.waitTextVisible("Confirm Ingestion Source Removal");
    cy.get("button").contains("Yes").click();
    cy.waitTextVisible("Removed ingestion source.");
    cy.ensureTextNotPresent(`${ingestion_source_name} EDITED`);
  });
});
