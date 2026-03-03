import { hasOperationName } from "../utils";

const number = crypto.getRandomValues(new Uint32Array(1))[0];
const accound_id = `account${number}`;
const warehouse_id = `warehouse${number}`;
const username = `user${number}`;
const password = `password${number}`;
const role = `role${number}`;
const ingestion_source_name = `ingestion source ${number}`;

export const setIngestionRedesignFlag = (isOn) => {
  cy.setFeatureFlags(false, (res) => {
    res.body.data.appConfig.featureFlags.showIngestionPageRedesign = isOn;
  });
};

describe("ingestion source creation flow", () => {
  beforeEach(() => {
    setIngestionRedesignFlag(false); // Set the ingestion redesign flag to false
  });

  it("create a ingestion source using ui, verify ingestion source details saved correctly, remove ingestion source", () => {
    // Go to ingestion page, create a snowflake source
    cy.loginWithCredentials();
    cy.goToIngestionPage();
    cy.clickOptionWithId('[data-node-key="Sources"]');
    cy.clickOptionWithTestId("create-ingestion-source-button");
    cy.clickOptionWithTextToScrollintoView("Snowflake");
    cy.waitTextVisible("Snowflake Details");
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
    cy.waitTextVisible("Give this data source a name");
    cy.get('[data-testid="source-name-input"]').type(ingestion_source_name);
    cy.clickOptionWithTestId("ingestion-source-save-button");
    cy.waitTextVisible("Successfully created ingestion source!").wait(5000);
    cy.waitTextVisible(ingestion_source_name);
    cy.get('[data-testid="ingestion-source-table-status"]')
      .contains("Pending")
      .should("be.visible");

    // Verify ingestion source details are saved correctly
    cy.contains("tr", ingestion_source_name)
      .find('[data-testid="ingestion-source-table-edit-button"]')
      .click();
    cy.waitTextVisible("Edit Data Source");
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
    cy.get('[data-testid="source-name-input"]').clear();
    cy.get('[data-testid="source-name-input"]').type(
      `${ingestion_source_name} EDITED`,
    );
    cy.clickOptionWithTestId("ingestion-source-save-button");
    cy.waitTextVisible("Successfully updated ingestion source!");
    cy.waitTextVisible(`${ingestion_source_name} EDITED`);

    // Remove ingestion source
    cy.get(
      `[data-testid="delete-ingestion-source-${ingestion_source_name} EDITED"]`,
    ).click();
    cy.waitTextVisible("Confirm Ingestion Source Removal");
    cy.get("button").contains("Yes").click();
    cy.waitTextVisible("Removed ingestion source.");
    cy.ensureTextNotPresent(`${ingestion_source_name} EDITED`);
  });
});
