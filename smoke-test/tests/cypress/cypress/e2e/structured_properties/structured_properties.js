const structuredProperty = {
  name: "Cypress Structured Prop",
  entity: "Dataset",
};

const updatedStructuredProperty = {
  name: "Updated Cypress Structured Prop",
  description: "Description of cypress property",
  entity: "Dashboard",
};

const fieldStructuredProperty = {
  name: "Field Structured property",
  entity: "Column",
};

const datasetUrn =
  "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)";
const datasetName = "SampleCypressHdfsDataset";

const structuredPropStringValue = "Cypress structured prop value";

const updatedStructuredPropStringValue =
  "Updated Cypress structured prop value";

const createToastMessage = "created";
const updateToastMessage = "updated";
const deleteToastMessage = "deleted";
const addToastMessage = "added";
const removeToastMessage = "removed";

const fieldName = "shipment_info";

const goToStructuredProperties = () => {
  cy.visit("/structured-properties");
  cy.waitTextVisible("Structured Properties");
};

const createStructuredProperty = (prop) => {
  cy.get('[data-testid="structured-props-create-button"').click();
  cy.get('[data-testid="structured-props-input-name"]').click().type(prop.name);
  cy.get('[data-testid="structured-props-select-input-type"]').click();
  cy.get('[data-testid="structured-props-property-type-options-list"]')
    .contains("Text")
    .click();
  cy.get('[data-testid="structured-props-select-input-applies-to"]').click();
  cy.get('[data-testid="applies-to-options-list"]')
    .contains(prop.entity)
    .click();
  cy.get('[data-testid="structured-props-select-input-applies-to"]').click();
  cy.get('[data-testid="structured-props-create-update-button"]').click();
};

const deleteStructuredProperty = (prop) => {
  cy.contains("td", prop.name)
    .siblings("td")
    .find('[data-testid="structured-props-more-options-icon"]')
    .click();
  cy.get("body .ant-dropdown-menu").contains("Delete").click();
  cy.get('[data-testid="modal-confirm-button"').click();
};

const addStructuredPropertyToEntity = (prop) => {
  cy.goToDataset(datasetUrn, datasetName);
  // Resize Observer Loop warning can be safely ignored - ref. https://github.com/cypress-io/cypress/issues/22113
  cy.get('[data-testid="Properties-entity-tab-header"]').click();
  cy.get('[data-testid="add-structured-prop-button"]').click();
  cy.get("body .ant-dropdown-menu")
    .should("be.visible")
    .within(() => {
      cy.contains(prop.name).click();
    });
  cy.get('[data-testid="structured-property-string-value-input"]')
    .click()
    .type(structuredPropStringValue);
  cy.get('[data-testid="add-update-structured-prop-on-entity-button"]').click();
};

const showStructuredPropertyInColumnsTable = (prop) => {
  cy.get('[data-testid="structured-props-table"]').contains(prop.name).click();
  cy.get(
    '[data-testid="structured-props-show-in-columns-table-switch"]',
  ).click();
  cy.get('[data-testid="structured-props-create-update-button"]').click();
  cy.waitTextVisible(updateToastMessage);
};

const addStructuredPropertyToField = (prop) => {
  cy.goToDataset(datasetUrn, datasetName);
  cy.contains(fieldName);
  cy.clickOptionWithText(fieldName);
  cy.get(`[data-testid="${prop.name}-add-or-edit-button"]`).click();
  cy.get('[data-testid="structured-property-string-value-input"]')
    .click()
    .type(structuredPropStringValue);
  cy.get('[data-testid="add-update-structured-prop-on-entity-button"]').click();
};

describe("Verify manage structured properties functionalities", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.login();
    goToStructuredProperties();
  });

  it("Verify creating a new structured property", () => {
    createStructuredProperty(structuredProperty);
    cy.waitTextVisible(createToastMessage);
    cy.waitTextVisible(structuredProperty.name);
    deleteStructuredProperty(structuredProperty);
  });

  it("Verify updating an existing structured property", () => {
    createStructuredProperty(structuredProperty);
    cy.get('[data-testid="structured-props-table"]')
      .contains(structuredProperty.name)
      .click();
    cy.waitTextVisible(structuredProperty.name);
    cy.waitTextVisible(structuredProperty.entity);
    cy.get('[data-testid="structured-props-input-name"]')
      .clear()
      .type(updatedStructuredProperty.name);
    cy.get('[data-testid="structured-props-input-description"]')
      .click()
      .type(updatedStructuredProperty.description);
    cy.get('[data-testid="structured-props-select-input-applies-to"]').click();
    cy.get('[data-testid="applies-to-options-list"]')
      .contains(updatedStructuredProperty.entity)
      .click();
    cy.get('[data-testid="structured-props-create-update-button"]').click();
    cy.waitTextVisible(updateToastMessage);
    cy.waitTextVisible(updatedStructuredProperty.name);
    cy.waitTextVisible(updatedStructuredProperty.description);
    cy.waitTextVisible(updatedStructuredProperty.entity);
    deleteStructuredProperty(updatedStructuredProperty);
  });

  it("Verify deleting a structured property", () => {
    createStructuredProperty(structuredProperty);
    deleteStructuredProperty(structuredProperty);
    cy.waitTextVisible(deleteToastMessage);
    cy.contains(structuredProperty.name).should("not.exist");
  });

  it("Verify the absence of hidden structured property", () => {
    createStructuredProperty(structuredProperty);
    cy.get('[data-testid="structured-props-table"]')
      .contains(structuredProperty.name)
      .click();
    cy.get('[data-testid="structured-props-hide-switch"]').click();
    cy.get('[data-testid="structured-props-create-update-button"]').click();
    cy.goToDataset(datasetUrn, datasetName);
    cy.get('[data-testid="Properties-entity-tab-header"]').click();
    cy.get('[data-testid="add-structured-prop-button"]').click();
    cy.get("body .ant-dropdown-menu")
      .should("be.visible")
      .within(() => {
        cy.contains(structuredProperty.name).should("not.exist");
      });
    goToStructuredProperties();
    deleteStructuredProperty(structuredProperty);
  });

  it("Verify adding a structured property to an entity", () => {
    createStructuredProperty(structuredProperty);
    addStructuredPropertyToEntity(structuredProperty);
    cy.waitTextVisible(addToastMessage);
    cy.waitTextVisible(structuredProperty.name);
    cy.waitTextVisible(structuredPropStringValue);
    goToStructuredProperties();
    deleteStructuredProperty(structuredProperty);
  });

  it("Verify removing a structured property from an entity", () => {
    createStructuredProperty(structuredProperty);
    addStructuredPropertyToEntity(structuredProperty);
    cy.contains("td", structuredPropStringValue)
      .siblings("td")
      .find('[data-testid="structured-prop-entity-more-icon"]')
      .click();
    cy.get("body .ant-dropdown-menu").contains("Remove").click();
    cy.get('[data-testid="modal-confirm-button"').click();
    cy.waitTextVisible(removeToastMessage);
    cy.contains(structuredPropStringValue).should("not.exist");
    goToStructuredProperties();
    deleteStructuredProperty(structuredProperty);
  });

  it("Verify editing a structured property on an entity", () => {
    createStructuredProperty(structuredProperty);
    addStructuredPropertyToEntity(structuredProperty);
    cy.contains("td", structuredPropStringValue)
      .siblings("td")
      .find('[data-testid="structured-prop-entity-more-icon"]')
      .click();
    cy.get("body .ant-dropdown-menu").contains("Edit").click();
    cy.get('[data-testid="structured-property-string-value-input"]')
      .clear()
      .type(updatedStructuredPropStringValue);
    cy.get(
      '[data-testid="add-update-structured-prop-on-entity-button"]',
    ).click();
    cy.waitTextVisible(updateToastMessage);
    cy.waitTextVisible(updatedStructuredPropStringValue);
    goToStructuredProperties();
    deleteStructuredProperty(structuredProperty);
  });

  it("Verify adding a structured property to a schema field", () => {
    createStructuredProperty(fieldStructuredProperty);
    showStructuredPropertyInColumnsTable(fieldStructuredProperty);
    addStructuredPropertyToField(fieldStructuredProperty);
    cy.waitTextVisible(addToastMessage);
    cy.get(".ant-drawer-content").contains(structuredPropStringValue);
    goToStructuredProperties();
    deleteStructuredProperty(fieldStructuredProperty);
  });

  it("Verify updating a structured property on a schema field", () => {
    createStructuredProperty(fieldStructuredProperty);
    showStructuredPropertyInColumnsTable(fieldStructuredProperty);
    addStructuredPropertyToField(fieldStructuredProperty);
    cy.get(
      `[data-testid="${fieldStructuredProperty.name}-add-or-edit-button"]`,
    ).click();
    cy.get('[data-testid="structured-property-string-value-input"]')
      .clear()
      .type(updatedStructuredPropStringValue);
    cy.get(
      '[data-testid="add-update-structured-prop-on-entity-button"]',
    ).click();
    cy.waitTextVisible(updateToastMessage);
    cy.get(".ant-drawer-content").contains(updatedStructuredPropStringValue);
    goToStructuredProperties();
    deleteStructuredProperty(fieldStructuredProperty);
  });

  it("Verify removing a structured property from a schema field", () => {
    createStructuredProperty(fieldStructuredProperty);
    showStructuredPropertyInColumnsTable(fieldStructuredProperty);
    addStructuredPropertyToField(fieldStructuredProperty);
    cy.get('[data-testid="Properties-field-drawer-tab-header"]').click();
    cy.contains("td", fieldStructuredProperty.name)
      .siblings("td")
      .find('[data-testid="structured-prop-entity-more-icon"]')
      .click();
    cy.get("body .ant-dropdown-menu").contains("Remove").click();
    cy.get('[data-testid="modal-confirm-button"').click();
    cy.waitTextVisible(removeToastMessage);
    cy.get(".ant-drawer-content")
      .contains(structuredPropStringValue)
      .should("not.exist");
    goToStructuredProperties();
    deleteStructuredProperty(fieldStructuredProperty);
  });
});
