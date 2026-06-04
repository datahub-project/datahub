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

const showStructuredPropertyInAssetSummaryTab = (prop) => {
  cy.get('[data-testid="structured-props-table"]').contains(prop.name).click();
  cy.get(
    '[data-testid="structured-props-show-in-asset-summary-switch"]',
  ).click();
  cy.get('[data-testid="structured-props-create-update-button"]').click();
  cy.waitTextVisible(updateToastMessage);
};

const hideStructuredPropertyInAssetSummaryTabWhenEmpty = (prop) => {
  cy.getWithTestId("structured-props-table").contains(prop.name).click();
  cy.getWithTestId(
    "structured-props-hide-in-asset-summary-when-empty-checkbox",
  ).click({ force: true });
  cy.getWithTestId("structured-props-create-update-button").click();
  cy.waitTextVisible(updateToastMessage);
};

const openEntityAssetSummaryTab = () => {
  cy.get('[id="entity-sidebar-tabs-tab-Summary"]').then(($el) => {
    const isSelected = $el.attr("aria-selected") === "true";
    if (!isSelected) cy.wrap($el).click();
  });
};

const ensureStructuredPropertyIsAvailableInAssetSummaryTab = (prop) => {
  cy.get('[id="entity-profile-v2-sidebar"]').should("contain", prop.name);
};

const ensureStructuredPropertyIsNotAvailableInAssetSummaryTab = (prop) => {
  cy.get('[id="entity-profile-v2-sidebar"]').should("not.contain", prop.name);
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
    cy.login();
    cy.goToStructuredProperties();
  });

  it("Verify creating a new structured property", () => {
    cy.createStructuredProperty(structuredProperty);
    cy.waitTextVisible(createToastMessage);
    cy.waitTextVisible(structuredProperty.name);
    cy.deleteStructuredProperty(structuredProperty);
  });

  it("Verify updating an existing structured property", () => {
    cy.createStructuredProperty(structuredProperty);
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
    cy.deleteStructuredProperty(updatedStructuredProperty);
  });

  it("Verify deleting a structured property", () => {
    cy.createStructuredProperty(structuredProperty);
    cy.deleteStructuredProperty(structuredProperty);
    cy.waitTextVisible(deleteToastMessage);
    cy.contains(structuredProperty.name).should("not.exist");
  });

  it("Verify the absence of hidden structured property", () => {
    cy.createStructuredProperty(structuredProperty);
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
    cy.goToStructuredProperties();
    cy.deleteStructuredProperty(structuredProperty);
  });

  it("Verify adding a structured property to an entity", () => {
    cy.createStructuredProperty(structuredProperty);
    addStructuredPropertyToEntity(structuredProperty);
    cy.waitTextVisible(addToastMessage);
    cy.waitTextVisible(structuredProperty.name);
    cy.waitTextVisible(structuredPropStringValue);
    cy.goToStructuredProperties();
    cy.deleteStructuredProperty(structuredProperty);
  });

  it("Verify removing a structured property from an entity", () => {
    cy.createStructuredProperty(structuredProperty);
    addStructuredPropertyToEntity(structuredProperty);
    cy.contains("td", structuredPropStringValue)
      .siblings("td")
      .find('[data-testid="structured-prop-entity-more-icon"]')
      .click();
    cy.get("body .ant-dropdown-menu").contains("Remove").click();
    cy.get('[data-testid="modal-confirm-button"').click();
    cy.waitTextVisible(removeToastMessage);
    cy.contains(structuredPropStringValue).should("not.exist");
    cy.goToStructuredProperties();
    cy.deleteStructuredProperty(structuredProperty);
  });

  it("Verify editing a structured property on an entity", () => {
    cy.createStructuredProperty(structuredProperty);
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
    cy.goToStructuredProperties();
    cy.deleteStructuredProperty(structuredProperty);
  });

  it("Verify adding a structured property to a schema field", () => {
    cy.createStructuredProperty(fieldStructuredProperty);
    showStructuredPropertyInColumnsTable(fieldStructuredProperty);
    addStructuredPropertyToField(fieldStructuredProperty);
    cy.waitTextVisible(addToastMessage);
    cy.get(".ant-drawer-content").contains(structuredPropStringValue);
    cy.goToStructuredProperties();
    cy.deleteStructuredProperty(fieldStructuredProperty);
  });

  it("Verify updating a structured property on a schema field", () => {
    cy.createStructuredProperty(fieldStructuredProperty);
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
    cy.goToStructuredProperties();
    cy.deleteStructuredProperty(fieldStructuredProperty);
  });

  it("Verify removing a structured property from a schema field", () => {
    cy.createStructuredProperty(fieldStructuredProperty);
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
    cy.goToStructuredProperties();
    cy.deleteStructuredProperty(fieldStructuredProperty);
  });

  it("Verify property is available in asset summary tab when empty", () => {
    const property = {
      ...structuredProperty,
      name: "Property-showInSummaryTab",
    };

    cy.createStructuredProperty(property);
    showStructuredPropertyInAssetSummaryTab(property);
    cy.goToDataset(datasetUrn, datasetName);

    openEntityAssetSummaryTab();
    ensureStructuredPropertyIsAvailableInAssetSummaryTab(property);

    cy.goToStructuredProperties();
    cy.deleteStructuredProperty(property);
  });

  it("Verify property is not available in asset summary tab when it's empty", () => {
    const property = {
      ...structuredProperty,
      name: "Property-hideInSummaryTabWithoutValue",
    };
    cy.createStructuredProperty(property);
    showStructuredPropertyInAssetSummaryTab(property);
    cy.goToStructuredProperties();
    hideStructuredPropertyInAssetSummaryTabWhenEmpty(property);
    cy.goToDataset(datasetUrn, datasetName);

    openEntityAssetSummaryTab();
    ensureStructuredPropertyIsNotAvailableInAssetSummaryTab(property);

    cy.goToStructuredProperties();
    cy.deleteStructuredProperty(property);
  });

  it("Verify property is available in asset summary tab when it isn't empty", () => {
    const property = {
      ...structuredProperty,
      name: "Property-hideInSummaryTabWhenEmptyWithValue",
    };
    cy.createStructuredProperty(property);
    showStructuredPropertyInAssetSummaryTab(property);
    cy.goToStructuredProperties();
    hideStructuredPropertyInAssetSummaryTabWhenEmpty(property);
    cy.goToDataset(datasetUrn, datasetName);
    addStructuredPropertyToEntity(property);

    openEntityAssetSummaryTab();
    ensureStructuredPropertyIsAvailableInAssetSummaryTab(property);

    cy.goToStructuredProperties();
    cy.deleteStructuredProperty(property);
  });

  it("Verify single-select dropdown supports search/filter on allowed values", () => {
    const propName = "Cypress SingleSelect Search Test";
    const propQualifiedName = "cypress_singleselect_search_test";
    const allowedValues = [
      "Alpha",
      "Bravo",
      "Charlie",
      "Delta",
      "Echo",
      "Foxtrot",
    ];
    let createdUrn;

    // Create structured property with 6 allowed values via GraphQL (6 values triggers Select dropdown)
    cy.request({
      method: "POST",
      url: "/api/v2/graphql",
      body: {
        query: `
          mutation createStructuredProperty($input: CreateStructuredPropertyInput!) {
            createStructuredProperty(input: $input) {
              urn
            }
          }
        `,
        variables: {
          input: {
            id: propQualifiedName,
            qualifiedName: propQualifiedName,
            displayName: propName,
            valueType: "urn:li:dataType:datahub.string",
            cardinality: "SINGLE",
            entityTypes: ["urn:li:entityType:datahub.dataset"],
            allowedValues: allowedValues.map((v) => ({ stringValue: v })),
          },
        },
      },
    }).then((resp) => {
      expect(resp.status).to.eq(200);
      createdUrn = resp.body.data.createStructuredProperty.urn;
    });

    // Navigate to dataset and open Properties tab
    cy.goToDataset(datasetUrn, datasetName);
    cy.get('[data-testid="Properties-entity-tab-header"]').click();
    cy.get('[data-testid="add-structured-prop-button"]').click();

    // Select our test property from the dropdown menu
    cy.get("body .ant-dropdown-menu")
      .should("be.visible")
      .within(() => {
        cy.contains(propName).click();
      });

    // Open the single-select dropdown and verify the search input exists
    cy.get(".ant-select-selector").last().click();
    cy.get(".ant-select-dropdown")
      .should("be.visible")
      .within(() => {
        cy.get(".ant-select-item").should("have.length", allowedValues.length);
      });

    // Type a search term and verify filtering works
    cy.get(".ant-select-selection-search-input").last().type("Cha");
    cy.get(".ant-select-dropdown")
      .should("be.visible")
      .within(() => {
        cy.contains(".ant-select-item", "Charlie").should("be.visible");
        cy.contains(".ant-select-item", "Alpha").should("not.exist");
        cy.contains(".ant-select-item", "Foxtrot").should("not.exist");
      });

    // Select "Charlie" and confirm it is applied
    cy.get(".ant-select-dropdown")
      .contains(".ant-select-item", "Charlie")
      .click();
    cy.get(".ant-select-selector").last().should("contain.text", "Charlie");

    // Close modal without saving (escape key) to avoid polluting dataset data
    cy.get("body").type("{esc}");

    // Cleanup: delete the structured property via GraphQL
    cy.then(() => {
      cy.request({
        method: "POST",
        url: "/api/v2/graphql",
        body: {
          query: `
            mutation deleteStructuredProperty($input: DeleteStructuredPropertyInput!) {
              deleteStructuredProperty(input: $input)
            }
          `,
          variables: {
            input: { urn: createdUrn },
          },
        },
      }).then((resp) => {
        expect(resp.status).to.eq(200);
      });
    });
  });
});
