const EXISTING_INCIDENT_TITLE = "test title";
const NEW_INCIDENT_VALUES = {
  NAME: "Incident new name",
  DESCRIPTION: "This is Description",
  TYPE: "Freshness",
  PRIORITY: "Critical",
  STAGE: "Investigation",
};
const EDITED_INCIDENT_VALUES = {
  NAME: "Edited Incident new name",
  DESCRIPTION: "Edited Description",
  PRIORITY: "High",
  STAGE: "In progress",
  TYPE: "Freshness",
  STATE: "Resolved",
};

describe("incidents", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });
  const newIncidentNameWithTimeStamp = `${NEW_INCIDENT_VALUES.NAME}-${Date.now()}`;
  const editedIncidentNameWithTimeStamp = `${newIncidentNameWithTimeStamp}-edited`;

  it("can view v1 incident", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset-v2,PROD)/Incidents",
    );
    cy.get(`[data-testid="incident-row-${EXISTING_INCIDENT_TITLE}"]`)
      .contains(EXISTING_INCIDENT_TITLE)
      .should("exist");
  });

  it("create a v2 incident with all fields set", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset-v2,PROD)/Incidents",
    );
    cy.get('[data-testid="create-incident-btn-main"]').click();
    cy.get('[data-testid="incident-name-input"]').type(
      newIncidentNameWithTimeStamp,
    );

    cy.get(".remirror-editor")
      .should("exist")
      .click({ force: true })
      .type(NEW_INCIDENT_VALUES.DESCRIPTION)
      .should("contain.text", NEW_INCIDENT_VALUES.DESCRIPTION);

    cy.get('[data-testid="category-select-input-type"]').click();
    cy.get('[data-testid="category-options-list"]')
      .contains(NEW_INCIDENT_VALUES.TYPE)
      .click();
    cy.get('[data-testid="priority-select-input-type"]').click();
    cy.get('[data-testid="priority-options-list"]')
      .contains(NEW_INCIDENT_VALUES.PRIORITY)
      .click();
    cy.get('[data-testid="stage-select-input-type"]').click();
    cy.get('[data-testid="stage-options-list"]')
      .contains(NEW_INCIDENT_VALUES.STAGE)
      .click();
    cy.get('[data-testid="incident-assignees-select-input-type"]').click();
    cy.get('[data-testid="incident-assignees-options-list"] label')
      .first()
      .click();

    cy.get('[data-testid="incident-editor-form-container"]')
      .children()
      .first()
      .click();
    cy.get('[data-testid="incident-create-button"]').click();
    // Wait for the incident to be created and appear in the list
    cy.get(`[data-testid="incident-row-${newIncidentNameWithTimeStamp}"]`, {
      timeout: 15000,
    }).should("exist");
    cy.get(`[data-testid="${newIncidentNameWithTimeStamp}"]`)
      .scrollIntoView()
      .should("be.visible");
    cy.get(
      `[data-testid="incident-row-${newIncidentNameWithTimeStamp}"]`,
    ).within(() => {
      cy.get('[data-testid="incident-stage"]')
        .invoke("text")
        .should("include", NEW_INCIDENT_VALUES.STAGE);
      cy.get('[data-testid="incident-category"]')
        .invoke("text")
        .should("include", NEW_INCIDENT_VALUES.TYPE);
      cy.get('[data-testid="incident-resolve-button-container"]')
        .should("be.visible")
        .should("contain", "Resolve");
    });
  });

  it("can update incident & resolve incident", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset-v2,PROD)/Incidents",
    );
    cy.get(`[data-testid="incident-row-${newIncidentNameWithTimeStamp}"]`)
      .should("exist")
      .click();

    // Wait for the incident details to load
    cy.get('[data-testid="edit-incident-icon"]', { timeout: 10000 })
      .should("be.visible")
      .click();

    cy.get('[data-testid="incident-name-input"]')
      .clear()
      .type(editedIncidentNameWithTimeStamp);
    cy.get(".remirror-editor")
      .should("exist")
      .click()
      .clear()
      .type(EDITED_INCIDENT_VALUES.DESCRIPTION)
      .should("contain.text", EDITED_INCIDENT_VALUES.DESCRIPTION);
    cy.get('[data-testid="priority-select-input-type"]').click();
    cy.get('[data-testid="priority-options-list"]')
      .contains(EDITED_INCIDENT_VALUES.PRIORITY)
      .click();
    cy.get('[data-testid="stage-select-input-type"]').click();
    cy.get('[data-testid="stage-options-list"]')
      .contains(EDITED_INCIDENT_VALUES.STAGE)
      .click();
    cy.get('[data-testid="incident-assignees-select-input-type"]').click();
    cy.get('[data-testid="incident-assignees-options-list"] label')
      .first()
      .click();

    cy.get('[data-testid="incident-editor-form-container"]')
      .children()
      .first()
      .click();
    cy.get('[data-testid="status-select-input-type"]').click();
    cy.get('[data-testid="status-options-list"]').contains("Resolved").click();
    cy.get('[data-testid="incident-create-button"]').click();
    // Wait for the incident to be updated and page to reload
    cy.get('[data-testid="nested-options-dropdown-container"]', {
      timeout: 15000,
    })
      .should("be.visible")
      .click();
    cy.get('[data-testid="child-option-RESOLVED"]').click();
    cy.get('[data-testid="nested-options-dropdown-container"]').click();
    cy.get('[data-testid="incident-group-HIGH"]').scrollIntoView();
    cy.get('[data-testid="incident-group-HIGH"]').within(() => {
      cy.get('[data-testid="group-header-collapsed-icon"]')
        .should(Cypress._.noop) // Prevent Cypress from failing if the element is missing
        .then(($icon) => {
          if ($icon.length > 0 && $icon.is(":visible")) {
            cy.wrap($icon).should("be.visible").click();
          } else {
            cy.log("Collapsed icon not found or not visible, skipping click");
          }
        });
    });
    cy.get(`[data-testid="incident-row-${editedIncidentNameWithTimeStamp}"]`)
      .scrollIntoView()
      .should("exist");
    cy.get(
      `[data-testid="incident-row-${editedIncidentNameWithTimeStamp}"]`,
    ).within(() => {
      cy.get('[data-testid="incident-stage"]')
        .invoke("text")
        .should("include", EDITED_INCIDENT_VALUES.STAGE);
      cy.get('[data-testid="incident-category"]')
        .invoke("text")
        .should("include", EDITED_INCIDENT_VALUES.TYPE);
      cy.get('[data-testid="incident-resolve-button-container"]').should(
        "contain.text",
        "Me",
      );
    });
  });

  it("Create V2 incident with all fields set and separate_siblings=false", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)/Incidents?is_lineage_mode=false&separate_siblings=false",
    );

    // Wait for loading to complete and use the correct test ID for sibling mode
    cy.findByTestId("create-incident-btn-main-with-siblings", {
      timeout: 10000,
    }).should("be.visible");

    // Click the button (this opens a dropdown when separate_siblings=false and siblings exist)
    cy.findByTestId("create-incident-btn-main-with-siblings").click();

    // For separate_siblings=false mode, we need to select a sibling from the dropdown first
    // Wait for dropdown to appear and select the first option
    cy.get(".ant-dropdown-menu-item", { timeout: 10000 })
      .first()
      .should("be.visible")
      .click();

    // Now the incident builder should open
    cy.get('[data-testid="drawer-header-title"]', { timeout: 30000 }).should(
      "contain.text",
      "Create New Incident",
    );
    cy.get('[data-testid="incident-name-input"]').type(
      newIncidentNameWithTimeStamp,
    );

    cy.get(".remirror-editor")
      .should("exist")
      .eq(1)
      .click({ force: true })
      .type(NEW_INCIDENT_VALUES.DESCRIPTION)
      .should("contain.text", NEW_INCIDENT_VALUES.DESCRIPTION);

    cy.get('[data-testid="category-select-input-type"]').click();
    cy.get('[data-testid="category-options-list"]')
      .contains(NEW_INCIDENT_VALUES.TYPE)
      .click();
    cy.get('[data-testid="priority-select-input-type"]').click();
    cy.get('[data-testid="priority-options-list"]')
      .contains(NEW_INCIDENT_VALUES.PRIORITY)
      .click();
    cy.get('[data-testid="stage-select-input-type"]').click();
    cy.get('[data-testid="stage-options-list"]')
      .contains(NEW_INCIDENT_VALUES.STAGE)
      .click();
    cy.get('[data-testid="incident-assignees-select-input-type"]').click();
    cy.get('[data-testid="incident-assignees-options-list"] label')
      .first()
      .click();

    cy.get('[data-testid="incident-editor-form-container"]')
      .children()
      .first()
      .click();
    cy.get('[data-testid="incident-create-button"]').click();
    // Wait for the incident to be created and incident groups to load
    cy.get('[data-testid="incident-group-CRITICAL"]', { timeout: 15000 })
      .should("exist")
      .scrollIntoView();
    cy.get('[data-testid="incident-group-CRITICAL"]').within(() => {
      cy.get('[data-testid="group-header-collapsed-icon"]')
        .should(Cypress._.noop) // Prevent Cypress from failing if the element is missing
        .then(($icon) => {
          if ($icon.length > 0 && $icon.is(":visible")) {
            cy.wrap($icon).should("be.visible").click();
          } else {
            cy.log("Collapsed icon not found or not visible, skipping click");
          }
        });
    });
    cy.get(
      `[data-testid="incident-row-${newIncidentNameWithTimeStamp}"]`,
    ).should("exist");
    cy.get(`[data-testid="${newIncidentNameWithTimeStamp}"]`)
      .scrollIntoView()
      .should("be.visible");
    cy.get(
      `[data-testid="incident-row-${newIncidentNameWithTimeStamp}"]`,
    ).within(() => {
      cy.get('[data-testid="incident-stage"]')
        .invoke("text")
        .should("include", NEW_INCIDENT_VALUES.STAGE);
      cy.get('[data-testid="incident-category"]')
        .invoke("text")
        .should("include", NEW_INCIDENT_VALUES.TYPE);
      cy.get('[data-testid="incident-resolve-button-container"]')
        .should("be.visible")
        .should("contain", "Resolve");
    });
  });

  it("Create V2 incident with all fields set and separate_siblings=true", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)/Incidents?is_lineage_mode=false&separate_siblings=true",
    );
    cy.get('[data-testid="create-incident-btn-main"]').click();
    cy.get('[data-testid="incident-name-input"]').type(
      `${newIncidentNameWithTimeStamp}-New`,
    );

    cy.get(".remirror-editor")
      .should("exist")
      .click({ force: true })
      .type(NEW_INCIDENT_VALUES.DESCRIPTION)
      .should("contain.text", NEW_INCIDENT_VALUES.DESCRIPTION);

    cy.get('[data-testid="category-select-input-type"]').click();
    cy.get('[data-testid="category-options-list"]')
      .contains(NEW_INCIDENT_VALUES.TYPE)
      .click();
    cy.get('[data-testid="priority-select-input-type"]').click();
    cy.get('[data-testid="priority-options-list"]')
      .contains(NEW_INCIDENT_VALUES.PRIORITY)
      .click();
    cy.get('[data-testid="stage-select-input-type"]').click();
    cy.get('[data-testid="stage-options-list"]')
      .contains(NEW_INCIDENT_VALUES.STAGE)
      .click();
    cy.get('[data-testid="incident-assignees-select-input-type"]').click();
    cy.get('[data-testid="incident-assignees-options-list"] label')
      .first()
      .click();

    cy.get('[data-testid="incident-editor-form-container"]')
      .children()
      .first()
      .click();
    cy.get('[data-testid="incident-create-button"]').click();
    // Wait for the incident to be created and incident groups to load
    cy.get('[data-testid="incident-group-CRITICAL"]', { timeout: 15000 })
      .should("exist")
      .scrollIntoView();
    cy.get('[data-testid="incident-group-CRITICAL"]').within(() => {
      cy.get('[data-testid="group-header-collapsed-icon"]')
        .should(Cypress._.noop) // Prevent Cypress from failing if the element is missing
        .then(($icon) => {
          if ($icon.length > 0 && $icon.is(":visible")) {
            cy.wrap($icon).should("be.visible").click();
          } else {
            cy.log("Collapsed icon not found or not visible, skipping click");
          }
        });
    });
    cy.get(
      `[data-testid="incident-row-${newIncidentNameWithTimeStamp}-New"]`,
    ).should("exist");
    cy.get(`[data-testid="${newIncidentNameWithTimeStamp}-New"]`)
      .scrollIntoView()
      .should("be.visible");
    cy.get(
      `[data-testid="incident-row-${newIncidentNameWithTimeStamp}-New"]`,
    ).within(() => {
      cy.get('[data-testid="incident-stage"]')
        .invoke("text")
        .should("include", NEW_INCIDENT_VALUES.STAGE);
      cy.get('[data-testid="incident-category"]')
        .invoke("text")
        .should("include", NEW_INCIDENT_VALUES.TYPE);
      cy.get('[data-testid="incident-resolve-button-container"]')
        .should("be.visible")
        .should("contain", "Resolve");
    });
  });
});
