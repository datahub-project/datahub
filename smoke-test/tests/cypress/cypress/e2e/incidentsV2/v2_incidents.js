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
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset,PROD)/Incidents",
    );
    cy.get(`[data-testid="incident-row-${EXISTING_INCIDENT_TITLE}"]`)
      .contains(EXISTING_INCIDENT_TITLE)
      .should("exist");
  });

  it("create a v2 incident with all fields set", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset,PROD)/Incidents",
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
    cy.wait(3000);
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

  it("can update incident & resolve incident", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset,PROD)/Incidents",
    );
    cy.get(`[data-testid="incident-row-${newIncidentNameWithTimeStamp}"]`)
      .should("exist")
      .click();

    cy.wait(1000);

    cy.get('[data-testid="edit-incident-icon"]').click();

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
    cy.wait(3000);
    cy.get('[data-testid="nested-options-dropdown-container"]').click();
    cy.get('[data-testid="child-option-RESOLVED"]').click();
    cy.get('[data-testid="nested-options-dropdown-container"]').click();
    cy.get('[data-testid="incident-group-HIGH"]').scrollIntoView();
    cy.get('[data-testid="incident-group-HIGH"]').within(() => {
      cy.get('[data-testid="group-header-collapsed-icon"]')
        .should(Cypress._.noop) // Prevent Cypress from failing if the element is missing
        .then(($icon) => {
          if ($icon.length > 0 && $icon.is(":visible")) {
            cy.wrap($icon).click();
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
});
