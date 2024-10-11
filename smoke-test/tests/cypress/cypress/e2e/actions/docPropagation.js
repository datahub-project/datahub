const testId = '[data-testid="docPropagationIndicator"]';

describe("docPropagation", () => {
  it("logs in and navigates to the schema page and checks for docPropagationIndicator", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_deleted,PROD)/Schema?is_lineage_mode=false&schemaFilter=",
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_deleted,PROD)/Schema?is_lineage_mode=false&schemaFilter=",
    );

    // verify that the indicator exists in the table
    cy.get(testId).should("exist");

    // click on the table row
    cy.get('[data-row-key="user_id"]').click();

    // verify that the indicator exists in id="entity-profile-sidebar"
    cy.get('[id="entity-profile-sidebar"]')
      .then(($sidebar) => {
        if ($sidebar.find(testId).length) return testId;
        return null;
      })
      .then((selector) => {
        cy.get(selector).should("exist");
      });
  });
});
