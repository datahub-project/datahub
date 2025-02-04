const urn =
  "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)";

describe("schema blame", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });
  Cypress.on("uncaught:exception", (err, runnable) => false);

  it("can activate the blame view and verify for the latest version of a dataset", () => {
    cy.login();
    cy.visit(`/dataset/${urn}`);
    cy.get("#column-field_baz").should("be.visible");
    cy.get("body").click();

    // Verify which fields are present, along with checking descriptions and tags
    cy.contains("field_foo").should("be.visible");
    cy.contains("field_baz").should("be.visible");
    cy.contains("field_bar").should("not.exist");
    cy.contains("Foo field description has changed");
    cy.contains("Baz field description").should("exist");
    cy.clickOptionWithText("field_foo").should("exist");
    cy.contains("Legacy").should("exist");
    cy.get(".ant-select-selection-item").click();
    cy.contains(/1\.0\.0\s*-/).click({ force: true });

    // Make sure the schema blame is accurate
    cy.get('[data-testid="schema-blame-button"]').click({ force: true });
    cy.contains("field_bar").should("be.visible");
    cy.contains("field_foo").should("be.visible");
    cy.contains("Bar field description").should("be.visible");
    cy.contains("Foo field description").should("be.visible");

    // Verify History table tab
    cy.contains("History").should("be.visible");
  });

  it("can activate the blame view and verify for an older version of a dataset", () => {
    cy.login();
    cy.visit(`/dataset/${urn}`);
    cy.get("#column-field_baz").should("be.visible");
    cy.get("body").click();
    cy.get(".ant-select-selection-item")
      .should("be.visible")
      .click({ force: true });
    cy.contains(/0\.0\.0\s*-/).click({ force: true });
    // Verify which fields are present, along with checking descriptions and tags
    cy.contains("field_foo").should("be.visible");
    cy.contains("field_bar").should("be.visible");
    cy.contains("field_baz").should("not.exist");
    cy.contains("Foo field description").should("exist");
    cy.contains("Bar field description").should("exist");
    cy.clickOptionWithText("field_foo");
    cy.get('[data-testid="schema-field-field_foo-tags"]')
      .contains("Legacy")
      .should("not.exist");
    cy.get(".anticon-file-text").click();
  });
});
