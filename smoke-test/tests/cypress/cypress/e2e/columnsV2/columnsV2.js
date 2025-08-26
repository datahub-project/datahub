const SAMPLE_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)";
const SAMPLE_DATASET_NAME = "SampleCypressHiveDataset";

const SAMPLE_COLUMN_NAME = "field_baz";
const SAMPLE_COLUMN_DESCRIPTION = "Baz field description";

describe("columns", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.loginWithCredentials();
  });
  it("should have correct descriptions", () => {
    cy.goToDataset(SAMPLE_DATASET_URN, SAMPLE_DATASET_NAME);

    // check field's description on the columns tab
    cy.getWithTestId(`column-${SAMPLE_COLUMN_NAME}`).contains(
      SAMPLE_COLUMN_DESCRIPTION,
    );

    // check field's description on the field's drawer
    cy.clickOptionWithTestId(`column-${SAMPLE_COLUMN_NAME}`);
    cy.getWithTestId("schema-field-drawer-content").within(() => {
      cy.getWithTestId("sidebar-section-content-Description").should(
        "contain",
        SAMPLE_COLUMN_DESCRIPTION,
      );
    });
  });
});
