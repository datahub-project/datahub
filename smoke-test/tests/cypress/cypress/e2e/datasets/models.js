import {
  addDatasetDocumentation,
  clearColumnsDescription,
  clickColumn,
  ensureColumnDescriptionInSidebar,
  ensureColumnDescriptionInSchemaTable,
  openDatasetTab,
  updateColumnDescription,
  ensureDatasetHasDocumentationInSidebar,
  clearDatasetDocumentation,
  ensureDatasetHasDocumentation,
} from "./utils";

const DATASET_NODEL_NAME = "cypress_model.model";
const DATASET_MODEL_URN =
  "urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_model.model,PROD)";
const COLUMN = "column";

describe("models", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.login();
    cy.skipIntroducePage();
  });

  it("should allow to set columns description ", () => {
    cy.goToDataset(DATASET_MODEL_URN, DATASET_NODEL_NAME);

    clickColumn(COLUMN);
    updateColumnDescription(`${COLUMN} description`);
    ensureColumnDescriptionInSchemaTable(COLUMN, `${COLUMN} description`);
    ensureColumnDescriptionInSidebar(`${COLUMN} description`);

    clearColumnsDescription();
  });

  it("should allow to set dataset documentation ", () => {
    cy.goToDataset(DATASET_MODEL_URN, DATASET_NODEL_NAME);
    openDatasetTab("Documentation");

    addDatasetDocumentation("model documentation");
    ensureDatasetHasDocumentationInSidebar("model documentation");
    ensureDatasetHasDocumentation("model documentation");

    clearDatasetDocumentation();
  });
});
