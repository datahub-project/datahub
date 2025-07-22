const DATASET_ENTITY_TYPE = "dataset";
const DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)";

const expandContractColumns = (asset) => {
  cy.contains(".react-flow__node-lineage-entity", asset)
    .find(`[data-testid="expand-contract-columns"]`)
    .click({ force: true });
};

describe("column-level lineage graph test", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });

  it("navigate to lineage graph view and verify that column-level lineage is showing correctly", () => {
    cy.login();
    cy.goToEntityLineageGraphV2(DATASET_ENTITY_TYPE, DATASET_URN);
    cy.wait(2000);
    // verify columns not shown by default
    cy.waitTextVisible("SampleCypressHdfs");
    cy.waitTextVisible("SampleCypressHive");
    cy.waitTextVisible("cypress_logging");
    cy.ensureTextNotPresent("shipment_info");
    cy.ensureTextNotPresent("field_foo");
    cy.ensureTextNotPresent("field_baz");
    cy.ensureTextNotPresent("event_name");
    cy.ensureTextNotPresent("event_data");
    cy.ensureTextNotPresent("timestamp");
    cy.ensureTextNotPresent("browser");
    expandContractColumns("SampleCypressHdfsDataset");
    cy.waitTextVisible("shipment_info");
    cy.waitTextVisible("shipment_info.date");
    cy.waitTextVisible("shipment_info.target");
    cy.waitTextVisible("shipment_info.destination");
    cy.waitTextVisible("shipment_info.geo_info");
    expandContractColumns("SampleCypressHiveDataset");
    cy.waitTextVisible("field_foo");
    cy.waitTextVisible("field_baz");
    expandContractColumns("cypress_logging_events");
    cy.waitTextVisible("event_name");
    cy.waitTextVisible("event_data");
    cy.waitTextVisible("timestamp");
    cy.waitTextVisible("browser");
    expandContractColumns("SampleCypressHiveDataset");
    cy.ensureTextNotPresent("field_foo");
    cy.ensureTextNotPresent("field_baz");
    expandContractColumns("SampleCypressHdfsDataset");
    expandContractColumns("cypress_logging_events");
    expandContractColumns("SampleCypressHiveDataset");
    cy.waitTextVisible("field_foo");
    cy.waitTextVisible("field_baz");
    cy.ensureTextNotPresent("shipment_info");
    cy.ensureTextNotPresent("event_name");
    cy.ensureTextNotPresent("event_data");
    cy.ensureTextNotPresent("timestamp");
    cy.ensureTextNotPresent("browser");
  });
});
