const JAN_1_2021_TIMESTAMP = 1609553357755;
const JAN_1_2022_TIMESTAMP = 1641089357755;
const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)';


const startAtDataSetLineage = () => {
    cy.login();
    cy.goToDataset(
      DATASET_URN,
      "SampleCypressKafkaDataset"
    );
    cy.openEntityTab("Lineage")
}

describe("impact analysis", () => {
  it("can see 1 hop of lineage by default", () => {
    startAtDataSetLineage()

    cy.ensureTextNotPresent("User Creations");
    cy.ensureTextNotPresent("User Deletions");
  });

  it("can see lineage multiple hops away", () => {
    startAtDataSetLineage()
    // click to show more relationships now that we default to 1 degree of dependency
    cy.clickOptionWithText("3+");

    cy.contains("User Creations");
    cy.contains("User Deletions");
  });

  it("can filter the lineage results as well", () => {
    startAtDataSetLineage()
    // click to show more relationships now that we default to 1 degree of dependency
    cy.clickOptionWithText("3+");

    cy.clickOptionWithText("Advanced");

    cy.clickOptionWithText("Add Filter");

    cy.clickOptionWithTestId('adv-search-add-filter-description');

    cy.get('[data-testid="edit-text-input"]').type("fct_users_deleted");

    cy.clickOptionWithTestId('edit-text-done-btn');

    cy.ensureTextNotPresent("User Creations");
    cy.waitTextVisible("User Deletions");
  });

  it("can view column level impact analysis and turn it off", () => {
    cy.login();
    cy.visit(
      `/dataset/${DATASET_URN}/Lineage?column=%5Bversion%3D2.0%5D.%5Btype%3Dboolean%5D.field_bar&is_lineage_mode=false`
    );

    // impact analysis can take a beat- don't want to time out here
    cy.wait(5000);

    cy.contains("SampleCypressHdfsDataset");
    cy.contains("Downstream column: shipment_info");
    cy.contains("some-cypress-feature-1").should("not.exist");
    cy.contains("Baz Chart 1").should("not.exist");

    // find button to turn off column-level impact analysis
    cy.get('[data-testid="column-lineage-toggle"]').click({ force: true });

    cy.wait(2000);

    cy.contains("SampleCypressHdfsDataset");
    cy.contains("Downstream column: shipment_info").should("not.exist");
    cy.contains("some-cypress-feature-1");
    cy.contains("Baz Chart 1");
  });


  it("can filter lineage edges by time", () => {
    cy.login();
    cy.visit(
      `/dataset/${DATASET_URN}/Lineage?filter_degree___false___EQUAL___0=1&is_lineage_mode=false&page=1&unionType=0&start_time_millis=${JAN_1_2021_TIMESTAMP}&end_time_millis=${JAN_1_2022_TIMESTAMP}`
    );

    // impact analysis can take a beat- don't want to time out here
    cy.wait(5000);

    cy.contains("SampleCypressHdfsDataset").should("not.exist");
    cy.contains("Downstream column: shipment_info").should("not.exist");
    cy.contains("some-cypress-feature-1").should("not.exist");
    cy.contains("Baz Chart 1").should("not.exist");
  });
});
