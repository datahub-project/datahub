const startAtDataSetLineage = () => {
    cy.login();
    cy.goToDataset(
      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)",
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
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)/Lineage?column=%5Bversion%3D2.0%5D.%5Btype%3Dboolean%5D.field_bar&is_lineage_mode=false"
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
});
