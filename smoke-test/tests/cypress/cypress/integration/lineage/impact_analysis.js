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
});
