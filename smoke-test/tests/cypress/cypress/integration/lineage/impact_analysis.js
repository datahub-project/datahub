describe("impact analysis", () => {
  it("can see 1 hop of lineage by default", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)/Lineage?is_lineage_mode=false"
    );

    // impact analysis can take a beat- don't want to time out here
    cy.wait(5000);

    cy.contains("User Creations").should("not.exist");
    cy.contains("User Deletions").should("not.exist");
  });

  it("can see lineage multiple hops away", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)/Lineage?is_lineage_mode=false"
    );
    // click to show more relationships now that we default to 1 degree of dependency
    cy.contains("3+").click({ force: true });

    // impact analysis can take a beat- don't want to time out here
    cy.wait(5000);

    cy.contains("User Creations");
    cy.contains("User Deletions");
  });

  it("can filter the lineage results as well", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)/Lineage?is_lineage_mode=false"
    );
    // click to show more relationships now that we default to 1 degree of dependency
    cy.contains("3+").click({ force: true });

    cy.wait(5000);

    cy.contains("Advanced").click();

    cy.contains("Add Filter").click();

    // impact analysis can take a beat- don't want to time out here
    cy.wait(5000);

    cy.get('[data-testid="adv-search-add-filter-description"]').click({
      force: true,
    });

    cy.get('[data-testid="edit-text-input"]').type("fct_users_deleted");

    cy.get('[data-testid="edit-text-done-btn"]').click({ force: true });

    cy.contains("User Creations").should("not.exist");
    cy.contains("User Deletions");
  });
});
