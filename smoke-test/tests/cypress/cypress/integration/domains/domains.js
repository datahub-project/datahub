describe("domains", () => {
  it("can see elements inside the domain", () => {
    cy.login();
    cy.visit(
      "http://localhost:9002/domain/urn:li:domain:marketing/Entities?is_lineage_mode=false"
    );

    cy.contains("Marketing");
    cy.contains("SampleCypressKafkaDataset");
    cy.contains("1 - 1 of 1");
  });
});
