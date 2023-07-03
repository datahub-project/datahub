describe("domains", () => {
  it("can see elements inside the domain", () => {
    cy.login();
    cy.goToDomain("urn:li:domain:marketing/Entities");

    cy.contains("Marketing");
    cy.contains("SampleCypressKafkaDataset");
    cy.contains("1 - 1 of 1");
  });
});
