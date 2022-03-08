describe('mutations', () => {
  it('can create and add a tag to dataset and visit new tag page', () => {
    cy.login();
    cy.visit('/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)/Lineage?is_lineage_mode=false');
    cy.contains('Impact Analysis').click({ force: true });

    // impact analysis can take a beat- don't want to time out here
    cy.wait(5000);

    cy.contains('User Creations');
    cy.contains('User Deletions');
   });
});
