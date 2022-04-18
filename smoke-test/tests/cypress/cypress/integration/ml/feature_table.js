describe('features', () => {
    it('can visit feature tables and see features', () => {
      cy.visit('/')
      cy.login();
      cy.visit('/featureTables/urn:li:mlFeatureTable:(urn:li:dataPlatform:sagemaker,cypress-feature-table)/Features?is_lineage_mode=false');

      // the feature table descriptions should be there
      cy.contains('Yet another test feature group');
      cy.contains('this is a description from source system');

      // additional properties are visible
      cy.contains('CypressPrimaryKeyTag');
      cy.contains('CypressFeatureTag');

      // navigate to sources
      cy.contains('Sources').click();

      // feature & primary key sources are visible
      cy.contains('SampleCypressHdfsDataset');
      cy.contains('SampleCypressKafkaDataset');
    });

    it('can visit feature page', () => {
      cy.visit('/')
      cy.login();
      cy.visit('/features/urn:li:mlFeature:(cypress-test-2,some-cypress-feature-1)/Feature%20Tables?is_lineage_mode=false');

      // Shows the parent table
      cy.contains('cypress-feature-table');

      // Has upstream & downstream lineage
      cy.contains('1 upstream, 1 downstream');
    });

    it('can visit primary key page', () => {
      cy.visit('/')
      cy.login();
      cy.visit('/mlPrimaryKeys/urn:li:mlPrimaryKey:(cypress-test-2,some-cypress-feature-2)/Feature%20Tables?is_lineage_mode=false');

      // Shows the parent table
      cy.contains('cypress-feature-table');

      // Has upstream from its sources
      cy.contains('1 upstream, 0 downstream');
    });
})
