const DATASET_ENTITY_TYPE = 'dataset';
const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)';
const JAN_1_2021_TIMESTAMP = 1609553357755;
const JAN_1_2022_TIMESTAMP = 1641089357755;

describe("lineage_graph", () => {
    it("can see full history", () => {
      cy.login();
      cy.goToEntityLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);

      cy.contains("SampleCypressKafka");
      cy.contains("SampleCypressHdfs");
      cy.contains("Baz Chart 1");
      cy.contains("some-cypress");
    });

    it("cannot see any lineage edges for 2021", () => {
        cy.login();
        cy.goToEntityLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN, JAN_1_2021_TIMESTAMP, JAN_1_2022_TIMESTAMP);

        cy.contains("SampleCypressKafka");
        cy.contains("SampleCypressHdfs").should("not.exist");
        cy.contains("Baz Chart 1").should("not.exist");
        cy.contains("some-cypress").should("not.exist");
      });
  });
  