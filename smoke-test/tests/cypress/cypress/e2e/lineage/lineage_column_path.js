import { aliasQuery } from "../utils";

const DATASET_ENTITY_TYPE = "dataset";
const DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)";
const DOWNSTREAM_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)";
const upstreamColumn =
  '[data-testid="node-urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)-Upstream"] text';
const downstreamColumn =
  '[data-testid="node-urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)-Downstream"] text';

const verifyColumnPathModal = (from, to) => {
  cy.get('[data-testid="entity-paths-modal"]')
    .contains(from)
    .should("be.visible");
  cy.get('[data-testid="entity-paths-modal"]')
    .contains(to)
    .should("be.visible");
};

describe("column-Level lineage and impact analysis path test", () => {
  beforeEach(() => {
    cy.on("uncaught:exception", (err, runnable) => false);
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  it("verify column-level lineage path at lineage praph and impact analysis ", () => {
    // Open dataset with column-level lineage configured an navigate to lineage tab -> visualize lineage
    cy.loginWithCredentials();
    cy.goToEntityLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);

    // Enable “show columns” toggle
    cy.waitTextVisible("SampleCypressHdfs");
    cy.clickOptionWithTestId("column-toggle");
    cy.waitTextVisible("shipment_info");

    // Verify functionality of column lineage
    cy.get(upstreamColumn).eq(3).click();
    cy.get(upstreamColumn)
      .eq(3)
      .prev()
      .should("not.have.attr", "fill", "white");
    cy.get(downstreamColumn)
      .eq(2)
      .prev()
      .should("not.have.attr", "stroke", "transparent");
    cy.get(downstreamColumn).eq(2).click();
    cy.get(downstreamColumn)
      .eq(2)
      .prev()
      .should("not.have.attr", "fill", "white");
    cy.get(upstreamColumn)
      .eq(3)
      .prev()
      .should("not.have.attr", "stroke", "transparent");

    // Open dataset impact analysis view, enable column lineage
    cy.goToDataset(DATASET_URN, "SampleCypressHdfsDataset");
    cy.openEntityTab("Lineage");
    cy.clickOptionWithText("Column Lineage");
    cy.clickOptionWithText("Downstream");

    // Verify upstream column lineage, test column path modal
    cy.clickOptionWithText("Upstream");
    cy.waitTextVisible("SampleCypressKafkaDataset");
    cy.ensureTextNotPresent("field_bar");
    cy.contains("Select column").click({ force: true }).wait(1000);
    cy.get(".rc-virtual-list").contains("shipment_info").click();
    cy.waitTextVisible("field_bar");
    cy.clickOptionWithText("field_bar");
    verifyColumnPathModal("shipment_info", "field_bar");
    cy.get('[data-testid="entity-paths-modal"] [data-icon="close"]').click();

    // Verify downstream column lineage, test column path modal
    cy.goToDataset(DOWNSTREAM_DATASET_URN, "SampleCypressKafkaDataset");
    cy.openEntityTab("Lineage");
    cy.clickOptionWithText("Column Lineage");
    cy.ensureTextNotPresent("shipment_info");
    cy.contains("Select column").click({ force: true }).wait(1000);
    cy.get(".rc-virtual-list").contains("field_bar").click();
    cy.waitTextVisible("shipment_info");
    cy.clickOptionWithText("shipment_info");
    verifyColumnPathModal("shipment_info", "field_bar");
    cy.get('[data-testid="entity-paths-modal"] [data-icon="close"]').click();
  });
});
