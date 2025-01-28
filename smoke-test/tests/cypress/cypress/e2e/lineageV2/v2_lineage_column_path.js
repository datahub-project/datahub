import { aliasQuery } from "../utils";

const DATASET_ENTITY_TYPE = "dataset";
const DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)";
const DOWNSTREAM_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)";
const upstreamColumn =
  '[data-testid="rf__node-urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)"]';
const downstreamColumn =
  '[data-testid="rf__node-urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleCypressHdfsDataset,PROD)"]';

const verifyColumnPathModal = (from, to) => {
  cy.get('[data-testid="entity-paths-modal"]')
    .contains(from)
    .should("be.visible");
  cy.get('[data-testid="entity-paths-modal"]')
    .contains(to)
    .should("be.visible");
};

const clickDownAndUpArrow = (asset, arrow) => {
  cy.contains(".react-flow__node-lineage-entity", asset)
    .find(`svg[data-testid="${arrow}"]`)
    .click();
};

describe("column-Level lineage and impact analysis path test", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.on("uncaught:exception", (err, runnable) => false);
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  it("verify column-level lineage path at lineage praph and impact analysis ", () => {
    // Open dataset with column-level lineage configured an navigate to lineage tab -> visualize lineage
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    cy.goToEntityLineageGraphV2(DATASET_ENTITY_TYPE, DATASET_URN);

    // Enable “show columns” toggle
    cy.waitTextVisible("SampleCypressHdfs");
    clickDownAndUpArrow("SampleCypressHdfsDataset", "KeyboardArrowDownIcon");
    cy.waitTextVisible("shipment_info");

    // Verify functionality of column lineage
    clickDownAndUpArrow("SampleCypressKafkaDataset", "KeyboardArrowDownIcon");
    cy.get(upstreamColumn).contains("field_bar").click();
    cy.get(upstreamColumn)
      .contains("field_bar")
      .prev()
      .should("not.have.attr", "fill", "white");
    cy.get(downstreamColumn)
      .contains("shipment_info")
      .prev()
      .should("not.have.attr", "stroke", "transparent");
    cy.get(downstreamColumn).contains("shipment_info").click();
    cy.get(downstreamColumn)
      .contains("shipment_info")
      .prev()
      .should("not.have.attr", "fill", "white");
    cy.get(upstreamColumn)
      .contains("field_bar")
      .prev()
      .should("not.have.attr", "stroke", "transparent");

    // Open dataset impact analysis view, enable column lineage
    cy.goToDataset(DATASET_URN, "SampleCypressHdfsDataset");
    cy.get('[data-node-key="Lineage"]').first().should("be.visible").click();
    cy.contains("Impact Analysis").click();
    cy.clickOptionWithText("Column Lineage");
    cy.clickOptionWithText("Downstream");

    // Verify upstream column lineage, test column path modal
    cy.clickOptionWithText("Upstream");
    cy.waitTextVisible("SampleCypressKafkaDataset");
    cy.ensureTextNotPresent("field_bar");
    cy.contains("Select column").click({ force: true }).wait(1000);
    cy.get(".rc-virtual-list").contains("shipment_info").click();
    cy.contains("field_bar").should("exist");
    cy.get(".ant-list-items").find(".anticon-layout").click({ force: true });
    cy.get('[class*="DisplayedColumns').first().click({ force: true });
    verifyColumnPathModal("shipment_info", "field_bar");
    cy.get('[data-testid="entity-paths-modal"] [data-icon="close"]').click();

    // Verify downstream column lineage, test column path modal
    cy.goToDataset(DOWNSTREAM_DATASET_URN, "SampleCypressKafkaDataset");
    cy.get('[data-node-key="Lineage"]').first().should("be.visible").click();
    cy.contains("Impact Analysis").click();
    cy.clickOptionWithText("Column Lineage");
    cy.ensureTextNotPresent("shipment_info");
    cy.contains("Select column").click({ force: true }).wait(1000);
    cy.get(".rc-virtual-list").contains("field_bar").click();
    cy.contains("shipment_info").should("exist");
    cy.get(".ant-list-items")
      .find(".anticon-layout")
      .first()
      .click({ force: true });
    cy.get('[class*="DisplayedColumns').first().click({ force: true });
    verifyColumnPathModal("shipment_info", "field_bar");
    cy.get('[data-testid="entity-paths-modal"] [data-icon="close"]').click();
  });
});
