export function checkIfNodeExist(nodeUrn) {
  cy.getWithTestId(`lineage-node-${nodeUrn}`).should("exist");
}

export function checkIfNodeNotExist(nodeUrn) {
  cy.getWithTestId(`lineage-node-${nodeUrn}`).should("not.exist");
}

export function checkIfEdgeExist(node1Urn, node2Urn) {
  cy.getWithTestId(`rf__edge-${node1Urn}-:-${node2Urn}`).should("exist");
}

export function checkIfEdgeNotExist(node1Urn, node2Urn) {
  cy.getWithTestId(`rf__edge-${node1Urn}-:-${node2Urn}`).should("not.exist");
}

export function expandOne(nodeUrn) {
  cy.getWithTestId(`expand-one-${nodeUrn}-button`).click({
    force: true,
  });
}

export function expandAll(nodeUrn) {
  cy.getWithTestId(`expand-all-${nodeUrn}-button`).click({
    force: true,
  });
}

export function expandColumns(nodeUrn) {
  cy.getWithTestId(`lineage-node-${nodeUrn}`).within(() => {
    cy.clickOptionWithTestId("expand-contract-columns");
  });
}

export function hoverColumn(nodeUrn, columnName) {
  cy.getWithTestId(`lineage-node-${nodeUrn}`).within(() => {
    cy.getWithTestId(`column-${columnName}`).trigger("mouseover");
  });
}

export function unhoverColumn(nodeUrn, columnName) {
  cy.getWithTestId(`lineage-node-${nodeUrn}`).within(() => {
    cy.getWithTestId(`column-${columnName}`).trigger("mouseout");
  });
}

export function selectColumn(nodeUrn, columnName) {
  cy.getWithTestId(`lineage-node-${nodeUrn}`).within(() => {
    cy.clickOptionWithTestId(`column-${columnName}`);
  });
}

export function checkIfEdgeBetweenColumnsExist(
  node1Urn,
  column1Name,
  node2Urn,
  column2Name,
) {
  cy.getWithTestId(
    `rf__edge-${node1Urn}::${column1Name}-${node2Urn}::${column2Name}`,
  ).should("exist");
}

export function checkIfEdgeBetweenColumnsNotExist(
  node1Urn,
  column1Name,
  node2Urn,
  column2Name,
) {
  cy.getWithTestId(
    `rf__edge-${node1Urn}::${column1Name}-${node2Urn}::${column2Name}`,
  ).should("not.exist");
}
