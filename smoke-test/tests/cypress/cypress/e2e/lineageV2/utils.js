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

export function contract(nodeUrn) {
  cy.clickOptionWithTestId(`contract-${nodeUrn}-button`);
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

function getFilteringNode(nodeUrn, direction) {
  const directionSign = direction === "up" ? "u" : "d";
  return cy.getWithTestId(`rf__node-lf:${directionSign}:${nodeUrn}`);
}

export function checkFilteringNodeExist(nodeUrn, direction) {
  getFilteringNode(nodeUrn, direction).should("exist");
}

export function showMore(nodeUrn, direction) {
  getFilteringNode(nodeUrn, direction).within(() => {
    cy.clickOptionWithTestId("show-more");
  });
}

export function showAll(nodeUrn, direction) {
  getFilteringNode(nodeUrn, direction).within(() => {
    cy.clickOptionWithTestId("show-all");
  });
}

export function showLess(nodeUrn, direction) {
  getFilteringNode(nodeUrn, direction).within(() => {
    cy.clickOptionWithTestId("show-less");
  });
}

export function filter(nodeUrn, direction, query) {
  getFilteringNode(nodeUrn, direction).within(() => {
    cy.getWithTestId("search-input")
      .clear({ force: true })
      .type(query, { force: true });
  });
}

export function clearFilter(nodeUrn, direction) {
  getFilteringNode(nodeUrn, direction).within(() => {
    cy.getWithTestId("search-input").clear({ force: true });
  });
}

export function ensureTitleHasText(nodeUrn, direction, text) {
  getFilteringNode(nodeUrn, direction).within(() => {
    cy.getWithTestId("title").should("have.text", text);
  });
}

export function checkMatches(nodeUrn, direction, matchesNumber) {
  getFilteringNode(nodeUrn, direction).within(() => {
    cy.getWithTestId("matches").should("have.text", `${matchesNumber} matches`);
  });
}

export function checkCounter(
  nodeUrn,
  direction,
  couterSection,
  counterType,
  value,
) {
  getFilteringNode(nodeUrn, direction).within(() => {
    cy.getWithTestId(`filter-counter-${couterSection}-${counterType}`).should(
      "have.text",
      `${value}`,
    );
  });
}
