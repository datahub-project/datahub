const DATAFLOW_URN = "urn:li:dataFlow:(airflow,snowflake_etl,prod)";
const GLOSSARY_TERM_URN = "urn:li:glossaryTerm:CypressNode.CypressTerm";

describe("Search and Search Ranking Tests", () => {
  it("should return snowflake_etl on search `snowflake`", () => {
    cy.login();
    cy.visit("/search");
    cy.get(".ant-skeleton-input").should("not.exist");
    cy.get('input[data-testid="search-input"]').type("snowflake{enter}");
    cy.get(".ant-skeleton-input").should("not.exist");
    cy.get("#search-result-list ul.ant-list-items")
      .find("div")
      .first()
      .find(`[data-testid="preview-${DATAFLOW_URN}"]`)
      .should("exist");
  });

  it("should surface glossary terms", () => {
    cy.login();
    cy.visit("/search");
    cy.get(".ant-skeleton-input").should("not.exist");
    cy.get('input[data-testid="search-input"]').type("CypressTerm{enter}");
    cy.get(".ant-skeleton-input").should("not.exist");
    cy.get("#search-result-list ul.ant-list-items")
      .find("div")
      .first()
      .find(`[data-testid="preview-${GLOSSARY_TERM_URN}"]`)
      .should("exist");
  });
});
