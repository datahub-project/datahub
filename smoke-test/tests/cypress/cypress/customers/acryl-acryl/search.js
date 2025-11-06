const DATAFLOW_URN =
  "urn:li:dataset:(urn:li:dataPlatform:slack,C03BWQPEQ9H,PROD)";
const GLOSSARY_TERM_URN = "urn:li:tag:jmx";

describe("Search and Search Ranking Tests", () => {
  it("should return cypress-dashboard-notifications on search `cypress`", () => {
    cy.login();
    cy.visit("/search");
    cy.get(".ant-skeleton-input").should("not.exist");
    cy.get('input[data-testid="search-input"]').type("cypress{enter}");
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
    cy.get('input[data-testid="search-input"]').type("jmx{enter}");
    cy.get(".ant-skeleton-input").should("not.exist");
    cy.get("#search-result-list ul.ant-list-items")
      .find("div")
      .first()
      .find(`[data-testid="preview-${GLOSSARY_TERM_URN}"]`)
      .should("exist");
  });
});
