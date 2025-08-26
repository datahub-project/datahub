const WAU_URN =
  "urn:li:dataset:(urn:li:dataPlatform:dbt,FIGLYTICS.figma.wau,PROD)";
const GLOSSARY_TERM_URN = "urn:li:glossaryTerm:AccountBalance";

describe("Search invariants", () => {
  it("should return FIGLYTICS.figma.wau on search `wau`", () => {
    cy.login();
    cy.visit("/search");
    cy.get(".ant-skeleton-input").should("not.exist");
    cy.get('input[data-testid="search-input"]').type("wau{enter}");
    cy.get(".ant-skeleton-input").should("not.exist");
    cy.get("#search-result-list ul.ant-list-items")
      .find("div")
      .first()
      .find(`[data-testid="preview-${WAU_URN}"]`)
      .should("exist");
  });

  it("should surface glossary terms", () => {
    cy.login();
    cy.visit("/search");
    cy.get(".ant-skeleton-input").should("not.exist");
    cy.get('input[data-testid="search-input"]').type("AccountBalance{enter}");
    cy.get(".ant-skeleton-input").should("not.exist");
    cy.get("#search-result-list ul.ant-list-items")
      .find("div")
      .first()
      .find(`[data-testid="preview-${GLOSSARY_TERM_URN}"]`)
      .should("exist");
  });
});
