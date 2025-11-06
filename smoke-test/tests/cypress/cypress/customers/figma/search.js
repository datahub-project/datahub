const WAU_URN =
  "urn:li:dataset:(urn:li:dataPlatform:dbt,FIGLYTICS.figma.wau,PROD)";
const WAU_URN_ALT =
  "urn:li:dataset:(urn:li:dataPlatform:dbt,dwh_core.product_usage.wau,PROD)";
const GLOSSARY_TERM_URN = "urn:li:glossaryTerm:AccountBalance";

describe("Search and Search Ranking test", () => {
  it("should return FIGLYTICS.figma.wau or dwh_core.product_usage.wau on search `wau`", () => {
    cy.login();
    cy.visit("/search");
    cy.get(".ant-skeleton-input").should("not.exist");
    cy.get('input[data-testid="search-input"]').type("wau{enter}");
    cy.get(".ant-skeleton-input").should("not.exist");
    cy.get("#search-result-list ul.ant-list-items")
      .find("div")
      .first()
      .then(($firstResult) => {
        // Check that the first result is one of the two WAU URNs
        const hasOriginalUrn =
          $firstResult.find(`[data-testid="preview-${WAU_URN}"]`).length > 0;
        const hasAltUrn =
          $firstResult.find(`[data-testid="preview-${WAU_URN_ALT}"]`).length >
          0;
        expect(hasOriginalUrn || hasAltUrn).to.be.true;
      });
  });

  //  it("should surface glossary terms", () => {
  //    cy.login();
  //    cy.visit("/search");
  //    cy.get(".ant-skeleton-input").should("not.exist");
  //    cy.get('input[data-testid="search-input"]').type("AccountBalance{enter}");
  //    cy.get(".ant-skeleton-input").should("not.exist");
  //    cy.get("#search-result-list ul.ant-list-items")
  //      .find("div")
  //      .first()
  //      .find(`[data-testid="preview-${GLOSSARY_TERM_URN}"]`)
  //      .should("exist");
  //  });
});
