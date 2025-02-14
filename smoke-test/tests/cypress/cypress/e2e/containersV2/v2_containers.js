describe("containers", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });

  it("can see elements inside the container", () => {
    cy.login();
    cy.goToContainer("urn:li:container:348c96555971d3f5c1ffd7dd2e7446cb");
    cy.contains("jaffle_shop");
    cy.contains("customers");
    cy.contains("customers_source");
    cy.contains("orders");
    cy.contains("raw_orders");
    cy.contains("1 - 9 of 9");
  });
});
