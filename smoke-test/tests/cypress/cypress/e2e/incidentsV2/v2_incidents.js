describe("incidents", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
  });
  it("can view incidents and resolve an incident", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset,PROD)/Incidents",
    );
    cy.waitTextVisible("1 active incidents, 0 resolved incidents");
    cy.get("body").click();
    cy.clickOptionWithTestId("resolve-incident");
    cy.waitTextVisible("Resolve Incident");
    cy.clickOptionWithTestId("confirm-resolve");
    cy.get(".ant-select-selection-item").click();
    cy.get(".ant-typography").contains("All").click({ force: true });
    cy.waitTextVisible("0 active incidents, 1 resolved incidents");
  });

  it("can re-open a closed incident", () => {
    cy.login();
    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset,PROD)/Incidents",
    );
    cy.get(".ant-typography").should("be.visible");
    cy.waitTextVisible("0 active incidents, 1 resolved incidents");
    cy.get("body").click();
    cy.get(".ant-select-selection-item").click();
    cy.get(".ant-typography").contains("All").click({ force: true });
    cy.waitTextVisible("0 active incidents, 1 resolved incidents");
    cy.clickOptionWithTestId("incident-menu");
    cy.clickOptionWithTestId("reopen-incident");
    cy.waitTextVisible("1 active incidents, 0 resolved incidents");
  });
});
