import * as utils from "./utils";

describe("summary tab - about section", () => {
  beforeEach(() => {
    utils.setThemeV2AndSummaryTabFlags(true);
    cy.login();
    cy.goToDataProduct("urn:li:dataProduct:testing");
    utils.goToSummaryTab();
  });

  it("about section", () => {
    utils.testAboutSection();
  });
});
