import * as utils from "./utils";

const TEST_DOMAIN_NAME = "Testing";
const TEST_TAG_NAME = "Cypress";
const TEST_TERM_NAME = "CypressTerm";
const TEST_ASSET_NAME = "Baz Dashboard"; // Used in the assets module

describe("summary tab - data product", () => {
  beforeEach(() => {
    utils.setThemeV2AndSummaryTabFlags(true);
    cy.login();
    cy.goToDataProduct("urn:li:dataProduct:testing");
    utils.goToSummaryTab();
  });

  it("data product - header section", () => {
    utils.testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS" },
      { name: "Domain", type: "DOMAIN", value: TEST_DOMAIN_NAME },
      { name: "Tags", type: "TAGS", value: TEST_TAG_NAME },
      { name: "Glossary Terms", type: "GLOSSARY_TERMS", value: TEST_TERM_NAME },
    ]);
  });

  it("data product - description section", () => {
    utils.testAboutSection();
  });

  it("data product - modules section", () => {
    const defaultModules = [
      {
        type: "assets",
        name: "Assets",
        value: TEST_ASSET_NAME,
      },
    ];

    utils.testTemplateSection(defaultModules);
  });
});
