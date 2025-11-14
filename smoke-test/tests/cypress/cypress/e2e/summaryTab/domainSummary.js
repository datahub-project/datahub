import * as utils from "./utils";

const TEST_USER_URN = "urn:li:corpuser:jdoe";
const TEST_ASSET_NAME = "Baz Dashboard";
const TEST_DOMAIN_URN = "urn:li:domain:testing";
const TEST_SUBDOMAIN_NAME = "Subdomain";
const TEST_DATA_PRODUCT_NAME = "Testing";

describe("summary tab - domain", () => {
  beforeEach(() => {
    utils.setThemeV2AndSummaryTabFlags(true);
    cy.login();
    utils.openDomainByUrn(TEST_DOMAIN_URN);
    utils.goToSummaryTab();
  });

  it("domain - header section", () => {
    utils.testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS", dataTestId: `owner-${TEST_USER_URN}` },
    ]);
  });

  it("domain - description section", () => {
    utils.testAboutSection();
  });

  it("domain - modules section", () => {
    const defaultModules = [
      {
        type: "assets",
        name: "Assets",
        value: TEST_ASSET_NAME,
      },
      {
        type: "hierarchy",
        // FYI: Domains module has different type in add module menu
        addType: "child-hierarchy",
        name: "Domains",
        value: TEST_SUBDOMAIN_NAME,
      },
      {
        type: "data-products",
        name: "Data Products",
        value: TEST_DATA_PRODUCT_NAME,
      },
    ];

    utils.testTemplateSection(defaultModules);
  });
});
