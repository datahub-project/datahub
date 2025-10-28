import * as utils from "./utils";

const TEST_USER_DISPLAY_NAME = "jdoe";
const TEST_ASSET_NAME = "Baz Dashboard";
const TEST_DOMAIN_NAME = "Testing";
const TEST_DOMAIN_URN = "urn:li:domain:testing";
const TEST_SUBDOMAIN_NAME = "Subdomain";
const TEST_DATA_PRODUCT_NAME = "Testing";
const TEST_GLOSSARY_NODE_URN = "urn:li:glossaryNode:CypressNode";
const TEST_GLOSSARY_TERM_URN = "urn:li:glossaryTerm:CypressNode.CypressTerm";
const TEST_GLOSSARY_TERM_NAME = "CypressTerm";
const TEST_TERM_NAME = "CypressTerm";
const TEST_RELATED_TERM_NAME = "RelatedCypressTerm";
const TEST_TAG_NAME = "Cypress";

function testPropertiesSection(properties) {
  const propertyTypes = properties.map((property) => property.type);
  utils.ensurePropertiesSectionIsVisible();
  utils.ensurePropertiesAreVisible(propertyTypes); // default properties

  properties.forEach((property) => utils.ensurePropertyExist(property));
}

function testAboutSection() {
  utils.ensureAboutSectionIsVisible();
  utils.updateDescription("description");
  utils.ensureDescriptionContainsText("description");
  utils.updateDescription("updated description");
  utils.ensureDescriptionContainsText("updated description");
  utils.addLink("https://test.com", "testLink");
  utils.ensureLinkExists("https://test.com", "testLink");
  utils.updateLink(
    "https://test.com",
    "testLink",
    "https://test-updated.com",
    "testLinkUpdated",
  );
  utils.ensureLinkExists("https://test-updated.com", "testLinkUpdated");
  utils.removeLink("https://test-updated.com", "testLinkUpdated");
  utils.ensureLinkDoesNotExist("https://test-updated.com", "testLinkUpdated");
}

function testTemplateSection(defaultModules, modulesAvailableToAdd) {
  utils.ensureTemplateSectionIsVisible();

  // Check default modules
  defaultModules.forEach((module) => utils.ensureModuleExist(module));
}

const ADDITIONAL_MODULES_AVAILABLE_TO_ADD = [
  {
    type: "asset-collection",
    name: "Collection",
  },
  {
    type: "documentation",
    name: "Documentation",
  },
  {
    type: "hierarchy",
    name: "Hierarchy",
  },
];

describe("summary tab - domain", () => {
  beforeEach(() => {
    utils.setThemeV2AndSummaryTabFlags(true);
    cy.login();
  });

  it("domain - header section", () => {
    utils.openDomainByUrn(TEST_DOMAIN_URN);
    utils.goToSummaryTab();

    testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS", value: TEST_USER_DISPLAY_NAME },
    ]);
  });

  it("domain - description section", () => {
    utils.openDomainByUrn(TEST_DOMAIN_URN);
    utils.goToSummaryTab();
    testAboutSection();
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
    const modulesAvailableToAdd = [
      ...defaultModules,
      ...ADDITIONAL_MODULES_AVAILABLE_TO_ADD,
    ];

    utils.openDomainByUrn(TEST_DOMAIN_URN);
    utils.goToSummaryTab();

    testTemplateSection(defaultModules, modulesAvailableToAdd);
  });
});

describe("summary tab - glossary node", () => {
  beforeEach(() => {
    utils.setThemeV2AndSummaryTabFlags(true);
    cy.login();
  });

  it("glossary node - header section", () => {
    cy.goToGlossaryNode(TEST_GLOSSARY_NODE_URN);

    testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS", value: TEST_USER_DISPLAY_NAME },
    ]);
  });

  it("glossary node - description section", () => {
    cy.goToGlossaryNode(TEST_GLOSSARY_NODE_URN);

    testAboutSection();
  });

  it("glossary node - modules section", () => {
    const defaultModules = [
      {
        type: "hierarchy",
        // FYI: Contents module has different type in add module menu
        addType: "child-hierarchy",
        name: "Contents",
        value: TEST_GLOSSARY_TERM_NAME,
      },
    ];
    const modulesAvailableToAdd = [
      ...defaultModules,
      ...ADDITIONAL_MODULES_AVAILABLE_TO_ADD,
    ];

    cy.goToGlossaryNode(TEST_GLOSSARY_NODE_URN);

    testTemplateSection(defaultModules, modulesAvailableToAdd);
  });
});

describe("summary tab - glossary term", () => {
  beforeEach(() => {
    utils.setThemeV2AndSummaryTabFlags(true);
    cy.login();
    cy.goToGlossaryTerm(TEST_GLOSSARY_TERM_URN);
  });

  it("glossary term - header section", () => {
    testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS", value: TEST_USER_DISPLAY_NAME },
      { name: "Domain", type: "DOMAIN", value: TEST_DOMAIN_NAME },
    ]);
  });

  it("glossary term - description section", () => {
    testAboutSection();
  });

  it("glossary term - modules section", () => {
    const defaultModules = [
      {
        type: "assets",
        name: "Assets",
        value: TEST_DOMAIN_NAME,
      },
      {
        type: "related-terms",
        name: "Related Terms",
        value: TEST_RELATED_TERM_NAME,
      },
    ];
    const modulesAvailableToAdd = [
      ...defaultModules,
      ...ADDITIONAL_MODULES_AVAILABLE_TO_ADD,
    ];

    testTemplateSection(defaultModules, modulesAvailableToAdd);
  });
});

describe("summary tab - data product", () => {
  beforeEach(() => {
    utils.setThemeV2AndSummaryTabFlags(true);
    cy.login();
  });

  it("data product - header section", () => {
    cy.goToDataProduct("urn:li:dataProduct:testing");

    testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS" },
      { name: "Domain", type: "DOMAIN", value: TEST_DOMAIN_NAME },
      { name: "Tags", type: "TAGS", value: TEST_TAG_NAME },
      { name: "Glossary Terms", type: "GLOSSARY_TERMS", value: TEST_TERM_NAME },
    ]);
  });

  it("data product - description section", () => {
    cy.goToDataProduct("urn:li:dataProduct:testing");

    testAboutSection();
  });

  it("data product - modules section", () => {
    const defaultModules = [
      {
        type: "assets",
        name: "Assets",
        value: TEST_ASSET_NAME,
      },
    ];
    const modulesAvailableToAdd = [
      ...defaultModules,
      ...ADDITIONAL_MODULES_AVAILABLE_TO_ADD,
    ];

    cy.goToDataProduct("urn:li:dataProduct:testing");
    utils.goToSummaryTab();

    testTemplateSection(defaultModules, modulesAvailableToAdd);
  });
});
