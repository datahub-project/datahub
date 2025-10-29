import * as utils from "./utils";

const USER_DISPLAY_NAME = Cypress.env("ADMIN_DISPLAY_NAME");

const TEST_ASSET_NAME = "SampleCypressHiveDataset";
const TEST_ASSET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)";

const TEST_DOMAIN = "Testing";
const TEST_TERM = "CypressTerm";
const TEST_TAG = "Cypress";

function getTestId() {
  return `summary_${new Date().getTime()}`;
}

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

// Domain Setup Functions
function setupDomainForTests() {
  const testId = getTestId();
  const domainName = `domain_${testId}`;
  const childDomainName = `child_domain_${testId}`;
  const dataProductName = `dp_${testId}`;

  utils.createDomain(domainName);
  utils.openDomain(domainName);
  utils.createChildDomain(childDomainName, domainName);
  utils.addAsset("DOMAIN", TEST_ASSET_NAME, TEST_ASSET_URN);
  utils.createDataProduct(domainName, dataProductName);

  return { domainName, childDomainName, dataProductName, testId };
}

// Glossary Node Setup Functions
function setupGlossaryNodeForTests() {
  const testId = getTestId();
  const nodeName = `node_${testId}`;
  const termName = `term_${testId}`;

  utils.createGlossaryNode(nodeName);
  utils.openGlossaryNode(nodeName);
  utils.createGlossaryTerm(nodeName, termName);

  return { nodeName, termName, testId };
}

// Glossary Term Setup Functions
function setupGlossaryTermForTests() {
  const testId = getTestId();
  const nodeName = `term_node_${testId}`;
  const termName = `term_${testId}`;

  utils.createGlossaryNode(nodeName);
  utils.createGlossaryTerm(nodeName, termName);
  utils.openGlossaryTerm(termName);
  utils.setDomainToOpenedEntity(TEST_DOMAIN);
  utils.addAsset("GLOSSARY_TERM", TEST_ASSET_NAME, TEST_ASSET_URN);
  utils.addRelatedTerm(TEST_TERM);

  return { nodeName, termName, testId };
}

// Data Product Setup Functions
function setupDataProductForTests() {
  const testId = getTestId();
  const dataProductName = `dp_${testId}`;

  utils.createDataProduct(TEST_DOMAIN, dataProductName);
  utils.openDataProduct(TEST_DOMAIN, dataProductName);
  utils.addAsset("DATA_PRODUCT", TEST_ASSET_NAME, TEST_ASSET_URN);
  utils.setGlossaryTermToOpenedEntity(TEST_TERM);
  utils.setTagToOpenedEntity(TEST_TAG);

  return { dataProductName, testId };
}

describe("summary tab - domain", () => {
  let cleanupData = {};

  beforeEach(() => {
    utils.setThemeV2AndSummaryTabFlags(true);
    cy.login();
    const setupData = setupDomainForTests();
    cleanupData = setupData;
    utils.ensureSummaryTabIsAvailable();
    utils.goToSummaryTab();
    cy.reload();
  });

  afterEach(() => {
    utils.openDataProduct(cleanupData.domainName, cleanupData.dataProductName);
    utils.deleteOpenedDataProduct();
    utils.deleteDomain(cleanupData.childDomainName);
    utils.deleteDomain(cleanupData.domainName);
  });

  it.skip("domain - header section", () => {
    testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS", value: USER_DISPLAY_NAME },
    ]);
  });

  it.skip("domain - description section", () => {
    testAboutSection();
  });

  it.skip("domain - modules section", () => {
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
        value: cleanupData.childDomainName,
      },
      {
        type: "data-products",
        name: "Data Products",
        value: cleanupData.dataProductName,
      },
    ];
    const modulesAvailableToAdd = [
      ...defaultModules,
      ...ADDITIONAL_MODULES_AVAILABLE_TO_ADD,
    ];

    testTemplateSection(defaultModules, modulesAvailableToAdd);
  });
});

describe("summary tab - glossary node", () => {
  let cleanupData = {};

  beforeEach(() => {
    utils.setThemeV2AndSummaryTabFlags(true);
    cy.login();
    const setupData = setupGlossaryNodeForTests();
    cleanupData = setupData;
    utils.goToSummaryTab();
    cy.reload();
    utils.ensureSummaryTabIsAvailable();
  });

  afterEach(() => {
    utils.deleteOpenedGlossaryNode();
    utils.openGlossaryTerm(cleanupData.termName);
    utils.deleteOpenedGLossaryTerm();
  });

  it.skip("glossary node - header section", () => {
    testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS", value: USER_DISPLAY_NAME },
    ]);
  });

  it.skip("glossary node - description section", () => {
    testAboutSection();
  });

  it.skip("glossary node - modules section", () => {
    const defaultModules = [
      {
        type: "hierarchy",
        // FYI: Contents module has different type in add module menu
        addType: "child-hierarchy",
        name: "Contents",
        value: cleanupData.termName,
      },
    ];
    const modulesAvailableToAdd = [
      ...defaultModules,
      ...ADDITIONAL_MODULES_AVAILABLE_TO_ADD,
    ];

    testTemplateSection(defaultModules, modulesAvailableToAdd);
  });
});

describe("summary tab - glossary term", () => {
  let cleanupData = {};

  beforeEach(() => {
    utils.setThemeV2AndSummaryTabFlags(true);
    cy.login();
    const setupData = setupGlossaryTermForTests();
    cleanupData = setupData;
    utils.goToSummaryTab();
    cy.reload();
    utils.ensureSummaryTabIsAvailable();
  });

  afterEach(() => {
    utils.deleteOpenedGLossaryTerm();
    utils.openGlossaryNode(cleanupData.nodeName);
    utils.deleteOpenedGlossaryNode();
  });

  it.skip("glossary term - header section", () => {
    testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS", value: USER_DISPLAY_NAME },
      { name: "Domain", type: "DOMAIN", value: TEST_DOMAIN },
    ]);
  });

  it.skip("glossary term - description section", () => {
    testAboutSection();
  });

  it.skip("glossary term - modules section", () => {
    const defaultModules = [
      {
        type: "assets",
        name: "Assets",
        value: TEST_ASSET_NAME,
      },
      {
        type: "related-terms",
        name: "Related Terms",
        value: TEST_TERM,
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
  let cleanupData = {};

  beforeEach(() => {
    utils.setThemeV2AndSummaryTabFlags(true);
    cy.login();
    const setupData = setupDataProductForTests();
    cleanupData = setupData;
    utils.ensureSummaryTabIsAvailable();
  });

  afterEach(() => {
    utils.deleteOpenedDataProduct();
  });

  it.skip("data product - header section", () => {
    testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS", value: USER_DISPLAY_NAME },
      { name: "Domain", type: "DOMAIN", value: TEST_DOMAIN },
      { name: "Tags", type: "TAGS", value: TEST_TAG },
      { name: "Glossary Terms", type: "GLOSSARY_TERMS", value: TEST_TERM },
    ]);
  });

  it.skip("data product - description section", () => {
    testAboutSection();
  });

  it.skip("data product - modules section", () => {
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

    testTemplateSection(defaultModules, modulesAvailableToAdd);
  });
});
