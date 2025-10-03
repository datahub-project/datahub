import * as utils from "./utils";

const USERNAME = Cypress.env("ADMIN_USERNAME");

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

  // SaaS-only tests
  // // Remove properties
  // propertyTypes.forEach((propertyType) => utils.removeProperty(propertyType));
  // utils.ensurePropertiesDoNotExist(propertyTypes); // properties were removed
  // utils.ensurePropertiesAvailableToAdd(propertyTypes);

  // // created and owners properties available on all entities right now
  // // Add and replace properties
  // utils.addProperty("CREATED");
  // utils.ensurePropertiesAreVisible(["CREATED"]);
  // utils.replaceProperty("CREATED", "OWNERS");
  // utils.ensurePropertiesAreVisible(["OWNERS"]);
  // utils.ensurePropertiesDoNotExist(["CREATED"]);
}

function testPropertiesSectionStructuredProperties(
  structuredPropertyName,
  structuredPropertyValue,
) {
  utils.addStructuredPropertyToOpenedEntity(
    structuredPropertyName,
    structuredPropertyValue,
  );
  utils.addStructuredPropertyToProperties(structuredPropertyName);
  utils.ensureStructuredPropertyExists(
    structuredPropertyName,
    structuredPropertyValue,
  );
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

  // SaaS-only tests
  // // Delete modules
  // defaultModules.forEach((module) => utils.deleteModule(module));
  // defaultModules.forEach((module) => utils.ensureModuleDoesNotExist(module));

  // // Check modules available to add
  // utils.ensureModulesAvailableToAdd(modulesAvailableToAdd);

  // // Add the first default module
  // const moduleToAdd = defaultModules[0];
  // utils.addModule(moduleToAdd);
  // utils.ensureModuleExist(moduleToAdd);
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

describe("summary tab", () => {
  beforeEach(() => {
    utils.setThemeV2AndSummaryTabFlags(true);
    cy.login();
  });

  it.only("domain", () => {
    const testId = getTestId();
    const domainName = `domain_${testId}`;
    const childDomainName = `child_domain_${testId}`;
    const dataProductName = `dp_${testId}`;
    // SaaS-only
    // const structuredPropertyName = `property_${testId}`;
    // const structuredPropertyValue = `value_${testId}`;
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
        value: childDomainName,
      },
      {
        type: "data-products",
        name: "Data Products",
        value: dataProductName,
      },
    ];
    const modulesAvailableToAdd = [
      ...defaultModules,
      ...ADDITIONAL_MODULES_AVAILABLE_TO_ADD,
    ];

    // SaaS-only
    // utils.createStructuredProperty(structuredPropertyName);
    utils.createDomain(domainName);
    utils.openDomain(domainName);
    utils.createChildDomain(childDomainName, domainName);
    utils.addAsset("DOMAIN", TEST_ASSET_NAME, TEST_ASSET_URN);
    utils.createDataProduct(domainName, dataProductName);
    utils.goToSummaryTab();
    cy.reload();

    utils.ensureSummaryTabIsAvailable();

    testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS", value: USERNAME },
    ]);

    // SaaS-only
    // testPropertiesSectionStructuredProperties(
    //   structuredPropertyName,
    //   structuredPropertyValue,
    // );

    testAboutSection();

    testTemplateSection(defaultModules, modulesAvailableToAdd);

    // Clean up
    utils.openDataProduct(domainName, dataProductName);
    utils.deleteOpenedDataProduct();
    utils.deleteDomain(childDomainName);
    utils.deleteDomain(domainName);
    // SaaS-only
    // utils.deleteStructuredProperty(structuredPropertyName);
  });

  it("glossary node", () => {
    const testId = getTestId();
    const termName = `term_${testId}`;
    const nodeName = `node_${testId}`;
    // SaaS-only
    // const structuredPropertyName = `property_${testId}`;
    // const structuredPropertyValue = `value_${testId}`;
    const defaultModules = [
      {
        type: "hierarchy",
        // FYI: Contents module has different type in add module menu
        addType: "child-hierarchy",
        name: "Contents",
        value: termName,
      },
    ];
    const modulesAvailableToAdd = [
      ...defaultModules,
      ...ADDITIONAL_MODULES_AVAILABLE_TO_ADD,
    ];

    // SaaS-only
    // utils.createStructuredProperty(structuredPropertyName);
    utils.createGlossaryNode(nodeName);
    utils.openGlossaryNode(nodeName);
    utils.createGlossaryTerm(nodeName, termName);
    utils.goToSummaryTab();
    cy.reload();

    utils.ensureSummaryTabIsAvailable();

    testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS", value: USERNAME },
    ]);

    // SaaS-only
    // testPropertiesSectionStructuredProperties(
    //   structuredPropertyName,
    //   structuredPropertyValue,
    // );

    testAboutSection();

    testTemplateSection(defaultModules, modulesAvailableToAdd);

    // Clean up
    utils.deleteOpenedGlossaryNode();
    utils.openGlossaryTerm(termName);
    utils.deleteOpenedGLossaryTerm();
    // SaaS-only
    // utils.deleteStructuredProperty(structuredPropertyName);
  });

  it("glossary term", () => {
    const testId = getTestId();
    const domainName = TEST_DOMAIN;
    const nodeName = `term_node_${testId}`;
    const termName = `term_${testId}`;
    const relatedTermName = TEST_TERM;
    // SaaS-only
    // const structuredPropertyName = `property_${testId}`;
    // const structuredPropertyValue = `value_${testId}`;
    const defaultModules = [
      {
        type: "assets",
        name: "Assets",
        value: TEST_ASSET_NAME,
      },
      {
        type: "related-terms",
        name: "Related Terms",
        value: relatedTermName,
      },
    ];
    const modulesAvailableToAdd = [
      ...defaultModules,
      ...ADDITIONAL_MODULES_AVAILABLE_TO_ADD,
    ];

    // SaaS-only
    // utils.createStructuredProperty(structuredPropertyName);
    utils.createGlossaryNode(nodeName);
    utils.createGlossaryTerm(nodeName, termName);
    utils.openGlossaryTerm(termName);
    utils.setDomainToOpenedEntity(domainName);
    utils.addAsset("GLOSSARY_TERM", TEST_ASSET_NAME, TEST_ASSET_URN);
    utils.addRelatedTerm(relatedTermName);
    utils.goToSummaryTab();
    cy.reload();

    utils.ensureSummaryTabIsAvailable();

    testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS", value: USERNAME },
      { name: "Domain", type: "DOMAIN", value: domainName },
    ]);

    // SaaS-only
    // testPropertiesSectionStructuredProperties(
    //   structuredPropertyName,
    //   structuredPropertyValue,
    // );

    testAboutSection();

    testTemplateSection(defaultModules, modulesAvailableToAdd);

    // Clean up
    utils.deleteOpenedGLossaryTerm();

    utils.openGlossaryNode(nodeName);
    utils.deleteOpenedGlossaryNode();

    // SaaS-only
    // utils.deleteStructuredProperty(structuredPropertyName);
  });

  it("data product", () => {
    const testId = getTestId();
    const domainName = `Testing`;
    const dataProductName = `dp_${testId}`;
    const termName = TEST_TERM;
    const tagName = TEST_TAG;
    // SaaS-only
    // const structuredPropertyName = `property_${testId}`;
    // const structuredPropertyValue = `value_${testId}`;
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

    // SaaS-only
    // utils.createStructuredProperty(structuredPropertyName);
    utils.createDataProduct(domainName, dataProductName);
    utils.openDataProduct(domainName, dataProductName);
    utils.addAsset("DATA_PRODUCT", TEST_ASSET_NAME, TEST_ASSET_URN);
    utils.setGlossaryTermToOpenedEntity(termName);
    utils.setTagToOpenedEntity(tagName);
    cy.reload();

    utils.ensureSummaryTabIsAvailable();

    testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS", value: USERNAME },
      { name: "Domain", type: "DOMAIN", value: domainName },
      { name: "Tags", type: "TAGS", value: tagName },
      { name: "Glossary Terms", type: "GLOSSARY_TERMS", value: termName },
    ]);

    // SaaS-only
    // testPropertiesSectionStructuredProperties(
    //   structuredPropertyName,
    //   structuredPropertyValue,
    // );

    testAboutSection();

    testTemplateSection(defaultModules, modulesAvailableToAdd);

    // Clean up
    utils.deleteOpenedDataProduct();
    // SaaS-only
    // utils.deleteStructuredProperty(structuredPropertyName);
  });
});
