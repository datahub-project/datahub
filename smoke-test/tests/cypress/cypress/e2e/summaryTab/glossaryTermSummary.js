import * as utils from "./utils";

const TEST_USER_DISPLAY_NAME = "John Doe";
const TEST_GLOSSARY_TERM_URN = "urn:li:glossaryTerm:CypressNode.CypressTerm";
const TEST_DOMAIN_NAME = "Testing";
const TEST_RELATED_TERM_NAME = "RelatedCypressTerm";

describe("summary tab - glossary term", () => {
  beforeEach(() => {
    utils.setThemeV2AndSummaryTabFlags(true);
    cy.login();
    cy.goToGlossaryTerm(TEST_GLOSSARY_TERM_URN);
    utils.goToSummaryTab();
  });

  it("glossary term - header section", () => {
    utils.testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS", value: TEST_USER_DISPLAY_NAME },
      { name: "Domain", type: "DOMAIN", value: TEST_DOMAIN_NAME },
    ]);
  });

  it("glossary term - description section", () => {
    utils.testAboutSection();
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

    utils.testTemplateSection(defaultModules);
  });
});
