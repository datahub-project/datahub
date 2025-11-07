import * as utils from "./utils";

const TEST_USER_DISPLAY_NAME = "jdoe";
const TEST_GLOSSARY_NODE_URN = "urn:li:glossaryNode:CypressNode";
const TEST_GLOSSARY_TERM_NAME = "CypressTerm";

describe("summary tab - glossary node", () => {
  beforeEach(() => {
    utils.setThemeV2AndSummaryTabFlags(true);
    cy.login();
    cy.goToGlossaryNode(TEST_GLOSSARY_NODE_URN);
    utils.goToSummaryTab();
  });

  it("glossary node - header section", () => {
    utils.testPropertiesSection([
      { name: "Created", type: "CREATED" },
      { name: "Owners", type: "OWNERS", value: TEST_USER_DISPLAY_NAME },
    ]);
  });

  it("glossary node - description section", () => {
    utils.testAboutSection();
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

    utils.testTemplateSection(defaultModules);
  });
});
