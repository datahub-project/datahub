const urn =
  "dataset/urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.customers_source,PROD)/";
const datasetName = "customers_source";
const glossaryTerm = "CypressGlossaryTerm";
const glossaryTermGroup = "CypressGlossaryGroup";
let myUrl;

const nevigateGlossaryPage = () => {
  cy.visit("/glossary");
  cy.get('[data-testid="glossaryPageV2"]').should("be.visible");
};

const addGlossaryTermToDataset = () => {
  cy.visit(urn);
  cy.waitTextVisible(datasetName);
  cy.get("body").click();
  cy.get('[id="entity-profile-glossary-terms"]').within(() => {
    cy.clickOptionWithTestId("add-terms-button");
  });
  cy.selectOptionInTagTermModal(glossaryTerm);
  cy.contains(glossaryTerm);
};

const deleteGlossary = (message) => {
  // The inline-edit pencil is rendered by antd Typography.Text's editable wrapper;
  // its outer class (`.ant-typography-edit`) is stable across icon swaps, while the
  // inner icon was migrated from antd's `.anticon-edit` to a phosphor `<PencilSimple>`.
  cy.get(".ant-typography-edit").should("be.visible");
  cy.get('[data-testid="MoreVertOutlinedIcon"]').should("be.visible").click();
  cy.clickOptionWithText("Delete");
  cy.clickOptionWithText("Yes");
  cy.reload();
  cy.ensureTextNotPresent(message);
};

describe("glossary", () => {
  beforeEach(() => {
    Cypress.on("uncaught:exception", (err, runnable) => false);
  });
  it("go to glossary page, create terms, term group", () => {
    cy.login();
    cy.skipIntroducePage();
    nevigateGlossaryPage();
    cy.clickOptionWithTestId("add-term-group-button-v2");
    // Modal was migrated to alchemy `<Input>`, which no longer renders antd's
    // `.ant-input-affix-wrapper`. Drive it via stable data-testids, matching the
    // pattern used by v2_glossary_navigation.js and v2_glossaryTerm.js.
    cy.waitTextVisible("Create Glossary");
    cy.enterTextInTestId(
      "create-glossary-entity-modal-name",
      glossaryTermGroup,
    );
    cy.clickOptionWithTestId("glossary-entity-modal-create-button");
    cy.contains(glossaryTermGroup)
      .filter(":visible")
      .first()
      .should("be.visible");
    cy.wait(2000);
    nevigateGlossaryPage();
    cy.clickOptionWithText(glossaryTermGroup);
    cy.clickOptionWithTestId("Contents-entity-tab-header");
    cy.clickOptionWithTestId("add-term-button");
    cy.waitTextVisible("Create Glossary Term");
    cy.enterTextInTestId("create-glossary-entity-modal-name", glossaryTerm);
    cy.clickOptionWithTestId("glossary-entity-modal-create-button");
    cy.contains(glossaryTerm).filter(":visible").first().should("be.visible");
    addGlossaryTermToDataset();
    cy.waitTextVisible(glossaryTerm);
    nevigateGlossaryPage();
    cy.clickOptionWithText(glossaryTermGroup);
    cy.clickOptionWithText(glossaryTerm);
    deleteGlossary(glossaryTerm);
    nevigateGlossaryPage();
    cy.clickOptionWithText(glossaryTermGroup);
    cy.ensureTextNotPresent(glossaryTerm);
    nevigateGlossaryPage();
    cy.clickOptionWithText(glossaryTermGroup);
    deleteGlossary(glossaryTermGroup);
    nevigateGlossaryPage();
    cy.waitTextVisible("CypressNode");
    cy.ensureTextNotPresent(glossaryTermGroup);
  });
});
