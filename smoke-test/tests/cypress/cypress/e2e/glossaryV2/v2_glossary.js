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
  cy.contains(".ant-collapse-header-text", "Terms")
    .parent()
    .find('[data-testid="AddRoundedIcon"]')
    .click();
  cy.selectOptionInTagTermModal(glossaryTerm);
  cy.contains(glossaryTerm);
};

const deleteGlossary = (message) => {
  cy.get(".anticon-edit").should("be.visible");
  cy.get('[data-testid="MoreVertOutlinedIcon"]').should("be.visible").click();
  cy.clickOptionWithText("Delete");
  cy.clickOptionWithText("Yes");
  cy.reload();
  cy.ensureTextNotPresent(message);
};

describe("glossary", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    Cypress.on("uncaught:exception", (err, runnable) => false);
  });
  it("go to glossary page, create terms, term group", () => {
    cy.login();
    cy.skipIntroducePage();
    nevigateGlossaryPage();
    cy.clickOptionWithTestId("add-term-group-button-v2");
    cy.addViaModal(
      glossaryTermGroup,
      "Create Glossary",
      glossaryTermGroup,
      "glossary-entity-modal-create-button",
    );
    cy.wait(2000);
    nevigateGlossaryPage();
    cy.clickOptionWithText(glossaryTermGroup);
    cy.clickOptionWithTestId("Contents-entity-tab-header");
    cy.clickOptionWithTestId("add-term-button");
    cy.addViaModal(
      glossaryTerm,
      "Create Glossary Term",
      glossaryTerm,
      "glossary-entity-modal-create-button",
    );
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
