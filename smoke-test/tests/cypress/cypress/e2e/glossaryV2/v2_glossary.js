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
    cy.loginWithCredentials();
    cy.skipIntroducePage();
    nevigateGlossaryPage();
    cy.clickOptionWithTestId("add-term-group-button-v2");
    cy.clickOptionWithText("Create Term Group");
    // Wait for modal container to appear first
    cy.get(".ant-modal-content", { timeout: 10000 }).should("be.visible");
    cy.get('[data-testid="glossary-entity-modal-create-button"]', {
      timeout: 10000,
    }).should("be.visible");
    cy.get('[data-testid="create-glossary-entity-modal-name"]', {
      timeout: 10000,
    }).should("be.visible");
    cy.enterTextInTestId(
      "create-glossary-entity-modal-name",
      glossaryTermGroup,
    );
    cy.clickOptionWithTestId("glossary-entity-modal-create-button");
    cy.contains(glossaryTermGroup).should("be.visible");
    cy.wait(2000);
    nevigateGlossaryPage();
    cy.clickOptionWithText(glossaryTermGroup);
    cy.clickOptionWithTestId("add-term-button");
    // Wait for modal container to appear first
    cy.get(".ant-modal-content", { timeout: 10000 }).should("be.visible");
    cy.get('[data-testid="glossary-entity-modal-create-button"]', {
      timeout: 10000,
    }).should("be.visible");
    cy.get('[data-testid="create-glossary-entity-modal-name"]', {
      timeout: 10000,
    }).should("be.visible");
    cy.enterTextInTestId("create-glossary-entity-modal-name", glossaryTerm);
    cy.clickOptionWithTestId("glossary-entity-modal-create-button");
    cy.contains(glossaryTerm).should("be.visible");
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
