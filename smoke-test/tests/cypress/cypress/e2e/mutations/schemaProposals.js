import { hasOperationName } from "../utils";

const datasetUrn =
  "urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)";
const datasetName = "DatasetToProposeOn";

const clickSchemaLevel = (field_id, text_to_click) => {
  cy.get(`[data-testid="${field_id}"]`).within(() =>
    cy.contains(text_to_click).click(),
  );
};

const clickAddTagsSchemaLevel = (field_id) => {
  clickSchemaLevel(field_id, "Add Tags");
};

const clickAddTermsSchemaLevel = (field_id) => {
  clickSchemaLevel(field_id, "Add Terms");
};

const createProposalOnModal = (to_type, text) => {
  cy.get(".ant-select-selector").eq(2).type(to_type);
  cy.wait(1000);
  cy.get(".rc-virtual-list").find("div").contains(to_type).click();
  cy.get('[data-testid="propose-tags-terms-on-entity-button"]').click({
    force: true,
  });
  cy.waitTextVisible(to_type);
  cy.contains(to_type).should("be.visible");
  cy.wait(2000);
};

const acceptProposalDatasetPage = (to_type, thing) => {
  cy.waitTextVisible(to_type);
  cy.get(`[data-testid="proposed-${thing}-${to_type}"]`)
    .first()
    .should("be.visible")
    .click({ force: true });
  cy.get(`[data-testid="proposal-accept-button"]`).click({
    force: true,
  });
  cy.get(`[data-testid="proposed-${thing}-${to_type}"]`).should("not.exist");
};

const rejectProposalDatasetPage = (to_type, thing) => {
  cy.waitTextVisible(to_type);
  cy.get(`[data-testid="proposed-${thing}-${to_type}"]`)
    .first()
    .should("be.visible")
    .click({ force: true });
  cy.get(`[data-testid="proposal-reject-button"]`).click({
    force: true,
  });
  cy.get(`[data-testid="proposed-${thing}-${to_type}"]`).should("not.exist");
};

const removeTagSchemaLevel = (field_id, element, tagORTerm, text) => {
  cy.get(".ant-drawer-content").contains(tagORTerm).should("be.visible");
  cy.get(`[data-testid=${field_id}]`).within(() => {
    cy.contains(element, tagORTerm)
      .find(".ant-tag-close-icon")
      .click({ force: true });
  });
  cy.contains("Yes").click();
  cy.waitTextVisible(text);
  cy.ensureTextNotPresent(text);
  cy.get(".ant-drawer-content").contains(tagORTerm).should("not.exist");
};

const deletePreviousEntity = (entity, locator, entityName, message) => {
  cy.get(`[data-testid="schema-field-field_foo-${entity}"]`)
    .invoke("text")
    .then((text) => {
      const storedVariable = text.trim();
      if (storedVariable.includes(entityName)) {
        removeTagSchemaLevel(
          `schema-field-field_foo-${entity}`,
          locator,
          entityName,
          message,
        );
      } else {
        cy.ensureTextNotPresent(entityName);
      }
    });
};

const setShowTaskCenterRedesignFlag = (isOn) => {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.reply((res) => {
        res.body.data.appConfig.featureFlags.showTaskCenterRedesign = isOn;
      });
    }
  });
};

describe("schemaProposals", () => {
  Cypress.on("uncaught:exception", (err, runnable) => false);

  beforeEach(() => {
    setShowTaskCenterRedesignFlag(false);
  });

  it("can propose a schema-level tag and then decline tag proposal from the dataset page", () => {
    cy.login();
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithText("field_foo");
    deletePreviousEntity(
      "tags",
      '[data-testid="tag-TagToPropose"]',
      "TagToPropose",
      "Removed Tag!",
    );
    clickAddTagsSchemaLevel("schema-field-field_foo-tags");
    createProposalOnModal("TagToPropose", "Proposed Tag!");
    rejectProposalDatasetPage("TagToPropose", "tag");
  });

  it("can propose a schema-level term and then decline term proposal from the dataset page", () => {
    cy.login();

    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithText("field_foo");
    deletePreviousEntity("terms", ".ant-tag", "TermToPropose", "Removed Term!");

    clickAddTermsSchemaLevel("schema-field-field_foo-terms");
    createProposalOnModal("TermToPropose", "Proposed Term!");
    rejectProposalDatasetPage("TermToPropose", "term");
  });

  it("can propose a schema-level tag and then accept tag proposal from the dataset page", () => {
    cy.login();

    // Proposing a tag
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithText("field_foo");
    deletePreviousEntity(
      "tags",
      '[data-testid="tag-TagToPropose"]',
      "TagToPropose",
      "Removed Tag!",
    );
    clickAddTagsSchemaLevel("schema-field-field_foo-tags");
    createProposalOnModal("TagToPropose", "Proposed Tag!");
    acceptProposalDatasetPage("TagToPropose", "tag");
    cy.clickOptionWithText("field_foo");
    removeTagSchemaLevel(
      "schema-field-field_foo-tags",
      '[data-testid="tag-TagToPropose"]',
      "TagToPropose",
      "Removed Tag!",
    );
  });

  it("can propose a schema-level term and then accept term proposal from the dataset page", () => {
    cy.login();

    // Proposing a term
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithText("field_foo");
    deletePreviousEntity("terms", ".ant-tag", "TermToPropose", "Removed Term!");
    clickAddTermsSchemaLevel("schema-field-field_foo-terms");
    createProposalOnModal("TermToPropose", "Proposed Term!");
    acceptProposalDatasetPage("TermToPropose", "term");

    // Data cleanup
    cy.clickOptionWithText("field_foo");
    cy.get("[data-testid=schema-field-field_foo-terms]").within(() => {
      cy.contains("TermToPropose").within(() =>
        cy.get("span[aria-label=close]").click(),
      );
    });
    cy.contains("Yes").click({ force: true });
    cy.contains("TermToPropose").should("not.exist");
  });

  it("can propose a schema-level tag and then decline the proposal from the Inbox tab", () => {
    cy.login();

    // Proposing a tag
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithText("field_foo");
    deletePreviousEntity(
      "tags",
      '[data-testid="proposed-tag-TagToPropose"]',
      "TagToPropose",
      "Removed Tag!",
    );
    clickAddTagsSchemaLevel("schema-field-field_foo-tags");
    createProposalOnModal("TagToPropose", "Proposed Tag!");
    cy.searchNotCachedContainsDataset("TagToPropose", datasetName);

    cy.rejectProposalInbox();
    cy.searchNotCachedDoesNotContainDataset("TagToPropose", datasetName);
  });

  it("can propose a schema-level glossary term and then decline the proposal from the Inbox tab", () => {
    cy.login();

    // Proposing a term
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithText("field_foo");
    deletePreviousEntity("terms", ".ant-tag", "TermToPropose", "Removed Term!");
    clickAddTermsSchemaLevel("schema-field-field_foo-terms");
    createProposalOnModal("TermToPropose", "Proposed Term!");
    cy.searchNotCachedContainsDataset("TermToPropose", datasetName);
    cy.rejectProposalInbox();
    cy.searchNotCachedDoesNotContainDataset("TermToPropose", datasetName);
  });

  it("can propose a schema-level tag to a dataset and then accept the proposal from the Inbox tab", () => {
    cy.login();
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithText("field_foo");
    deletePreviousEntity(
      "tags",
      '[data-testid="proposed-tag-TagToPropose"]',
      "TagToPropose",
      "Removed Tag!",
    );
    clickAddTagsSchemaLevel("schema-field-field_foo-tags");
    createProposalOnModal("TagToPropose", "Proposed Tag!");
    cy.searchNotCachedContainsDataset("TagToPropose", datasetName);
    cy.acceptProposalInbox();
    cy.searchNotCachedContainsDataset("TagToPropose", datasetName);
    // Verifying the applied tag is present
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithText("field_foo");
    cy.get('[data-testid="schema-field-field_foo-tags"]')
      .should("be.visible")
      .contains("TagToPropose");
    cy.get('[data-testid="proposed-tag-TagToPropose"]').should("not.exist");
    removeTagSchemaLevel(
      "schema-field-field_foo-tags",
      '[data-testid="tag-TagToPropose"]',
      "TagToPropose",
      "Removed Tag!",
    );
  });

  it("can propose a schema-level term to dataset and then accept the proposal from the Inbox tab", () => {
    cy.login();

    // Proposing a term
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithText("field_foo");
    deletePreviousEntity("terms", ".ant-tag", "TermToPropose", "Removed Term!");
    clickAddTermsSchemaLevel("schema-field-field_foo-terms");
    createProposalOnModal("TermToPropose", "Proposed Term!");
    cy.searchNotCachedContainsDataset("TermToPropose", datasetName);
    cy.acceptProposalInbox();
    cy.searchNotCachedContainsDataset("TermToPropose", datasetName);

    // Verifying the applied glossary term is present
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithText("field_foo");
    cy.get('[data-testid="schema-field-field_foo-terms"]')
      .should("be.visible")
      .contains("TermToPropose");

    // Data cleanup
    cy.get("[data-testid=schema-field-field_foo-terms]").within(() => {
      cy.contains("TermToPropose").within(() =>
        cy.get("span[aria-label=close]").click(),
      );
    });
    cy.contains("Yes").click({ force: true });
    cy.contains("TermToPropose").should("not.exist");
  });
});
