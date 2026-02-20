import { hasOperationName } from "../utils";

const datasetUrn =
  "urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)";
const datasetName = "DatasetToProposeOn";

const ELEMENT_TYPE_TAG = "tag";
const ELEMENT_TYPE_TERM = "term";

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

const deleteOrReject = (elementType, field, tagOrTermName) => {
  const elementTypePlural = elementType === ELEMENT_TYPE_TAG ? "tags" : "terms";
  const elementTypeCapitalized =
    elementType.charAt(0).toUpperCase() + elementType.slice(1);
  const containerSelector = `[data-testid="schema-field-${field}-${elementTypePlural}"]`;
  const proposedSelector = `[data-testid="proposed-${elementType}-${tagOrTermName}"]`;
  const elementSelector = `[data-testid="${elementType}-${tagOrTermName}"]`;

  cy.get("body").then(($body) => {
    const $container = Cypress.$(containerSelector, $body);
    const $proposed = Cypress.$(proposedSelector, $container);
    if ($proposed.length > 0) {
      cy.wrap($proposed).click({ force: true });
      cy.getWithTestId("proposal-reject-button").should("be.visible").click();
      cy.contains("Proposal declined.").should("be.visible");
      cy.contains("Proposal declined.").should("not.exist");
    }
  });

  cy.get("body").then(($body) => {
    const $container = Cypress.$(containerSelector, $body);
    const $element = Cypress.$(elementSelector, $container);
    if ($element.length > 0) {
      const $closeIcon = Cypress.$(".ant-tag-close-icon", $element);
      if ($closeIcon.length > 0) {
        cy.wrap($closeIcon).click({ force: true });
        cy.contains("Yes").click();
        cy.contains(`Removed ${elementTypeCapitalized}!`).should("be.visible");
        cy.contains(`Removed ${elementTypeCapitalized}!`).should("not.exist");
      }
    }
  });

  cy.get(containerSelector)
    .should("be.visible")
    .and("not.contain.text", tagOrTermName);
};

const setShowTaskCenterRedesignFlag = (isOn) => {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.reply((res) => {
        res.body.data.appConfig.featureFlags.showTaskCenterRedesign = isOn;
        res.body.data.appConfig.featureFlags.themeV2Enabled = false;
        res.body.data.appConfig.featureFlags.themeV2Default = false;
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
    deleteOrReject(ELEMENT_TYPE_TAG, "field_foo", "TagToPropose");
    clickAddTagsSchemaLevel("schema-field-field_foo-tags");
    createProposalOnModal("TagToPropose", "Proposed Tag!");
    rejectProposalDatasetPage("TagToPropose", "tag");
  });

  it("can propose a schema-level term and then decline term proposal from the dataset page", () => {
    cy.login();

    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithText("field_foo");
    deleteOrReject(ELEMENT_TYPE_TERM, "field_foo", "TermToPropose");

    clickAddTermsSchemaLevel("schema-field-field_foo-terms");
    createProposalOnModal("TermToPropose", "Proposed Term!");
    rejectProposalDatasetPage("TermToPropose", "term");
  });

  it("can propose a schema-level tag and then accept tag proposal from the dataset page", () => {
    cy.login();

    // Proposing a tag
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithText("field_foo");
    deleteOrReject(ELEMENT_TYPE_TAG, "field_foo", "TagToPropose");

    clickAddTagsSchemaLevel("schema-field-field_foo-tags");
    createProposalOnModal("TagToPropose", "Proposed Tag!");
    acceptProposalDatasetPage("TagToPropose", "tag");
    cy.clickOptionWithText("field_foo");
    deleteOrReject(ELEMENT_TYPE_TAG, "field_foo", "TagToPropose");
  });

  it("can propose a schema-level term and then accept term proposal from the dataset page", () => {
    cy.login();

    // Proposing a term
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithText("field_foo");
    deleteOrReject(ELEMENT_TYPE_TERM, "field_foo", "TermToPropose");
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
    deleteOrReject(ELEMENT_TYPE_TAG, "field_foo", "TagToPropose");

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
    deleteOrReject(ELEMENT_TYPE_TERM, "field_foo", "TermToPropose");
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
    deleteOrReject(ELEMENT_TYPE_TAG, "field_foo", "TagToPropose");
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
    deleteOrReject(ELEMENT_TYPE_TAG, "field_foo", "TagToPropose");
  });

  it("can propose a schema-level term to dataset and then accept the proposal from the Inbox tab", () => {
    cy.login();

    // Proposing a term
    cy.goToDataset(datasetUrn, datasetName);
    cy.clickOptionWithText("field_foo");
    deleteOrReject(ELEMENT_TYPE_TERM, "field_foo", "TermToPropose");
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
