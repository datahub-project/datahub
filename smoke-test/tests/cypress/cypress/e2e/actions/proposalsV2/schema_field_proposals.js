import {
  setThemeV2AndTaskCenterRedesignFlags,
  proposeItems,
  proposeStructuredProperty,
  acceptProposalFromAssetPage,
  rejectProposalFromAssetPage,
  createSidebarStructuredProp,
} from "./utils";

const dataset = {
  urn: "/dataset/urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.stg_customers,PROD)/",
  name: "stg_customers",
};

const schemaFieldName = "customer_id";

const structuredProperty = {
  name: "Proposal Field Structured Prop",
  entity: "Column",
};

const propValue = "Field Property String Value";

const TAG_TO_PROPOSE = "TagToPropose";
const SECOND_TAG_TO_PROPOSE = "Cypress";
const TERM_TO_PROPOSE = "TermToPropose";
const SECOND_TERM_TO_PROPOSE = "CypressTerm";

describe("proposals on dataset page", () => {
  beforeEach(() => {
    setThemeV2AndTaskCenterRedesignFlags(true);
    cy.loginWithCredentials();
  });

  Cypress.on("uncaught:exception", (err, runnable) => false);

  it("can propose tag and accept it on schema field", () => {
    proposeItems(
      dataset,
      [TAG_TO_PROPOSE],
      "tag",
      "schema-field-add-tags-button",
      "tag-term-modal-input",
      undefined,
      schemaFieldName,
    );
    acceptProposalFromAssetPage(TAG_TO_PROPOSE, "tag", schemaFieldName);
  });

  it("can propose multiple tags and reject them on schema field", () => {
    proposeItems(
      dataset,
      [TAG_TO_PROPOSE, SECOND_TAG_TO_PROPOSE],
      "tag",
      "schema-field-add-tags-button",
      "tag-term-modal-input",
      undefined,
      schemaFieldName,
    );
    rejectProposalFromAssetPage(TAG_TO_PROPOSE, "tag", schemaFieldName);
  });

  it("can propose term and accept it on schema field", () => {
    proposeItems(
      dataset,
      [TERM_TO_PROPOSE],
      "term",
      "schema-field-add-terms-button",
      "tag-term-modal-input",
      undefined,
      schemaFieldName,
    );
    acceptProposalFromAssetPage(TERM_TO_PROPOSE, "term", schemaFieldName);
  });

  it("can propose multiple terms and reject them on schema field", () => {
    proposeItems(
      dataset,
      [TERM_TO_PROPOSE, SECOND_TERM_TO_PROPOSE],
      "term",
      "schema-field-add-terms-button",
      "tag-term-modal-input",
      undefined,
      schemaFieldName,
    );
    rejectProposalFromAssetPage(TERM_TO_PROPOSE, "term", schemaFieldName);
  });

  it("can propose structured property and accept it on schema field", () => {
    createSidebarStructuredProp(structuredProperty, schemaFieldName);
    proposeStructuredProperty(
      dataset,
      structuredProperty,
      propValue,
      schemaFieldName,
    );
    acceptProposalFromAssetPage(
      propValue,
      `property-${structuredProperty.name}-value`,
      schemaFieldName,
    );
    cy.goToStructuredProperties();
    cy.deleteStructuredProperty(structuredProperty);
  });

  it("can propose structured property and reject on schema field", () => {
    createSidebarStructuredProp(structuredProperty, schemaFieldName);
    proposeStructuredProperty(
      dataset,
      structuredProperty,
      propValue,
      schemaFieldName,
    );
    rejectProposalFromAssetPage(
      propValue,
      `property-${structuredProperty.name}-value`,
      schemaFieldName,
    );
    cy.goToStructuredProperties();
    cy.deleteStructuredProperty(structuredProperty);
  });
});
