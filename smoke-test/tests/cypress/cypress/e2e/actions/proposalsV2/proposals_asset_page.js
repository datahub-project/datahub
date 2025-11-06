import {
  setThemeV2AndTaskCenterRedesignFlags,
  proposeItems,
  proposeStructuredProperty,
  acceptProposalFromAssetPage,
  rejectProposalFromAssetPage,
  createSidebarStructuredProp,
} from "./utils";

const assetMap = {
  datasetToProposeOn: {
    urn: "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)",
    name: "DatasetToProposeOn",
  },
  dailyTemperatureDataset: {
    urn: "/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD)/",
    name: "daily_temperature",
  },
  dashboard: {
    urn: "/dashboard/urn:li:dashboard:(looker,cypress_baz)",
    name: "Baz Dashboard",
  },
  dataFlow: {
    urn: "/tasks/urn:li:dataJob:(urn:li:dataFlow:(airflow,cypress_dag_abc,PROD),cypress_task_123)",
    name: "User Creations",
  },
  chart: {
    urn: "/chart/urn:li:chart:(looker,cypress_baz1)",
    name: "Baz Chart 1",
  },
  mlFeature: {
    urn: "/features/urn:li:mlFeature:(cypress-test-2,some-cypress-feature-1)",
    name: "some-cypress-feature-1",
  },
};

const structuredProperty = {
  name: "Proposal Structured Prop",
  entity: "Dataset",
};

const propValue = "Property String Value";

const OWNER_TO_PROPOSE = Cypress.env("ADMIN_DISPLAYNAME");
const SECOND_OWNER_TO_PROPOSE = "user1";
const TAG_TO_PROPOSE = "TagToPropose";
const SECOND_TAG_TO_PROPOSE = "Cypress";
const TERM_TO_PROPOSE = "TermToPropose";
const SECOND_TERM_TO_PROPOSE = "CypressTerm";
const DOMAIN_TO_PROPOSE = "Marketing";

describe("proposals on dataset page", () => {
  beforeEach(() => {
    setThemeV2AndTaskCenterRedesignFlags(true);
    cy.loginWithCredentials();
  });

  Cypress.on("uncaught:exception", (err, runnable) => false);

  it("can propose owner and accept from the dataset page", () => {
    proposeItems(
      assetMap.dailyTemperatureDataset,
      [OWNER_TO_PROPOSE],
      "owner",
      "add-owners-button",
      "edit-owners-modal-find-actors-input",
    );
    acceptProposalFromAssetPage(OWNER_TO_PROPOSE, "owner");
  });

  it("can propose owner and reject from the dataset page", () => {
    proposeItems(
      assetMap.dailyTemperatureDataset,
      [OWNER_TO_PROPOSE],
      "owner",
      "add-owners-button",
      "edit-owners-modal-find-actors-input",
    );
    rejectProposalFromAssetPage(OWNER_TO_PROPOSE, "owner");
  });

  it("can propose domain and accept from the dataset page", () => {
    proposeItems(
      assetMap.datasetToProposeOn,
      [DOMAIN_TO_PROPOSE],
      "domain",
      "set-domain-button",
      "set-domain-select",
    );
    acceptProposalFromAssetPage(DOMAIN_TO_PROPOSE, "domain");
  });

  it("can propose domain and reject from the dataset page", () => {
    proposeItems(
      assetMap.datasetToProposeOn,
      [DOMAIN_TO_PROPOSE],
      "domain",
      "set-domain-button",
      "set-domain-select",
    );
    rejectProposalFromAssetPage(DOMAIN_TO_PROPOSE, "domain");
  });

  it("can propose tag and accept from the dataset page", () => {
    proposeItems(
      assetMap.datasetToProposeOn,
      [TAG_TO_PROPOSE],
      "tag",
      "add-tags-button",
      "tag-term-modal-input",
    );
    acceptProposalFromAssetPage(TAG_TO_PROPOSE, "tag");
  });

  it("can propose tag and reject from the dataset page", () => {
    proposeItems(
      assetMap.datasetToProposeOn,
      [TAG_TO_PROPOSE],
      "tag",
      "add-tags-button",
      "tag-term-modal-input",
    );
    rejectProposalFromAssetPage(TAG_TO_PROPOSE, "tag");
  });

  it("can propose term and accept from the dataset page", () => {
    proposeItems(
      assetMap.datasetToProposeOn,
      [TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    acceptProposalFromAssetPage(TERM_TO_PROPOSE, "term");
  });

  it("can propose term and reject from the dataset page", () => {
    proposeItems(
      assetMap.datasetToProposeOn,
      [TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    rejectProposalFromAssetPage(TERM_TO_PROPOSE, "term");
  });

  it("can propose structured property and accept from the dataset page", () => {
    createSidebarStructuredProp(structuredProperty);
    proposeStructuredProperty(
      assetMap.datasetToProposeOn,
      structuredProperty,
      propValue,
    );
    acceptProposalFromAssetPage(
      propValue,
      `property-${structuredProperty.name}-value`,
    );
    cy.goToStructuredProperties();
    cy.deleteStructuredProperty(structuredProperty);
  });

  it("can propose structured property and reject from the dataset page", () => {
    createSidebarStructuredProp(structuredProperty);
    proposeStructuredProperty(
      assetMap.datasetToProposeOn,
      structuredProperty,
      propValue,
    );
    rejectProposalFromAssetPage(
      propValue,
      `property-${structuredProperty.name}-value`,
    );
    cy.goToStructuredProperties();
    cy.deleteStructuredProperty(structuredProperty);
  });

  it("can view proposals from sidebar header", () => {
    proposeItems(
      assetMap.datasetToProposeOn,
      [TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    cy.get(`[data-testid="view-tasks-button"]`).should("be.visible").click();
    cy.get('[data-testid="proposals-table"]').should("exist");
    cy.get('[data-testid="proposals-table"] tbody tr')
      .its("length")
      .should("be.greaterThan", 0);
    cy.get('[data-testid="modal-close-icon"]').click();
    rejectProposalFromAssetPage(TERM_TO_PROPOSE, "term");
  });

  it("can propose a tag with note and view the note in proposals table", () => {
    proposeItems(
      assetMap.datasetToProposeOn,
      [TAG_TO_PROPOSE],
      "tag",
      "add-tags-button",
      "tag-term-modal-input",
      "tag proposal note",
    );
    cy.get(`[data-testid="view-tasks-button"]`).should("be.visible").click();
    cy.get('[data-testid="proposals-table"]')
      .find("tbody tr")
      .first()
      .should("contain.text", "tag proposal note");

    cy.get('[data-testid="modal-close-icon"]').click();
    rejectProposalFromAssetPage(TAG_TO_PROPOSE, "tag");
  });

  it("can propose term and accept on chart entity type", () => {
    proposeItems(
      assetMap.chart,
      [TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    acceptProposalFromAssetPage(TERM_TO_PROPOSE, "term");
  });

  it("can propose term and accept on dashboard entity type", () => {
    proposeItems(
      assetMap.dashboard,
      [TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    acceptProposalFromAssetPage(TERM_TO_PROPOSE, "term");
  });

  it("can propose term and accept on data flow entity type", () => {
    proposeItems(
      assetMap.dataFlow,
      [TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    acceptProposalFromAssetPage(TERM_TO_PROPOSE, "term");
  });

  it("can propose term and accept on ML Feature entity type", () => {
    proposeItems(
      assetMap.mlFeature,
      [TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    acceptProposalFromAssetPage(TERM_TO_PROPOSE, "term");
  });

  it("can propose multiple tags and reject them from the dataset page", () => {
    proposeItems(
      assetMap.datasetToProposeOn,
      [TAG_TO_PROPOSE, SECOND_TAG_TO_PROPOSE],
      "tag",
      "add-tags-button",
      "tag-term-modal-input",
    );
    rejectProposalFromAssetPage(TAG_TO_PROPOSE, "tag");
  });

  it("can propose multiple terms and reject them from the dataset page", () => {
    proposeItems(
      assetMap.datasetToProposeOn,
      [TERM_TO_PROPOSE, SECOND_TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    rejectProposalFromAssetPage(TERM_TO_PROPOSE, "term");
  });

  it("can propose multiple owners and reject them from the dataset page", () => {
    proposeItems(
      assetMap.dailyTemperatureDataset,
      [OWNER_TO_PROPOSE, SECOND_OWNER_TO_PROPOSE],
      "owner",
      "add-owners-button",
      "edit-owners-modal-find-actors-input",
    );
    rejectProposalFromAssetPage(OWNER_TO_PROPOSE, "owner");
  });
});
