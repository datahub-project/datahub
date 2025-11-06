import { proposalsList } from "./helpers/ProposalsList";
import {
  proposeItems,
  rejectProposalFromProposalsPage,
  setThemeV2AndTaskCenterRedesignFlags,
} from "./utils";

const assetMap = {
  entityOne: {
    urn: "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,cypress_health_test,PROD)/",
    name: "cypress_health_test",
    simpleUrn:
      "urn:li:dataset:(urn:li:dataPlatform:hive,cypress_health_test,PROD)",
  },
  entityTwo: {
    urn: "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)/",
    name: "SampleCypressHiveDataset",
    simpleUrn:
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)",
  },
};

const proposalTypes = {
  domain: "Domain Association",
  term: "Term Association",
};

const DOMAIN_TO_PROPOSE = "Marketing";
const TERM_TO_PROPOSE = "TermToPropose";

describe("Proposals Filters interactions", () => {
  beforeEach(() => {
    setThemeV2AndTaskCenterRedesignFlags(true);
    cy.login();
  });

  it("should filter the list based on the filters applied", () => {
    // create proposals. One completed, one pending. One owner, one domain
    proposeItems(
      assetMap.entityOne,
      [TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    proposeItems(
      assetMap.entityTwo,
      [DOMAIN_TO_PROPOSE],
      "domain",
      "set-domain-button",
      "set-domain-select",
    );
    rejectProposalFromProposalsPage(TERM_TO_PROPOSE);

    // Default filter
    cy.getWithTestId("proposal-completed-by").should("not.exist");
    cy.reload();

    // Different status
    proposalsList.clickFilterOption("status", "Completed");
    cy.getWithTestId("proposal-completed-by").should("exist");
    proposalsList.clickFilterOption("status", "Pending");
    cy.getWithTestId("approve-button").should("not.exist");

    rejectProposalFromProposalsPage(DOMAIN_TO_PROPOSE);

    // Different types
    proposalsList.clickFilterOption("status", "Pending");
    proposalsList.clickFilterOption("type", proposalTypes.domain);
    proposalsList.findRowContaining(TERM_TO_PROPOSE).should("not.exist");
    proposalsList.clickFilterOption("type", proposalTypes.domain);

    proposalsList.clickFilterOption("type", proposalTypes.term);
    proposalsList.findRowContaining(DOMAIN_TO_PROPOSE).should("not.exist");
  });

  it("should filter the rows based on an entity selected", () => {
    proposeItems(
      assetMap.entityOne,
      [TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    rejectProposalFromProposalsPage(TERM_TO_PROPOSE);
    proposeItems(
      assetMap.entityTwo,
      [DOMAIN_TO_PROPOSE],
      "domain",
      "set-domain-button",
      "set-domain-select",
    );
    rejectProposalFromProposalsPage(DOMAIN_TO_PROPOSE);

    // select an option from the dropdown
    proposalsList.selectEntityOptionContaining(assetMap.entityOne);
    // check if the selected option is shown in the component
    proposalsList.getEntitySelectComponent().click();
    cy.getWithTestId("entity-select-options")
      .contains(assetMap.entityOne.name)
      .should("exist");

    proposalsList.clickFilterOption("status", "Completed");
    // check if the filter is applied
    proposalsList
      .findRowContaining(assetMap.entityTwo.name)
      .should("not.exist");

    // clear the filters
    cy.get('[data-testid="clear-Entity-filter"]').should("be.visible").click();
    // check if the filter is removed
    proposalsList.findRowContaining(assetMap.entityTwo.name).should("exist");
  });

  it("should filter the rows based on multiple entities selected", () => {
    proposeItems(
      assetMap.entityOne,
      [TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    rejectProposalFromProposalsPage(TERM_TO_PROPOSE);
    proposeItems(
      assetMap.entityTwo,
      [DOMAIN_TO_PROPOSE],
      "domain",
      "set-domain-button",
      "set-domain-select",
    );
    rejectProposalFromProposalsPage(DOMAIN_TO_PROPOSE);

    // select multiple entities
    proposalsList.selectEntityOptionContaining(assetMap.entityOne);
    proposalsList.selectEntityOptionContaining(assetMap.entityTwo);

    proposalsList.clickFilterOption("status", "Completed");

    // check if the filter is applied
    proposalsList
      .findRowContaining(assetMap.entityTwo.name)
      .should("be.visible");
    proposalsList
      .findRowContaining(assetMap.entityOne.name)
      .should("be.visible");

    // Unselect one option
    proposalsList.selectEntityOptionContaining(assetMap.entityOne);
    proposalsList
      .findRowContaining(assetMap.entityOne.name)
      .should("not.exist");

    // Unselect another option
    proposalsList.selectEntityOptionContaining(assetMap.entityTwo);
    proposalsList.findRowContaining(assetMap.entityOne.name).should("exist");
  });

  it("should clear the filters upon tab switch", () => {
    proposeItems(
      assetMap.entityTwo,
      [DOMAIN_TO_PROPOSE],
      "domain",
      "set-domain-button",
      "set-domain-select",
    );
    rejectProposalFromProposalsPage(DOMAIN_TO_PROPOSE);

    proposalsList.clickFilterOption("status", "Completed");
    proposalsList.selectEntityOptionContaining(assetMap.entityTwo);

    proposalsList.visitMyProposals();

    // Check if entity selected is cleared
    proposalsList
      .getEntitySelectComponent()
      .contains(assetMap.entityTwo.name)
      .should("not.exist");
    // Check the status filter doesn't show any number
    proposalsList
      .getFilter("status")
      .find('[data-testid="pill-container"]')
      .should("not.exist");
  });
});
