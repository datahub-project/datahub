import { proposalsList } from "./helpers/ProposalsList";
import {
  setThemeV2AndTaskCenterRedesignFlags,
  proposeItems,
  acceptProposalFromProposalsPage,
  rejectProposalFromProposalsPage,
} from "./utils";

const assetMap = {
  entityOne: {
    urn: "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_created_no_tag,PROD)/",
    name: "fct_cypress_users_created_no_tag",
  },
};

const TERM_TO_PROPOSE = "TermToPropose";
const TERM_TO_PROPOSE_2 = "CypressTermToProposeOn";

const PROPOSAL_REJECTION_NOTE = "Rejecting Proposal";

describe("proposals page in task center", () => {
  beforeEach(() => {
    setThemeV2AndTaskCenterRedesignFlags(true);
    cy.login();
  });

  Cypress.on("uncaught:exception", (err, runnable) => false);

  it("can view proposed term in the table on proposals page", () => {
    proposeItems(
      assetMap.entityOne,
      [TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    proposalsList.visit();
    proposalsList.findRowContaining(TERM_TO_PROPOSE).should("be.visible");
    rejectProposalFromProposalsPage(TERM_TO_PROPOSE);
  });

  it("can propose term and accept from proposals page", () => {
    proposeItems(
      assetMap.entityOne,
      [TERM_TO_PROPOSE_2],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    acceptProposalFromProposalsPage(
      TERM_TO_PROPOSE_2,
      assetMap.entityOne,
      "term",
    );
  });

  it("can propose term and reject from proposals page", () => {
    proposeItems(
      assetMap.entityOne,
      [TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    rejectProposalFromProposalsPage(TERM_TO_PROPOSE);
  });

  it("can propose term and reject with a note from proposals page", () => {
    proposeItems(
      assetMap.entityOne,
      [TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    rejectProposalFromProposalsPage(TERM_TO_PROPOSE, PROPOSAL_REJECTION_NOTE);
    proposalsList.visit();
    proposalsList.clickFilterOption("status", "Completed");
    cy.get('[data-testid="proposal-result-note"]').first().trigger("mouseover");
    cy.contains(PROPOSAL_REJECTION_NOTE);
  });
});
