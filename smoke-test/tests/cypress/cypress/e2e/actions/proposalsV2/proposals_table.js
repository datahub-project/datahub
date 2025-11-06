import { hasOperationName } from "../../utils";
import { proposalsList } from "./helpers/ProposalsList";
import {
  proposeItems,
  rejectProposalFromProposalsPage,
  setThemeV2AndTaskCenterRedesignFlags,
} from "./utils";

const assetMap = {
  entityOne: {
    urn: "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_created,PROD)/",
    name: "fct_cypress_users_created",
  },
  entityTwo: {
    urn: "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)/",
    name: "cypress_logging_events",
  },
};

const TERM_TO_PROPOSE = "TermToPropose";

describe("Proposals Table Interactions", () => {
  beforeEach(() => {
    setThemeV2AndTaskCenterRedesignFlags(true);
    cy.login();
  });

  Cypress.on("uncaught:exception", (err, runnable) => false);

  it("should properly handle the opening and closing of the entity sidebar", () => {
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
      [TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    rejectProposalFromProposalsPage(TERM_TO_PROPOSE);

    proposalsList.visit();
    proposalsList.clickFilterOption("status", "Completed");

    // Check if the clicking the row opens the sidebar with correct data
    proposalsList.clickRowContaining(assetMap.entityOne.name);
    proposalsList
      .getSidebar()
      .should("be.visible")
      .waitTextVisible(assetMap.entityOne.name);
    proposalsList.clickRowContaining(assetMap.entityTwo.name);
    proposalsList
      .getSidebar()
      .should("be.visible")
      .waitTextVisible(assetMap.entityTwo.name);

    // Check if clicking the same row removes the sidebar
    proposalsList.clickRowContaining(assetMap.entityTwo.name);
    proposalsList.getSidebar().should("not.exist");
  });

  it("should not allow selection in MyProposals", () => {
    proposalsList.visit();
    proposalsList.visitMyProposals();

    cy.get('[data-testid="proposals-table"]')
      .get("tbody tr")
      .should("be.visible")
      .find("#checked-input")
      .should("not.exist");
  });

  it("should properly handle the row selection", () => {
    proposalsList.visit();
    proposeItems(
      assetMap.entityOne,
      [TERM_TO_PROPOSE],
      "term",
      "add-terms-button",
      "tag-term-modal-input",
    );
    proposalsList.visit();

    // Select rows and check if the footer shows up
    cy.get('[data-testid="proposals-table"]')
      .get("tbody tr")
      .find("#checked-input:not([disabled])")
      .first()
      .parent()
      .click();

    proposalsList.getFooter().contains("1").should("exist");

    // The footer should vanish upon switching to other group
    proposalsList.visitMyProposals();
    proposalsList.getFooter().should("not.exist");
    rejectProposalFromProposalsPage(TERM_TO_PROPOSE, "term");
  });
});
