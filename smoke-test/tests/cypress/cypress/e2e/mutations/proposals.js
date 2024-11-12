const datasetName = "DatasetToProposeOn";
const addDescription = "Description to propose";
const updateDescription = "Description to proposes";

const datasetUrn = {
  dataPlatform:
    "urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)",
  dataPlatformDocument:
    "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)/Documentation",
  dataGlossaryTerm:
    "/glossaryTerm/urn:li:glossaryTerm:CypressNode.CypressTermToProposeOn/Documentation",
  datasetToProposeOn:
    "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)",
  datadashboard: "/dashboard/urn:li:dashboard:(looker,cypress_baz)",
  dataFlow:
    "/tasks/urn:li:dataJob:(urn:li:dataFlow:(airflow,cypress_dag_abc,PROD),cypress_task_123)",
};

describe("proposals", () => {
  beforeEach(() => {
    cy.login();
  });

  Cypress.on("uncaught:exception", (err, runnable) => false);

  const proposeDatasetDescription = () => {
    cy.visit(datasetUrn.dataPlatformDocument);
    cy.get('[data-testid="empty-tab-description"]').should("exist");
    cy.get('[data-testid="add-documentation"]').click({ force: true });
    cy.focused().type(addDescription);
    cy.get('[data-testid="propose-description"]').click({ force: true });
    cy.contains("Proposed description update!").should("be.visible");
    cy.contains("Proposed description update!").should("not.exist");
  };

  const proposeGlossaryDescription = () => {
    cy.visit(datasetUrn.dataPlatformDocument);
    cy.get('[data-testid="add-documentation"]')
      .should("be.visible")
      .click({ force: true });
    cy.focused().clear().type(updateDescription);
    cy.get('[data-testid="propose-description"]').click({ force: true });
    cy.contains("Proposed description update!").should("be.visible");
    cy.contains("Proposed description update!").should("not.exist");
  };

  const removeTagSchemaLevel = (tag) => {
    cy.get("#entity-profile-sidebar").within(() => {
      cy.contains(".ant-tag", tag)
        .find(".ant-tag-close-icon")
        .click({ force: true });
    });
    cy.contains("Yes").click();
    cy.get("#entity-profile-sidebar").contains(tag).should("not.exist");
  };

  const deletePreviousEntity = (entityName) => {
    cy.get(`#entity-profile-sidebar`)
      .invoke("text")
      .then((text) => {
        const storedVariable = text.trim();
        if (storedVariable.includes(entityName)) {
          removeTagSchemaLevel(entityName);
        } else {
          cy.ensureTextNotPresent(entityName);
        }
      });
  };

  const proposeTagOrTerm = (entityRoute, tagNameOrTermName, dataTestId) => {
    cy.visit(entityRoute);
    cy.wait(2000);
    deletePreviousEntity(tagNameOrTermName);
    cy.get(
      `[data-testid="proposed-${dataTestId}-${tagNameOrTermName}"]`,
    ).should("not.exist");
    // Click "Add Tags" or "Add Terms" based on the dataTestId
    cy.contains(
      `Add ${dataTestId.charAt(0).toUpperCase() + dataTestId.slice(1)}`,
    ).click({ force: true });
    // Type the tag or term name
    cy.get('[data-testid="tag-term-modal-input"]')
      .should("be.visible")
      .type(tagNameOrTermName);
    // Select the tag or term
    cy.get('[data-testid="tag-term-option"]')
      .should("be.visible")
      .within(() => cy.contains(tagNameOrTermName).click({ force: true }));
    // Click the create proposal button
    cy.get('[data-testid="create-proposal-btn"]').click({ force: true });
    cy.wait(1000);
    // Verify the proposed tag or term is visible
    cy.get(
      `[data-testid="proposed-${dataTestId}-${tagNameOrTermName}"]`,
    ).should("be.visible");
  };

  const acceptProposalDatasetPage = (to_type, thing) => {
    cy.waitTextVisible(to_type);
    cy.get(`[data-testid="proposed-${thing}-${to_type}"]`)
      .should("be.visible")
      .click({ force: true });
    cy.get(`[data-testid="proposal-accept-button-${to_type}"]`).click({
      force: true,
    });
    cy.get(`[data-testid="proposed-${thing}-${to_type}"]`).should("not.exist");
  };

  const rejectProposalDatasetPage = (to_type, thing) => {
    cy.waitTextVisible(to_type);
    cy.get(`[data-testid="proposed-${thing}-${to_type}"]`)
      .should("be.visible")
      .click({ force: true });
    cy.get(`[data-testid="proposal-reject-button-${to_type}"]`).click({
      force: true,
    });
    cy.get("#entity-profile-sidebar").contains(to_type).should("be.visible");
  };

  it.only("can propose description to dataset and then reject description proposal from the my requests tab", () => {
    proposeDatasetDescription();
    cy.rejectProposalInbox();
    cy.visit(datasetUrn.dataPlatformDocument);
    cy.get('[data-testid="empty-tab-description"]').should("exist");
  });

  it("can propose description to dataset and then accept description proposal from the my requests tab", () => {
    proposeDatasetDescription();
    cy.acceptProposalInbox();
    cy.visit(datasetUrn.dataPlatformDocument);
    cy.contains(addDescription);
  });

  it("can propose description to glossary term and then reject description proposal from the my requests tab", () => {
    proposeGlossaryDescription();
    cy.rejectProposalInbox();
    cy.visit(datasetUrn.dataGlossaryTerm);
    cy.contains(updateDescription).should("not.exist");
  });

  it("can propose description to glossary term and then accept description proposal from the my requests tab", () => {
    proposeGlossaryDescription();
    cy.acceptProposalInbox();
    cy.visit(datasetUrn.dataGlossaryTerm);
    cy.get('[data-testid="edit-documentation-button"]')
      .should("be.visible")
      .click({ force: true });
    // clean up from the test so its idempotent
    cy.focused().clear().type("This description has not been proposed on yet.");
    cy.get('[data-testid="description-editor-save-button"]').click({
      force: true,
    });
  });

  it("can propose tag to dataset and then decline tag proposal from the dataset page", () => {
    proposeTagOrTerm(datasetUrn.datasetToProposeOn, "TagToPropose", "tag");
    rejectProposalDatasetPage("TagToPropose", "tag");
  });

  it("can propose tag to dashboard and then decline tag proposal from the dashboard page", () => {
    proposeTagOrTerm(datasetUrn.datadashboard, "TagToPropose", "tag");
    rejectProposalDatasetPage("TagToPropose", "tag");
  });

  it("can propose tag to dataJob and then decline tag proposal from the dataJob page", () => {
    proposeTagOrTerm(datasetUrn.dataFlow, "TagToPropose", "tag");
    rejectProposalDatasetPage("TagToPropose", "tag");
  });

  it("can propose term to dataset and then decline term proposal from the dataset page", () => {
    proposeTagOrTerm(datasetUrn.datasetToProposeOn, "TermToPropose", "term");
    rejectProposalDatasetPage("TermToPropose", "term");
  });

  it("can propose term to dashboard and then decline term proposal from the dashboard page", () => {
    proposeTagOrTerm(datasetUrn.datadashboard, "TermToPropose", "term");
    rejectProposalDatasetPage("TermToPropose", "term");
  });

  it("can propose term to dataJob and then decline term proposal from the dataJob page", () => {
    proposeTagOrTerm(datasetUrn.dataFlow, "TermToPropose", "term");
    rejectProposalDatasetPage("TermToPropose", "term");
  });

  it("can propose tag to dataset and then accept tag proposal from the dataset page", () => {
    // Proposing the tag
    proposeTagOrTerm(datasetUrn.datasetToProposeOn, "TagToPropose", "tag");
    cy.waitTextPresent(datasetName);
    // Accepting the proposal
    acceptProposalDatasetPage("TagToPropose", "tag");

    // Deleting the tag (data cleanup)
    removeTagSchemaLevel("TagToPropose");
  });

  it("can propose term to dataset and then accept term proposal from the dataset page", () => {
    // Proposing the term
    proposeTagOrTerm(datasetUrn.datasetToProposeOn, "TermToPropose", "term");
    cy.waitTextPresent(datasetName);
    // Accepting the proposal
    acceptProposalDatasetPage("TermToPropose", "term");
    cy.contains("TermToPropose").within(() =>
      cy.get("span[aria-label=close]").click(),
    );
    cy.contains("Yes").click({ force: true });
  });

  it("can propose tag to dataset and then decline tag proposal from the my requests tab", () => {
    // Proposing the tag
    proposeTagOrTerm(datasetUrn.datasetToProposeOn, "TagToPropose", "tag");
    cy.waitTextPresent(datasetName);
    cy.searchNotCachedContainsDataset("TagToPropose", datasetName);
    cy.rejectProposalInbox();
    cy.searchNotCachedDoesNotContainDataset("TagToPropose", datasetName);
    cy.goToDataset(datasetUrn.dataPlatform, datasetName);
    cy.get('[data-testid="proposed-tag-TagToPropose"]').should("not.exist");
  });

  it("can propose term to dataset and then decline term proposal from the my requests tab", () => {
    // Proposing the term
    proposeTagOrTerm(datasetUrn.datasetToProposeOn, "TermToPropose", "term");
    cy.waitTextPresent(datasetName);
    cy.searchNotCachedContainsDataset("TermToPropose", datasetName);
    cy.rejectProposalInbox();
    cy.searchNotCachedDoesNotContainDataset("TermToPropose", datasetName);
    cy.goToDataset(datasetUrn.dataPlatform, datasetName);
    cy.get('[data-testid="proposed-term-TermToPropose"]').should("not.exist");
  });

  it("can propose tag to dataset and then accept tag proposal from the my requests tab", () => {
    // Proposing the tag
    proposeTagOrTerm(datasetUrn.datasetToProposeOn, "TagToPropose", "tag");
    cy.waitTextPresent(datasetName);
    cy.acceptProposalInbox();
    cy.searchNotCachedContainsDataset("TagToPropose", datasetName);
    cy.goToDataset(datasetUrn.dataPlatform, datasetName);
    removeTagSchemaLevel("TagToPropose");
  });

  it("can propose term to dataset and then accept term proposal from the my requests tab", () => {
    // Proposing the term
    proposeTagOrTerm(datasetUrn.datasetToProposeOn, "TermToPropose", "term");
    cy.waitTextPresent(datasetName);
    cy.acceptProposalInbox();
    cy.searchNotCachedContainsDataset("TermToPropose", datasetName);
    cy.goToDataset(datasetUrn.dataPlatform, datasetName);
    cy.contains("TermToPropose").within(() =>
      cy.get("span[aria-label=close]").click(),
    );
    cy.contains("Yes").click({ force: true });
  });
});
