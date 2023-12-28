let datasetUrn = 'urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)';
let datasetName = 'DatasetToProposeOn';

describe("proposals", () => {
  Cypress.on("uncaught:exception", (err, runnable) => {
    return false;
  });

  function proposeDatasetDescription() {
    cy.login();

    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)/Documentation"
    );

    cy.get('[data-testid="empty-tab-description"]').should("exist");
    cy.get('[data-testid="add-documentation"]').click({ force: true });

    cy.focused().type("Description to propose");
    cy.get('[data-testid="propose-description"]').click({ force: true });

    cy.wait(2000);
  }

  function proposeGlossaryDescription() {
    cy.login();

    cy.visit(
      "/glossaryTerm/urn:li:glossaryTerm:CypressNode.CypressTermToProposeOn/Documentation"
    );

    //cy.get('[data-testid="edit-documentation-button"]').click({ force: true });
    cy.get('[data-testid="add-documentation"]').click({ force: true });

    cy.focused().clear().type("Description to propose");
    cy.get('[data-testid="propose-description"]').click({ force: true });

    cy.wait(2000);
  }

  it("can propose description to dataset and then reject description proposal from the my requests tab", () => {
    proposeDatasetDescription();

    cy.rejectProposalInbox();

    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)/Documentation"
    );
    cy.get('[data-testid="empty-tab-description"]').should("exist");
    cy.wait(1000);
  });

  it("can propose description to dataset and then accept description proposal from the my requests tab", () => {
    proposeDatasetDescription();

    cy.acceptProposalInbox();

    cy.visit(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)/Documentation"
    );
    cy.contains("Description to propose");

    cy.wait(1000);
  });

  it("can propose description to glossary term and then reject description proposal from the my requests tab", () => {
    proposeGlossaryDescription();

    cy.rejectProposalInbox();

    cy.visit(
      "/glossaryTerm/urn:li:glossaryTerm:CypressNode.CypressTermToProposeOn/Documentation"
    );
    cy.contains("Description to propose").should("not.exist");

    cy.wait(1000);
  });

  it("can propose description to glossary term and then accept description proposal from the my requests tab", () => {
    proposeGlossaryDescription();

    cy.acceptProposalInbox();

    cy.visit(
      "/glossaryTerm/urn:li:glossaryTerm:CypressNode.CypressTermToProposeOn/Documentation"
    );
    cy.contains("Description to propose");

    cy.wait(1000);

    cy.get('[data-testid="edit-documentation-button"]').click({ force: true });

    // clean up from the test so its idempotent
    cy.focused().clear().type("This description has not been proposed on yet.");
    cy.get('[data-testid="description-editor-save-button"]').click({ force: true });
  });

  function proposeTagAndDeclineOnProfile(entityRoute) {
    cy.login();

    // Proposing the tag
    cy.visit(entityRoute);

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should("not.exist");

    cy.contains("Add Tags").click({ force: true });
    cy.wait(1000);

    // cy.focused().type('TagToPropose');
    cy.get('[data-testid="tag-term-modal-input"]').type("TagToPropose");
    cy.wait(3000);

    cy.get(".ant-select-item-option-content").within(() =>
      cy.contains("TagToPropose").click({ force: true })
    );
    cy.get('[data-testid="create-proposal-btn"]').click({ force: true });
    cy.wait(5000);
    cy.reload();

    // Rejecting the proposal
    cy.get('[data-testid="proposed-tag-TagToPropose"]').click({ force: true });
    cy.get('[data-testid="proposal-reject-button-TagToPropose"]').click({
      force: true,
    });
    cy.wait(1000);

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should("not.exist");
  }

  it("can propose tag to dataset and then decline tag proposal from the dataset page", () => {
    proposeTagAndDeclineOnProfile(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)"
    );
  });

  it("can propose tag to dashboard and then decline tag proposal from the dashboard page", () => {
    proposeTagAndDeclineOnProfile(
      "/dashboard/urn:li:dashboard:(looker,cypress_baz)"
    );
  });

  it("can propose tag to dataJob and then decline tag proposal from the dataJob page", () => {
    proposeTagAndDeclineOnProfile(
      "/tasks/urn:li:dataJob:(urn:li:dataFlow:(airflow,cypress_dag_abc,PROD),cypress_task_123)"
    );
  });

  function proposeTermAndDeclineOnProfile(entityRoute) {
    cy.login();

    // Proposing the term
    cy.visit(entityRoute);

    cy.get('[data-testid="proposed-term-TermToPropose"]').should("not.exist");
    cy.contains("Add Terms").click({ force: true });
    cy.wait(1000);

    cy.get('[data-testid="tag-term-modal-input"]').type("TermToPropose");
    cy.wait(3000);

    cy.contains("TermToPropose").click({ force: true });

    cy.get(".ant-select-item-option-content").within(() =>
      cy.contains("TermToPropose").click({ force: true })
    );
    cy.get('[data-testid="create-proposal-btn"]').click({ force: true });
    cy.wait(5000);
    cy.reload();

    // Rejecting the proposal
    cy.get('[data-testid="proposed-term-TermToPropose"]').click({
      force: true,
    });
    cy.get('[data-testid="proposal-reject-button-TermToPropose"]').click({
      force: true,
    });
    cy.wait(1000);

    cy.get('[data-testid="proposed-term-TermToPropose"]').should("not.exist");
  }

  it("can propose term to dataset and then decline term proposal from the dataset page", () => {
    proposeTermAndDeclineOnProfile(
      "/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)"
    );
  });

  it("can propose term to dashboard and then decline term proposal from the dashboard page", () => {
    proposeTermAndDeclineOnProfile(
      "/dashboard/urn:li:dashboard:(looker,cypress_baz)"
    );
  });

  it("can propose term to dataJob and then decline term proposal from the dataJob page", () => {
    proposeTermAndDeclineOnProfile(
      "/tasks/urn:li:dataJob:(urn:li:dataFlow:(airflow,cypress_dag_abc,PROD),cypress_task_123)"
    );
  });

  it("can propose tag to dataset and then accept tag proposal from the dataset page", () => {
    cy.login();

    // Proposing the tag
    cy.goToDataset(datasetUrn, datasetName);
    cy.contains("Add Tags").click({ force: true });

    cy.get('[data-testid="tag-term-modal-input"]').type("TagToPropose");
    cy.wait(3000);

    cy.get(".ant-select-item-option-content").within(() =>
      cy.contains("TagToPropose").click({ force: true })
    );

    cy.get('[data-testid="create-proposal-btn"]').click({ force: true });
    cy.wait(5000);
    cy.reload();

    // Accepting the proposal
    cy.get('[data-testid="proposed-tag-TagToPropose"]').click({ force: true });
    cy.get('[data-testid="proposal-accept-button-TagToPropose"]').click({
      force: true,
    });
    cy.wait(3000);

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should("not.exist");
    cy.wait(1000);

    // Deleting the tag (data cleanup)
    cy.get('[data-testid="tag-TagToPropose"]').within(() =>
      cy.get("span[aria-label=close]").click({ force: true })
    );
    cy.wait(1000);

    cy.contains("Yes").click({ force: true });
    cy.wait(1000);
  });

  it("can propose term to dataset and then accept term proposal from the dataset page", () => {
    cy.login();

    // Proposing the term
    cy.goToDataset(datasetUrn, datasetName);

    cy.get('[data-testid="proposed-term-TermToPropose"]').should("not.exist");
    cy.contains("Add Terms").click({ force: true });
    cy.wait(1000);

    cy.get('[data-testid="tag-term-modal-input"]').type("TermToPropose");
    cy.wait(3000);

    cy.contains("TermToPropose").click({ force: true });

    cy.get(".ant-select-item-option-content").within(() =>
      cy.contains("TermToPropose").click({ force: true })
    );
    cy.get('[data-testid="create-proposal-btn"]').click({ force: true });
    cy.wait(5000);
    cy.reload();

    // Accepting the proposal
    cy.get('[data-testid="proposed-term-TermToPropose"]').click({
      force: true,
    });
    cy.get('[data-testid="proposal-accept-button-TermToPropose"]').click({
      force: true,
    });
    cy.wait(3000);

    cy.get('[data-testid="proposed-term-TermToPropose"]').should("not.exist");
    cy.wait(1000);

    // Deleting the term (data cleanup)
    cy.contains("TermToPropose").within(() =>
      cy.get("span[aria-label=close]").click()
    );
    cy.contains("Yes").click({ force: true });
    cy.wait(1000);
  });

  it("can propose tag to dataset and then decline tag proposal from the my requests tab", () => {
    cy.login();

    // Proposing the tag
    cy.goToDataset(datasetUrn, datasetName);

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should("not.exist");

    cy.contains("Add Tags").click({ force: true });

    cy.get('[data-testid="tag-term-modal-input"]').type("TagToPropose");
    cy.wait(3000);

    cy.get(".ant-select-item-option-content").within(() =>
      cy.contains("TagToPropose").click({ force: true })
    );

    cy.get('[data-testid="create-proposal-btn"]').click({ force: true });
    cy.wait(5000);
    cy.reload();

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should("exist");

    cy.searchNotCachedContainsDataset("TagToPropose", datasetName);

    cy.rejectProposalInbox();
    cy.searchNotCachedDoesNotContainDataset("TagToPropose", datasetName);

    cy.goToDataset(datasetUrn, datasetName);
    cy.get('[data-testid="proposed-tag-TagToPropose"]').should("not.exist");
  });

  it("can propose term to dataset and then decline term proposal from the my requests tab", () => {
    cy.login();

    // Proposing the term
    cy.goToDataset(datasetUrn, datasetName);

    cy.get('[data-testid="proposed-term-TermToPropose"]').should("not.exist");
    cy.contains("Add Terms").click({ force: true });

    cy.get('[data-testid="tag-term-modal-input"]').type("TermToPropose");
    cy.wait(3000);

    cy.contains("TermToPropose").click({ force: true });

    cy.get(".ant-select-item-option-content").within(() =>
      cy.contains("TermToPropose").click({ force: true })
    );
    cy.get('[data-testid="create-proposal-btn"]').click({ force: true });
    cy.wait(5000);
    cy.reload();

    cy.get('[data-testid="proposed-term-TermToPropose"]').should("exist");

    cy.searchNotCachedContainsDataset("TermToPropose", datasetName);

    cy.rejectProposalInbox();
    cy.searchNotCachedDoesNotContainDataset("TermToPropose", datasetName);

    cy.goToDataset(datasetUrn, datasetName);
    cy.get('[data-testid="proposed-term-TermToPropose"]').should("not.exist");
  });

  it("can propose tag to dataset and then accept tag proposal from the my requests tab", () => {
    cy.login();

    // Proposing the tag
    cy.goToDataset(datasetUrn, datasetName);
    cy.contains("Add Tags").click({ force: true });

    cy.get('[data-testid="tag-term-modal-input"]').type("TagToPropose");
    cy.wait(3000);

    cy.get(".ant-select-item-option-content").within(() =>
      cy.contains("TagToPropose").click({ force: true })
    );

    cy.get('[data-testid="create-proposal-btn"]').click({ force: true });
    cy.wait(5000);
    cy.reload();

    cy.get('[data-testid="proposed-tag-TagToPropose"]').should("exist");

    cy.acceptProposalInbox();

    cy.searchNotCachedContainsDataset("TagToPropose", datasetName);

    cy.goToDataset(datasetUrn, datasetName);
    cy.get('[data-testid="proposed-tag-TagToPropose"]').should("not.exist");

    cy.get('[data-testid="tag-TagToPropose"]').within(() =>
      cy.get("span[aria-label=close]").click({ force: true })
    );
    // cy.contains('TagToPropose').within(() => cy.get('span[aria-label=close]').click({force: true}));
    cy.wait(1000);

    cy.contains("Yes").click({ force: true });
    cy.wait(1000);
  });

  it("can propose term to dataset and then accept term proposal from the my requests tab", () => {
    cy.login();

    // Proposing the term
    cy.goToDataset(datasetUrn, datasetName);

    cy.get('[data-testid="proposed-term-TermToPropose"]').should("not.exist");
    cy.contains("Add Terms").click({ force: true });

    cy.get('[data-testid="tag-term-modal-input"]').type("TermToPropose");
    cy.wait(3000);

    cy.contains("TermToPropose").click({ force: true });

    cy.get(".ant-select-item-option-content").within(() =>
      cy.contains("TermToPropose").click({ force: true })
    );
    cy.get('[data-testid="create-proposal-btn"]').click({ force: true });
    cy.wait(5000);
    cy.reload();

    cy.get('[data-testid="proposed-term-TermToPropose"]').should("exist");

    cy.wait(1000);

    cy.acceptProposalInbox();

    cy.searchNotCachedContainsDataset("TermToPropose", datasetName);

    cy.goToDataset(datasetUrn, datasetName);
    cy.get('[data-testid="proposed-term-TermToPropose"]').should("not.exist");
    cy.wait(1000);

    cy.contains("TermToPropose").within(() =>
      cy.get("span[aria-label=close]").click()
    );
    cy.contains("Yes").click({ force: true });
    cy.wait(1000);
  });
});
