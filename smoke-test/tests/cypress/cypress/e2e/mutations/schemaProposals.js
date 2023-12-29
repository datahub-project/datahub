let datasetUrn = 'urn:li:dataset:(urn:li:dataPlatform:hive,DatasetToProposeOn,PROD)';
let datasetName = 'DatasetToProposeOn';

let clickSchemaLevel = (field_id, text_to_click) => {
  cy.get('[data-testid="' + field_id + '"]').trigger('mouseover', {force: true});
  cy.get('[data-testid="' + field_id + '"]').within(() => cy.contains(text_to_click).click());
};

let clickAddTagsSchemaLevel = (field_id) => {
  clickSchemaLevel(field_id, 'Add Tags');
};

let clickAddTermsSchemaLevel = (field_id) => {
  clickSchemaLevel(field_id, 'Add Terms');
};

let createProposalOnModal = (to_type) => {
  cy.get('.ant-select-selector').eq(2).type(to_type);
  cy.get(".rc-virtual-list").find("div").contains(to_type).click();
  cy.get('[data-testid="create-proposal-btn"]').click({force: true});
  cy.contains(to_type).should('be.visible')
};

let acceptProposalDatasetPage = (to_type, thing) => {
  cy.waitTextVisible(to_type);
  cy.get('[data-testid="proposed-' + thing + '-' + to_type + '"]').should('be.visible').click({ force: true });
  cy.get('[data-testid="proposal-accept-button-' + to_type + '"]').click({ force: true });
  cy.get('[data-testid="proposed-' + thing + '-' + to_type + '"]').should('not.exist');
};

let rejectProposalDatasetPage = (to_type, thing) => {
  cy.waitTextVisible(to_type);
  cy.get('[data-testid="proposed-' + thing + '-' + to_type + '"]').should('be.visible').click({ force: true });
  cy.get('[data-testid="proposal-reject-button-' + to_type + '"]').click({ force: true });
  cy.get('[data-testid="proposed-' + thing + '-' + to_type + '"]').should('not.exist');
};

let removeTagSchemaLevel = (field_id, tag) => {
  cy.get('[data-testid=' + field_id + ']').within(() => {
    cy.contains('[data-testid=tag-' + tag + ']', tag)
      .find('.ant-tag-close-icon')
      .click({ force: true }  );
  }); 
  cy.contains('Yes').click();
  cy.contains(tag).should('not.exist');
};

describe('schemaProposals', () => {
  Cypress.on('uncaught:exception', (err, runnable) => {
    return false;
  });
  
  it('can propose a schema-level tag and then decline tag proposal from the dataset page', () => {
    cy.login();

    cy.goToDataset(datasetUrn, datasetName);
    clickAddTagsSchemaLevel('schema-field-field_foo-tags');
    createProposalOnModal('TagToPropose');
    rejectProposalDatasetPage('TagToPropose', 'tag');
  });

  it('can propose a schema-level term and then decline term proposal from the dataset page', () => {
    cy.login();

    cy.goToDataset(datasetUrn, datasetName);
    clickAddTermsSchemaLevel('schema-field-field_foo-terms');
    createProposalOnModal('TermToPropose');
    rejectProposalDatasetPage('TermToPropose', 'term');
  });

  it('can propose a schema-level tag and then accept tag proposal from the dataset page', () => {
    cy.login();

    // Proposing a tag
    cy.goToDataset(datasetUrn, datasetName);
    clickAddTagsSchemaLevel('schema-field-field_foo-tags');
    createProposalOnModal('TagToPropose'); 
    acceptProposalDatasetPage('TagToPropose', 'tag');

    removeTagSchemaLevel('schema-field-field_foo-tags', 'TagToPropose');
  });

  it('can propose a schema-level term and then accept term proposal from the dataset page', () => {
    cy.login();

    // Proposing a term
    cy.goToDataset(datasetUrn, datasetName);
    clickAddTermsSchemaLevel('schema-field-field_foo-terms');
    createProposalOnModal('TermToPropose');
    acceptProposalDatasetPage('TermToPropose', 'term');

    // Data cleanup
    cy.contains('TermToPropose').within(() => cy.get('span[aria-label=close]').dblclick());
    cy.contains('Yes').click();
  });
  
  it('can propose a schema-level tag and then decline the proposal from the Inbox tab', () => {
      cy.login();

      // Proposing a tag
      cy.goToDataset(datasetUrn, datasetName);
      clickAddTagsSchemaLevel('schema-field-field_foo-tags');
      createProposalOnModal('TagToPropose');
      cy.searchNotCachedContainsDataset('TagToPropose', datasetName);

      cy.rejectProposalInbox();
      cy.searchNotCachedDoesNotContainDataset('TagToPropose', datasetName);
  });

  it('can propose a schema-level glossary term and then decline the proposal from the Inbox tab', () => {
      cy.login();

      // Proposing a term
      cy.goToDataset(datasetUrn, datasetName);
      clickAddTermsSchemaLevel('schema-field-field_foo-terms');
      createProposalOnModal('TermToPropose');
      cy.searchNotCachedContainsDataset('TermToPropose', datasetName);
      cy.rejectProposalInbox();
      cy.searchNotCachedDoesNotContainDataset('TermToPropose', datasetName);
  });
    
  it('can propose a schema-level tag to a dataset and then accept the proposal from the Inbox tab', () => {
      cy.login();

      // Proposing a tag
      cy.goToDataset(datasetUrn, datasetName);
      clickAddTagsSchemaLevel('schema-field-field_foo-tags');
      createProposalOnModal('TagToPropose');
      cy.searchNotCachedContainsDataset('TagToPropose', datasetName);

      cy.acceptProposalInbox();
      cy.searchNotCachedContainsDataset('TagToPropose', datasetName);

      // Verifying the applied tag is present
      cy.goToDataset(datasetUrn, datasetName);
      cy.get('[data-testid="schema-field-field_foo-tags"]').should('be.visible').contains('TagToPropose');
      cy.get('[data-testid="proposed-tag-TagToPropose"]').should('not.exist');

      removeTagSchemaLevel('schema-field-field_foo-tags', 'TagToPropose');
  });

  it('can propose a schema-level term to dataset and then accept the proposal from the Inbox tab', () => {
      cy.login();

      // Proposing a term
      cy.goToDataset(datasetUrn, datasetName);
      clickAddTermsSchemaLevel('schema-field-field_foo-terms');
      createProposalOnModal('TermToPropose');
      cy.searchNotCachedContainsDataset('TermToPropose', datasetName); 
      cy.acceptProposalInbox();
      cy.searchNotCachedContainsDataset('TermToPropose', datasetName);

      // Verifying the applied glossary term is present
      cy.goToDataset(datasetUrn, datasetName);
      cy.get('[data-testid="schema-field-field_foo-terms"]').should('be.visible').contains('TermToPropose');

      // Data cleanup
      cy.contains('TermToPropose').within(() => cy.get('span[aria-label=close]').click({force: true}));
      cy.contains('Yes').click();
  });
  
  })
 