import { aliasQuery } from "../utils";

const DATASET_NAME = "SampleCypressKafkaDataset";
const DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleCypressKafkaDataset,PROD)";
const DATASET_URL = `/dataset/${DATASET_URN}`;

const ADMIN_USER_URN = "urn:li:corpuser:admin";

const FORM_NAME = "Test name";
const FORM_NAME_EDITED = "Test name edited";
const FORM_DESCRIPTION = "Test description";
const FORM_DESCRIPTION_EDITED = "Test description edited";

const QUESTION_TITLE = "Test question title";
const QUESTION_DESCRIPTION = "Test question description";

const goToComplianceFormsPage = () => {
  cy.visit("/govern/dashboard?documentationTab=forms");
};

const goToEntity = () => {
  cy.visit(DATASET_URL);
  cy.waitTextVisible(DATASET_NAME);
};

const goToCreateNewComplianceFormPage = () => {
  cy.visit("/govern/dashboard/new-form");
};

const goToEditFormPage = (formUrn) => {
  cy.visit(`/govern/dashboard/edit-form/${formUrn}`);
  cy.getWithTestId("form-title").should("be.visible");
  // wait for the fields filled
  cy.getWithTestId("name-input").should("not.be.empty");
  cy.getWithTestId("description-textarea").should("not.be.empty");
};

const fillForm = (name, description) => {
  cy.getWithTestId("name-input").type(name);
  cy.getWithTestId("description-textarea").type(description);

  // Add question (documentation)
  cy.clickOptionWithTestId("add-questions-button");
  cy.getWithTestId("select-question-type").click();
  cy.clickOptionWithTestId("select-question-type-option-documentation");
  cy.getWithTestId("question-title-input").type(
    `{selectall}{backspace}${QUESTION_TITLE}`,
  );
  cy.getWithTestId("question-description-textarea").type(
    `{selectall}{backspace}${QUESTION_DESCRIPTION}`,
  );
  cy.clickOptionWithTestId("save-button");

  // Assign asset
  cy.clickOptionWithTestId("add-assets-button");
  cy.clickOptionWithTestId("query-builder-add-condition-button");
  cy.getWithTestId("condition-select").within(() => {
    cy.get(".ant-select-selector").click();
  });
  cy.clickOptionWithTestId("condition-select-option-urn");
  cy.getWithTestId("condition-operator-select").within(() =>
    cy.get(".ant-select-selector").click(),
  );
  cy.clickOptionWithTestId("condition-operator-select-option-equals");
  cy.getWithTestId("entity-search-input").type(DATASET_NAME);
  cy.clickOptionWithTestId(`${DATASET_URN}-entity-search-input-result`);

  // Asign recipients
  cy.clickOptionWithTestId("add-recipients-button");
  cy.getWithTestId("user-select").type(Cypress.env("ADMIN_DISPLAYNAME"));
  cy.clickOptionWithTestId(`select-result-option-${ADMIN_USER_URN}`);
  cy.clickOptionWithTestId("add-button");
};

const editForm = (name, description) => {
  cy.getWithTestId("name-input").type(`{selectall}{backspace}${name}`);
  cy.getWithTestId("description-textarea").type(
    `{selectall}{backspace}${description}`,
  );
};

const saveDraft = (isCreation) => {
  cy.clickOptionWithTestId("form-save-draft-button");
  if (isCreation) {
    cy.wait("@gqlcreateFormQuery")
      .its("response.body.data.createForm.urn")
      .as("createdFormUrn");
  }
};

const deleteForm = (formUrn) => {
  cy.getWithTestId(`${formUrn}-name`)
    .parents("tr")
    .find(`[data-testid="MoreVertOutlinedIcon"]`)
    .click();

  cy.clickOptionWithTestId("action-delete");
  cy.clickOptionWithTestId("modal-confirm-button");
};

const publishForm = () => {
  cy.clickOptionWithTestId("form-publish-button");
  cy.clickOptionWithTestId("modal-confirm-button");
};

const unpublishForm = () => {
  cy.clickOptionWithTestId("form-unpublish-button");
  cy.clickOptionWithTestId("modal-confirm-button");
};

const startFormCompleting = () => {
  cy.clickOptionWithTestId("incomplete-documentation-title");
  cy.clickOptionWithTestId("complete-documentation-button");
};

const checkQuestionsOnForm = () => {
  cy.getWithTestId("prompt-title")
    .contains(QUESTION_TITLE)
    .should("be.visible");

  cy.getWithTestId("prompt-required").should("exist");
};

const checkIfEntityHasAwaitingForms = () => {
  cy.getWithTestId("incomplete-documentation-title").should("be.visible");
};

const checkIfEntityHasNoAwaitingForms = () => {
  cy.getWithTestId("incomplete-documentation-title").should("not.to.exist");
};

const checkForm = (formUrn, name, description, status) => {
  // check name
  cy.getWithTestId(`${formUrn}-name`).should("have.text", name);
  // check description
  cy.getWithTestId(`${formUrn}-description`).should("have.text", description);
  // check status
  cy.getWithTestId(`${formUrn}-status-${status}`).should("be.visible");
};

const checkIfFormDoesNotExist = (formUrn) => {
  cy.getWithTestId(`${formUrn}-name`).should("not.exist");
};

describe("manage compliance forms", () => {
  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.login();
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "createForm");
    }).as("apiCall");
  });

  it("allows to create the form", () => {
    goToCreateNewComplianceFormPage();
    fillForm(FORM_NAME, FORM_DESCRIPTION);
    saveDraft(true);

    cy.get("@createdFormUrn").then((formUrn) => {
      // Check the created form
      goToComplianceFormsPage();
      checkForm(formUrn, FORM_NAME, FORM_DESCRIPTION, "draft");

      // Check if draft form not available on the related entity
      cy.wait(3000); // wait for ES to update with form on it
      goToEntity();
      checkIfEntityHasNoAwaitingForms();

      goToComplianceFormsPage();
      deleteForm(formUrn);
    });
  });

  it("allows to edit the form", () => {
    goToCreateNewComplianceFormPage();
    fillForm(FORM_NAME, FORM_DESCRIPTION);
    saveDraft(true);

    cy.get("@createdFormUrn").then((formUrn) => {
      goToEditFormPage(formUrn);
      editForm(FORM_NAME_EDITED, FORM_DESCRIPTION_EDITED);
      saveDraft();

      // Check the edited form
      goToComplianceFormsPage();
      checkForm(formUrn, FORM_NAME_EDITED, FORM_DESCRIPTION_EDITED, "draft");

      deleteForm(formUrn);
    });
  });

  it("allows to publish the form", () => {
    goToCreateNewComplianceFormPage();
    fillForm(FORM_NAME, FORM_DESCRIPTION);
    saveDraft(true);

    cy.get("@createdFormUrn").then((formUrn) => {
      goToEditFormPage(formUrn);
      publishForm();

      // Check state of the form
      goToComplianceFormsPage();
      checkForm(formUrn, FORM_NAME, FORM_DESCRIPTION, "published");

      // Check if the form is available on the entity
      cy.wait(3000); // wait for ES to update with form on it
      goToEntity();
      checkIfEntityHasAwaitingForms();

      // Check questions on the form
      startFormCompleting();
      checkQuestionsOnForm();

      goToComplianceFormsPage();
      deleteForm(formUrn);
    });
  });

  it("allows to unpublish the published form", () => {
    goToCreateNewComplianceFormPage();
    fillForm(FORM_NAME, FORM_DESCRIPTION);
    saveDraft(true);

    cy.get("@createdFormUrn").then((formUrn) => {
      goToEditFormPage(formUrn);
      publishForm();
      goToEditFormPage(formUrn);
      unpublishForm();

      // Check if the form is not available on the entity
      cy.wait(3000); // wait for ES to update with form on it
      goToEntity();
      checkIfEntityHasNoAwaitingForms();

      goToComplianceFormsPage();
      deleteForm(formUrn);
    });
  });

  it("allows to delete the draft form", () => {
    goToCreateNewComplianceFormPage();
    fillForm(FORM_NAME, FORM_DESCRIPTION);
    saveDraft(true);

    cy.get("@createdFormUrn").then((formUrn) => {
      goToComplianceFormsPage();
      deleteForm(formUrn);

      // Check if the deleted form does not exist
      checkIfFormDoesNotExist(formUrn);
    });
  });

  it("allows to delete the published form", () => {
    goToCreateNewComplianceFormPage();
    fillForm(FORM_NAME, FORM_DESCRIPTION);
    saveDraft(true);

    cy.get("@createdFormUrn").then((formUrn) => {
      goToEditFormPage(formUrn);
      publishForm();
      goToComplianceFormsPage();
      deleteForm(formUrn);

      // Check if the deleted form does not exist
      checkIfFormDoesNotExist(formUrn);

      // Check if the deleted form is not available on the entity
      cy.wait(3000); // wait for ES to update with form on it
      goToEntity();
      checkIfEntityHasNoAwaitingForms();
    });
  });
});
