const test_id = `test_v2_edit_documentation_${new Date().getTime()}`;

const SAMPLE_DATASET_NAME = "SampleCypressHiveDataset";
const SAMPLE_DATASET_URN =
  "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)";
const SAMPLE_DOCUMENTAION = `This is ${test_id} documentation EDITED`;

const getSampleUrl = (path) => {
  const url = `https://${test_id}.com`;
  if (!path) return url;
  return `${url}/${path}`;
};

const removeLinksByUrl = (url) => {
  cy.getWithTestId("link-list").within(() => {
    cy.get(`[href='${url}']`).each(($el) => {
      cy.wrap($el)
        .closest(".ant-list-item")
        .within(() => cy.clickOptionWithTestId("remove-link-button"));
    });
  });
  cy.waitTextVisible("Link Removed");
};

const fillLinksForm = (url, label, shouldShowInPreview) => {
  cy.clearTextInTestId("link-form-modal-url");
  cy.enterTextInTestId("link-form-modal-url", url);
  cy.clearTextInTestId("link-form-modal-label");
  cy.enterTextInTestId("link-form-modal-label", label);

  cy.getWithTestId("link-form-modal-show-in-asset-preview")
    .children("input")
    .invoke("attr", "aria-checked")
    .then((value) => {
      const isChecked = value === "true";
      // Toggle checkbox if needed
      if (isChecked && !shouldShowInPreview) {
        cy.clickOptionWithTestId("link-form-modal-show-in-asset-preview");
      } else if (!isChecked && shouldShowInPreview) {
        cy.clickOptionWithTestId("link-form-modal-show-in-asset-preview");
      }
    });
};

const submitForm = () =>
  cy.clickOptionWithTestId("link-form-modal-submit-button");

const openAddLinkForm = () => {
  cy.clickOptionWithTestId("add-link-button").wait(1000);
};

const addLink = (url, label, shouldShowInPreview) => {
  openAddLinkForm();
  fillLinksForm(url, label, shouldShowInPreview);
  submitForm();
  cy.waitTextVisible("Link Added");
};

const opendUpdateLinkForm = (url, label) => {
  cy.getWithTestId("link-list").within(() => {
    cy.get(`[href='${url}']`).each(($el) => {
      cy.wrap($el)
        .closest(".ant-list-item")
        .contains(label)
        .closest(".ant-list-item")
        .should("exist")
        .within(() => cy.clickOptionWithTestId("edit-link-button"));
    });
  });
};

const goToEntityDocumentationTab = () => {
  cy.goToDataset(SAMPLE_DATASET_URN, SAMPLE_DATASET_NAME);
  cy.openEntityTab("Documentation");
};

const goToSearchPage = () => {
  cy.visit(`/search?query=${SAMPLE_DATASET_NAME}`);
  cy.wait(3000);
  cy.waitTextVisible(SAMPLE_DATASET_NAME);
};

const updateLink = (
  currentUrl,
  currentLabel,
  url,
  label,
  shouldShowInPreview,
) => {
  opendUpdateLinkForm(currentUrl, currentLabel);
  fillLinksForm(url, label, shouldShowInPreview);
  submitForm();
  cy.waitTextVisible("Link Updated");
};

const ensureThatUrlIsAvaliableOnDocumentationTab = (url) => {
  cy.getWithTestId("link-list").within(() => {
    cy.get(`[href='${url}']`).should("have.length", 1);
  });
};

const ensureThatUrlIsNotAvaliableOnDocumentationTab = (url) => {
  cy.getWithTestId("link-list").within(() => {
    cy.get(`[href='${url}']`).should("not.exist");
  });
};

const ensureThatUrlIsAvaliableOnEntityHeader = (url) => {
  cy.getWithTestId("platform-links-container").within(() => {
    cy.getWithTestId("overflow-list-container").within(() => {
      cy.get(`[href='${url}']`).should("be.visible");
    });
  });
};

const ensureThatUrlIsNotAvaliableOnEntityHeader = (url) => {
  cy.get(`[data-testid="platform-links-container"] > [href='${url}']`).should(
    "not.exist",
  );
};

const ensureThatUrlIsAvaliableOnEntityHeaderInViewMore = (_url) => {
  cy.getWithTestId("platform-links-container").should("be.visible");
  // FYI: Dropdown's content is not shown in cypress, so we ignore it for now and just check if dropdown is exist
  // cy.getWithTestId("platform-links-container").within(() => {
  //   cy.clickOptionWithTestId('view-more-dropdown');
  // }).then(() => {
  //   cy.getWithTestId(`${url}-${url}`).should('be.visible');
  // }).then(() => {
  //   cy.getWithTestId("platform-links-container").within(() => {
  //     cy.clickOptionWithTestId('view-more-dropdown');
  //   })
  // });
};

const ensureThatUrlIsAvaliableOnSidebar = (url) => {
  cy.getWithTestId("sidebar-section-content-Documentation").within(() => {
    cy.get(`[href='${url}']`).should("be.visible");
  });
};

const ensureThatUrlIsNotAvaliableOnSidebar = (url) => {
  cy.getWithTestId("sidebar-section-content-Documentation").within(() => {
    cy.ensureTextNotPresent(url);
  });
};

describe("edit documentation and link to dataset", () => {
  // https://github.com/cypress-io/cypress/issues/29277
  Cypress.on(
    "uncaught:exception",
    (err) => !err.message.includes("ResizeObserver loop"),
  );

  beforeEach(() => {
    cy.setIsThemeV2Enabled(true);
    cy.ignoreResizeObserverLoop();
    cy.loginWithCredentials();
    goToEntityDocumentationTab();
  });

  it("should allow to edit the documentation", () => {
    cy.openEntityTab("Documentation");
    cy.clickOptionWithTestId("edit-documentation-button");
    cy.focused().clear();
    cy.focused().type(SAMPLE_DOCUMENTAION);
    cy.clickOptionWithTestId("description-editor-save-button");
    cy.waitTextVisible("Description Updated");
    cy.waitTextVisible(SAMPLE_DOCUMENTAION);
    // return documentation to original state
    cy.clickOptionWithTestId("edit-documentation-button");
    cy.focused().clear().wait(1000);
    cy.focused().type("my hive dataset");
    cy.clickOptionWithTestId("description-editor-save-button");
    cy.waitTextVisible("Description Updated");
    cy.waitTextVisible("my hive dataset");
  });

  it("should validate add link form", () => {
    openAddLinkForm();

    // Should validate url
    cy.enterTextInTestId("link-form-modal-url", "incorrect_url");
    cy.waitTextVisible("This field must be a valid url.");

    // Url should be required
    cy.focused().clear();
    cy.waitTextVisible("A URL is required.");

    // The label should be required
    cy.enterTextInTestId("link-form-modal-label", "label");
    cy.focused().clear();
    cy.waitTextVisible("A label is required.");
  });

  it("should successflully add new link", () => {
    const sample = getSampleUrl("add-new-link");

    addLink(sample, sample, false);

    ensureThatUrlIsAvaliableOnDocumentationTab(sample);
    ensureThatUrlIsAvaliableOnSidebar(sample);
    ensureThatUrlIsNotAvaliableOnEntityHeader(sample);

    removeLinksByUrl(sample);
  });

  it("should successflully add new link with showing in asset preview", () => {
    const sample = getSampleUrl("add-with-show-in-preview");

    addLink(sample, sample, true);

    ensureThatUrlIsAvaliableOnDocumentationTab(sample);
    ensureThatUrlIsAvaliableOnEntityHeader(sample);
    ensureThatUrlIsNotAvaliableOnSidebar(sample);

    goToSearchPage();
    ensureThatUrlIsAvaliableOnEntityHeader(sample);

    goToEntityDocumentationTab();
    removeLinksByUrl(sample);
  });

  it("should collapse links in the entity header", () => {
    const sample1 = getSampleUrl("collapse1");
    const sample2 = getSampleUrl("collapse2");
    const sample3 = getSampleUrl("collapse3");

    addLink(sample1, sample1, true);
    addLink(sample2, sample2, true);
    addLink(sample3, sample3, true);

    ensureThatUrlIsAvaliableOnDocumentationTab(sample1);
    ensureThatUrlIsAvaliableOnDocumentationTab(sample2);
    ensureThatUrlIsAvaliableOnDocumentationTab(sample3);
    ensureThatUrlIsNotAvaliableOnSidebar(sample1);
    ensureThatUrlIsNotAvaliableOnSidebar(sample2);
    ensureThatUrlIsNotAvaliableOnSidebar(sample3);
    ensureThatUrlIsAvaliableOnEntityHeader(sample1);
    ensureThatUrlIsAvaliableOnEntityHeader(sample2);
    ensureThatUrlIsAvaliableOnEntityHeaderInViewMore(sample3);

    removeLinksByUrl(sample1);
    removeLinksByUrl(sample2);
    removeLinksByUrl(sample3);
  });

  it("should successfully update the link", () => {
    const sample_url = getSampleUrl("edit_link");
    const sample_edited_url = getSampleUrl("edit_link_edited");

    addLink(sample_url, sample_url, false);

    ensureThatUrlIsAvaliableOnDocumentationTab(sample_url);
    ensureThatUrlIsAvaliableOnSidebar(sample_url);
    ensureThatUrlIsNotAvaliableOnEntityHeader(sample_url);

    updateLink(
      sample_url,
      sample_url,
      sample_edited_url,
      sample_edited_url,
      true,
    );

    ensureThatUrlIsNotAvaliableOnDocumentationTab(sample_url, sample_url);
    ensureThatUrlIsAvaliableOnDocumentationTab(sample_edited_url);
    ensureThatUrlIsAvaliableOnEntityHeader(sample_edited_url);
    ensureThatUrlIsNotAvaliableOnSidebar(sample_edited_url);

    updateLink(
      sample_edited_url,
      sample_edited_url,
      sample_edited_url,
      sample_edited_url,
      false,
    );

    ensureThatUrlIsAvaliableOnDocumentationTab(sample_edited_url);
    ensureThatUrlIsAvaliableOnSidebar(sample_edited_url);

    removeLinksByUrl(sample_edited_url);
  });

  it("should successfully remove the link", () => {
    const sample_url = getSampleUrl("remove_link");

    addLink(sample_url, sample_url, true);

    ensureThatUrlIsAvaliableOnDocumentationTab(sample_url);
    ensureThatUrlIsAvaliableOnEntityHeader(sample_url);

    removeLinksByUrl(sample_url);

    ensureThatUrlIsNotAvaliableOnDocumentationTab(sample_url, sample_url);
    ensureThatUrlIsNotAvaliableOnEntityHeader(sample_url);
  });
});
