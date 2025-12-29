import {
  clickFirstAddModuleButton,
  removeFirstModuleWithTestId,
} from "../homeV3/utils";
import { hasOperationName } from "../utils";

// Common

export function setThemeV2AndSummaryTabFlags(isOn) {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.reply((res) => {
        res.body.data.appConfig.featureFlags.themeV2Enabled = isOn;
        res.body.data.appConfig.featureFlags.themeV2Default = isOn;
        res.body.data.appConfig.featureFlags.showNavBarRedesign = isOn;
        res.body.data.appConfig.featureFlags.assetSummaryPageV1 = isOn;
      });
    }
  });
}

export function deleteOpenedEntity() {
  cy.get('[id="entity-profile-sidebar"]')
    .invoke("attr", "aria-expanded")
    .then((expanded) => {
      if (expanded === "true") {
        cy.getWithTestId("view-more-button").click();
      }
    });
  cy.getWithTestId("entity-menu-delete-button").should("be.visible").click();
  cy.clickOptionWithText("Yes");
}

export function openEntitySidebar() {
  cy.get('[id="entity-profile-sidebar"]')
    .invoke("attr", "aria-expanded")
    .then((expanded) => {
      if (expanded !== "true") {
        cy.get('[id="entity-sidebar-tabs-tab-collapse"]').click();
      }
    });
}

export function openSummaryTabOnSidebar() {
  openEntitySidebar();
  cy.get('[id="entity-sidebar-tabs-tab-Summary"]')
    .invoke("attr", "aria-selected")
    .then((selected) => {
      if (selected !== "true") {
        cy.get('[id="entity-sidebar-tabs-tab-Summary"]').click();
      }
    });
}

export function setDomainToOpenedEntity(domainName) {
  openSummaryTabOnSidebar();

  cy.get('[id="entity-profile-domains"]').within(() => {
    cy.clickOptionWithTestId("set-domain-button");
  });

  cy.getWithTestId("set-domain-select").type(domainName);
  cy.get(".ant-select-item-option-content").contains(domainName).click();
  cy.clickOptionWithTestId("submit-button");
}

export function setGlossaryTermToOpenedEntity(termName) {
  openSummaryTabOnSidebar();

  cy.get('[id="entity-profile-glossary-terms"]').within(() => {
    cy.clickOptionWithTestId("add-terms-button");
  });

  cy.getWithTestId("tag-term-modal-input").filter(":visible").type(termName);
  cy.get(".ant-select-item-option-content").contains(termName).click();
  cy.clickOptionWithTestId("add-tag-term-from-modal-btn");
}

export function setTagToOpenedEntity(tagName) {
  openSummaryTabOnSidebar();

  cy.get('[id="entity-profile-tags"]').within(() => {
    cy.clickOptionWithTestId("add-tags-button");
  });

  cy.getWithTestId("tag-term-modal-input").filter(":visible").type(tagName);
  cy.get(".ant-select-item-option-content").contains(tagName).click();
  cy.clickOptionWithTestId("add-tag-term-from-modal-btn");
}

export function addAsset(entityType, assetName, assetUrn) {
  let addAssetButtonTestId;
  let assetAddedText;
  if (entityType === "DOMAIN") {
    addAssetButtonTestId = "domain-batch-add";
    assetAddedText = "Added assets to Domain!";
  } else if (entityType === "GLOSSARY_TERM") {
    addAssetButtonTestId = "glossary-batch-add";
    assetAddedText = "Added Glossary Term to entities!";
  } else if (entityType === "DATA_PRODUCT") {
    addAssetButtonTestId = "data-product-batch-add";
    assetAddedText = "Added assets to Data Product!";
  }

  if (addAssetButtonTestId) {
    cy.clickOptionWithTestId(addAssetButtonTestId);
    cy.getWithTestId("search-select-modal").within(() => {
      cy.getWithTestId("search-input").type(assetName);
      cy.clickOptionWithTestId(`checkbox-${assetUrn}`);
      cy.get('[id="continueButton"]').click();
    });
    if (assetAddedText) {
      cy.waitTextVisible(assetAddedText);
    }
  }
}

// Domain

export function createDomainFromLeftSidebar(domainName, parentDomainName) {
  cy.clickOptionWithTestId("sidebar-create-domain-button");
  cy.waitTextVisible("Create New Domain");
  cy.getWithTestId("create-domain-name").click().type(domainName);

  if (parentDomainName) {
    cy.getWithTestId("parent-domain-select").type(parentDomainName);
    cy.get(".ant-select-dropdown").within(() => {
      cy.clickOptionWithText(parentDomainName);
    });
  }

  cy.clickOptionWithTestId("create-domain-button");
  cy.waitTextVisible("Created domain!");
}

export function createDomain(name) {
  cy.visit("/domains");
  createDomainFromLeftSidebar(name);
}

export function createChildDomain(domainName, parentDomainName) {
  createDomainFromLeftSidebar(domainName, parentDomainName);
}

export function openDomain(name) {
  cy.visit("/domains");
  cy.get('[id="browse-v2"]').within(() => {
    cy.getWithTestId("search-bar-input").type(name);
    cy.getWithTestId("search-results").within(() => {
      cy.clickOptionWithText(name);
    });
  });
}

export function openDomainByUrn(urn) {
  cy.visit(`/domain/${urn}`);
}

export function deleteOpenedDomain() {
  deleteOpenedEntity();
  cy.waitTextVisible("Deleted Domain!");
}

export function deleteDomain(domainName) {
  openDomain(domainName);
  deleteOpenedDomain(domainName);
}

// Glossary node

export function openGlossaryTermOrNode(glossaryTermOrNodeName) {
  cy.visit("/glossary");
  cy.getWithTestId("glossary-browser-sidebar").within(() => {
    cy.getWithTestId("search-bar-input").type(glossaryTermOrNodeName);
  });
  cy.clickOptionWithText(glossaryTermOrNodeName);
}

export function deleteOpenedGlossaryNode() {
  deleteOpenedEntity();
  cy.waitTextVisible("Deleted Term Group!");
}

export function createGlossaryNode(name) {
  cy.visit("/glossary");
  cy.clickOptionWithTestId("create-glossary-button");
  cy.waitTextVisible("Create Glossary");
  cy.enterTextInTestId("create-glossary-entity-modal-name", name);
  cy.clickOptionWithTestId("glossary-entity-modal-create-button");
  cy.waitTextVisible("Created Term Group!");
}

export function openGlossaryNode(name) {
  openGlossaryTermOrNode(name);
}

// Glossary term

export function createGlossaryTerm(nodeName, termName) {
  openGlossaryNode(nodeName);
  cy.clickOptionWithTestId("Contents-entity-tab-header");
  cy.clickOptionWithTestId("add-term-button");
  cy.waitTextVisible("Create Glossary Term");
  cy.enterTextInTestId("create-glossary-entity-modal-name", termName);
  cy.clickOptionWithTestId("glossary-entity-modal-create-button");
  cy.waitTextVisible(termName);
}

export function openGlossaryTerm(termName) {
  openGlossaryTermOrNode(termName);
}

export function deleteOpenedGLossaryTerm() {
  deleteOpenedEntity();
  cy.waitTextVisible("Deleted Glossary Term!");
}

export function addRelatedTerm(termName) {
  cy.clickOptionWithTestId("Related Terms-entity-tab-header").click();
  cy.clickOptionWithTestId("add-related-term-button");
  cy.getWithTestId("add-related-terms-modal").within(() => {
    cy.get(".ant-select-selection-search-input").type(termName);
  });
  cy.get(".ant-select-item-option-content").contains(termName).click();
  cy.get("body").type("{esc}"); // close the dropdown
  cy.clickOptionWithTestId("submit-button");
}

// Data product

export function createDataProductForOpenedDomain(name) {
  cy.clickOptionWithTestId("Data Products-entity-tab-header");
  cy.clickOptionWithTestId("create-data-product-button");
  cy.getWithTestId("create-data-product-modal").within(() => {
    cy.getWithTestId("name-input").type(name);
    cy.clickOptionWithTestId("submit-button");
  });
}

export function createDataProduct(domainName, name) {
  openDomain(domainName);
  createDataProductForOpenedDomain(name);
}

export function openDataProductOnOpenedDomain(name) {
  cy.clickOptionWithTestId("Data Products-entity-tab-header");
  cy.getWithTestId("entity-title")
    .filter(`:contains("${name}")`)
    .click({ force: true });
}
export function openDataProduct(domainName, name) {
  openDomain(domainName);
  openDataProductOnOpenedDomain(name);
}

export function deleteOpenedDataProduct() {
  deleteOpenedEntity();
  cy.waitTextVisible("Deleted Data Product!");
}

export function reloadPageWithMemoryManagement() {
  cy.window().then((win) => {
    win.location.reload();
  });
  cy.wait(2000);
}

// Summary tab

export function ensureSummaryTabIsAvailable() {
  cy.getWithTestId("Summary-entity-tab-header").should("be.visible");
}

export function goToSummaryTab() {
  cy.clickOptionWithTestId("Summary-entity-tab-header");
}

// Properties section

export function ensurePropertiesSectionIsVisible() {
  cy.getWithTestId("properties-section").should("be.visible");
}

export function ensurePropertiesAreVisible(propertyTypes) {
  propertyTypes.forEach((propertyType) => {
    cy.getWithTestId(`property-${propertyType}`).should("be.visible");
  });
}

export function ensurePropertyExist(property) {
  cy.getWithTestId(`property-${property.type}`).within(() => {
    cy.getWithTestId("property-title").should("contain", property.name);
    if (property.value !== undefined) {
      cy.getWithTestId("property-value").should(($el) => {
        const text = $el.text();
        expect(text).to.match(new RegExp(property.value, "i"));
      });
    }
    if (property.dataTestId !== undefined) {
      cy.getWithTestId(property.dataTestId).should("exist");
    }
  });
}

export function ensurePropertiesDoNotExist(propertyTypes) {
  propertyTypes.forEach((propertyType) => {
    cy.getWithTestId(`property-${propertyType}`).should("not.exist");
  });
}

export function removeProperty(propertyType) {
  cy.getWithTestId(`property-${propertyType}`).within(() => {
    cy.clickOptionWithTestId("property-title");
  });
  cy.getWithTestId("menu-item-remove").should("be.visible");
  cy.getWithTestId("menu-item-remove").filter(":visible").click();
}

export function ensurePropertiesAvailableToAdd(propertyTypes) {
  cy.clickOptionWithTestId("add-property-button");
  propertyTypes.forEach((propertyType) => {
    cy.getWithTestId(`menu-item-${propertyType}`).should("be.visible");
  });
  // close the menu
  cy.get("body").type("{esc}");
}

export function addProperty(propertyType) {
  cy.clickOptionWithTestId("add-property-button");
  cy.clickOptionWithTestId(`menu-item-${propertyType}`);
}

export function replaceProperty(propertyTypeToReplace, targetPropertyType) {
  cy.getWithTestId(`property-${propertyTypeToReplace}`).within(() => {
    cy.clickOptionWithTestId("property-title");
  });

  cy.getWithTestId("menu-item-replace").filter(":visible").trigger("mouseover");
  // cy.getWithTestId(`menu-item-${targetPropertyType}`).should("be.visible");
  cy.get(".ant-dropdown-menu-submenu")
    .filter(":visible")
    .within(() => {
      cy.getWithTestId(`menu-item-${targetPropertyType}`)
        .trigger("mouseover")
        .click();
    });
}

// Structured properties

export function createStructuredProperty(name) {
  cy.visit("/structured-properties");
  cy.clickOptionWithTestId("structured-props-create-button");
  cy.getWithTestId("structured-props-input-name").within(() => {
    cy.get("input").type(name);
  });

  cy.clickOptionWithTestId("structured-props-select-input-type");
  cy.getWithTestId("structured-props-property-type-options-list").within(() => {
    cy.clickOptionWithText("Text");
  });

  cy.clickOptionWithTestId("structured-props-select-input-applies-to");
  cy.getWithTestId("applies-to-options-list").within(() => {
    cy.clickOptionWithText("All Asset Types");
  });

  cy.clickOptionWithTestId("structured-props-create-update-button");
  cy.waitTextVisible("Structured property created!");
}

export function addStructuredPropertyToOpenedEntity(
  structuredPropertyName,
  value,
) {
  cy.get('[id="entity-sidebar-tabs-tab-Properties"]').click({ force: true });

  cy.clickOptionWithTestId("add-structured-prop-button");

  cy.getWithTestId("add-structured-property-dropdown").within(() => {
    cy.getWithTestId("search-input").type(structuredPropertyName);
    cy.getWithTestId("options-container").within(() => {
      cy.clickOptionWithText(structuredPropertyName);
    });
  });

  cy.getWithTestId("structured-property-string-value-input").type(value);
  cy.clickOptionWithTestId("add-update-structured-prop-on-entity-button");
  cy.waitTextVisible("Successfully added structured property!");
}

export function addStructuredPropertyToProperties(structuredPropertyName) {
  cy.clickOptionWithTestId("add-property-button");
  cy.getWithTestId("menu-item-structuredProperties").trigger("mouseover");
  cy.getWithTestId("structured-property-search").within(() => {
    cy.getWithTestId("search-bar-input").type(structuredPropertyName);
  });
  cy.get('[data-testid^="menu-item-urn:li:structuredProperty"]')
    .contains(structuredPropertyName)
    .click({ force: true });
}

export function ensureStructuredPropertyExists(structuredPropertyName, value) {
  cy.getWithTestId("property-STRUCTURED_PROPERTY").within(() => {
    cy.getWithTestId("property-title").contains(structuredPropertyName);
    cy.getWithTestId("property-value").contains(value);
  });
}

export function deleteStructuredProperty(structuredPropertyName) {
  cy.visit("/structured-properties");
  cy.getWithTestId("search-bar-input")
    .scrollIntoView()
    .type(structuredPropertyName);
  cy.getWithTestId("structured-props-table").within(() => {
    cy.get('[data-testid^="urn:li:structuredProperty"]')
      .contains(structuredPropertyName)
      .closest("tr")
      .within((el) => {
        cy.clickOptionWithTestId("structured-props-more-options-icon");
      });
  });

  cy.clickOptionWithTestId("menu-item-3"); // delete
  cy.clickOptionWithTestId("modal-confirm-button");
}

// About section

export function ensureAboutSectionIsVisible() {
  cy.getWithTestId("about-section").should("be.visible");
}

export function updateDescription(description) {
  cy.clickOptionWithTestId("edit-description-button");
  cy.getWithTestId("description-editor").within(() => {
    cy.get(".remirror-editor").clear().type(description);
  });
  cy.clickOptionWithTestId("publish-button");
  cy.getWithTestId("publish-button").should("not.exist"); // wait for closing of the modal
  // cy.wait(300); // wait for closing of the modal
}

export function ensureDescriptionContainsText(description) {
  cy.getWithTestId("description-viewer").contains(description);
}

export function addLink(url, label) {
  // Click the "+" button to open the menu, then click "Add link" menu item
  cy.clickOptionWithTestId("add-related-button").wait(500);
  cy.contains("Add link").click().wait(500);
  cy.getWithTestId("url-input").clear().type(url);
  cy.getWithTestId("label-input").clear().type(label);
  cy.clickOptionWithTestId("link-form-modal-submit-button");
}

export function updateLink(prevUrl, prevLabel, url, label) {
  cy.getWithTestId(`${prevUrl}-${prevLabel}`).within(() => {
    cy.clickOptionWithTestId("edit-link-button");
  });

  cy.getWithTestId("url-input").clear().type(url);
  cy.getWithTestId("label-input").clear().type(label);
  cy.clickOptionWithTestId("link-form-modal-submit-button");
}

export function removeLink(url, label) {
  cy.getWithTestId(`${url}-${label}`).within(() => {
    cy.clickOptionWithTestId("remove-link-button");
  });
  cy.clickOptionWithTestId("modal-confirm-button");
}

export function ensureLinkExists(url, label) {
  cy.getWithTestId("about-section").within(() => {
    cy.getWithTestId(`${url}-${label}`).should("be.visible");
    cy.getWithTestId(`${url}-${label}`).contains(label);
  });
}

export function ensureLinkDoesNotExist(url, label) {
  cy.getWithTestId("about-section").within(() => {
    cy.getWithTestId(`${url}-${label}`).should("not.exist");
  });
}

// Template section

export function ensureTemplateSectionIsVisible() {
  cy.getWithTestId("template-wrapper").should("be.visible");
}

export function ensureModulesAreVisible(modules) {
  modules.forEach((module) => {
    cy.getWithTestId(`${module}-module`).should("be.visible");
  });
}

export function ensureModuleExist(module) {
  cy.getWithTestId(`${module.type}-module`)
    .filter(`:contains("${module.name}")`)
    .scrollIntoView({ block: "center" })
    .within(() => {
      cy.getWithTestId("large-module-drag-handle").contains(module.name);
      if (module.value !== undefined) {
        cy.getWithTestId("module-content").contains(module.value);
      }
    });
}

export function ensureModuleDoesNotExist(module) {
  cy.contains(`[data-testid="${module.type}-module"]`, module.name).should(
    "not.exist",
  );
}

export function deleteModule(module) {
  removeFirstModuleWithTestId(`${module.type}-module`);
}

export function ensureModulesAvailableToAdd(modules) {
  clickFirstAddModuleButton();
  modules.forEach((module) =>
    cy
      .contains(
        `[data-testid="add-${module.addType ?? module.type}-module"]`,
        module.name,
      )
      .should("be.visible"),
  );
  cy.get("body").type("{esc}");
}

export function addModule(module) {
  clickFirstAddModuleButton();
  cy.contains(
    `[data-testid="add-${module.addType ?? module.type}-module"]`,
    module.name,
  ).click();
}

// Tests

export function testPropertiesSection(properties) {
  const propertyTypes = properties.map((property) => property.type);
  ensurePropertiesSectionIsVisible();
  ensurePropertiesAreVisible(propertyTypes); // default properties

  properties.forEach((property) => ensurePropertyExist(property));
}

export function testAboutSection() {
  ensureAboutSectionIsVisible();
  updateDescription("description");
  ensureDescriptionContainsText("description");
  updateDescription("updated description");
  ensureDescriptionContainsText("updated description");
  addLink("https://test.com", "testLink");
  ensureLinkExists("https://test.com", "testLink");
  updateLink(
    "https://test.com",
    "testLink",
    "https://test-updated.com",
    "testLinkUpdated",
  );
  ensureLinkExists("https://test-updated.com", "testLinkUpdated");
  removeLink("https://test-updated.com", "testLinkUpdated");
  ensureLinkDoesNotExist("https://test-updated.com", "testLinkUpdated");
}

export function testTemplateSection(defaultModules) {
  ensureTemplateSectionIsVisible();

  // Check default modules
  defaultModules.forEach((module) => ensureModuleExist(module));
}
