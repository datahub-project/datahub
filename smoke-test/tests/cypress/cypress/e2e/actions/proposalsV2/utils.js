import { hasOperationName } from "../../utils";
import { proposalsList } from "./helpers/ProposalsList";

const REJECT_PROPOSAL_MESSAGE = "Proposal declined";
const ACCEPT_PROPOSAL_MESSAGE = "Successfully accepted";

export const setThemeV2AndTaskCenterRedesignFlags = (isOn) => {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.reply((res) => {
        res.body.data.appConfig.featureFlags.showTaskCenterRedesign = isOn;
        res.body.data.appConfig.featureFlags.themeV2Enabled = isOn;
        res.body.data.appConfig.featureFlags.themeV2Default = isOn;
        res.body.data.appConfig.featureFlags.showNavBarRedesign = isOn;
      });
    }
  });
};

export const removeItem = (name, schemaField) => {
  const drawerSelector = schemaField
    ? '[data-testid="schema-field-drawer-content"]'
    : "#entity-profile-sidebar";
  cy.get(drawerSelector).contains(name).parents().eq(2).as("parentElement");

  cy.get("@parentElement").then(($parent) => {
    const iconId = '[data-testid="remove-icon"], [data-testid^="remove-owner"]';
    if ($parent.find(iconId).length > 0) {
      cy.get("@parentElement")
        .trigger("mouseover")
        .find(iconId)
        .click({ force: true });
      cy.contains("Yes").click();
      cy.get(drawerSelector).contains(name).should("not.exist");
    } else {
      cy.get("@parentElement").contains(name).click();
      cy.get('[data-testid="proposal-reject-button"').click();
    }
  });
};

export const deletePreviousEntity = (entityName, schemaField) => {
  cy.get(
    schemaField
      ? '[data-testid="schema-field-drawer-content"]'
      : "#entity-profile-sidebar",
  )
    .invoke("text")
    .then((text) => {
      const storedVariable = text.trim();
      if (storedVariable.includes(entityName)) {
        removeItem(entityName);
      } else {
        cy.ensureTextNotPresent(entityName);
      }
    });
};

export const goToEntityPage = (entityRoute, name) => {
  cy.visit(entityRoute);
  cy.contains(name, { timeout: 10000 }).should("be.visible");
};

const entitytoProposeButtonId = {
  term: "propose-tags-terms-on-entity-button",
  tag: "propose-tags-terms-on-entity-button",
  owner: "propose-owners-on-entity-button",
  domain: "propose-domain-on-entity-button",
};

export const proposeItems = (
  asset,
  itemNames,
  itemType,
  addButtonId,
  inputId,
  proposalNote,
  schemaField,
) => {
  goToEntityPage(asset.urn, asset.name);
  if (schemaField) {
    cy.clickOptionWithText(schemaField);
  }

  itemNames.forEach((itemName) => {
    deletePreviousEntity(itemName, schemaField);
    cy.get(`[data-testid="proposed-${itemType}-${itemName}"]`).should(
      "not.exist",
    );
  });

  // Click Add button
  cy.clickOptionWithTestId(addButtonId);

  itemNames.forEach((itemName) => {
    // Type the name
    cy.get(`[data-testid="${inputId}"]`).should("be.visible").type(itemName);

    // Select the entity
    cy.get(
      `[data-testid="${itemType === "tag" || itemType === "term" ? "tag-term" : itemType}-option-${itemName}"]`,
    )
      .should("exist")
      .click({ force: true });
  });
  // Click the create proposal button
  cy.get(`[data-testid=${entitytoProposeButtonId[itemType]}]`).click({
    force: true,
  });

  if (proposalNote) {
    cy.get(`[data-testid="proposal-note-input"]`)
      .should("be.visible")
      .type(proposalNote);
  }

  cy.get('[data-testid="add-proposal-note-button"]').click({ force: true });

  itemNames.forEach((itemName) => {
    // Verify the proposed item is visible
    cy.get(`[data-testid="proposed-${itemType}-${itemName}"]`).should(
      "be.visible",
    );
  });
};

export const proposeStructuredProperty = (
  asset,
  structuredProperty,
  propValue,
  schemaField,
) => {
  goToEntityPage(asset.urn, asset.name);
  if (schemaField) {
    cy.clickOptionWithText(schemaField);
  }
  cy.contains(structuredProperty.name).scrollIntoView().should("be.visible");
  cy.clickOptionWithTestId(`${structuredProperty.name}-add-or-edit-button`);
  cy.get('[data-testid="structured-property-string-value-input"]')
    .click()
    .type(propValue);
  cy.get(
    '[data-testid="propose-update-structured-prop-on-entity-button"]',
  ).click({ force: true });
  cy.get('[data-testid="add-proposal-note-button"]').click({ force: true });
  cy.get(
    `[data-testid="proposed-property-${structuredProperty.name}-value-${propValue}"]`,
  ).should("be.visible");
};

export const acceptProposalFromAssetPage = (name, itemType, schemaField) => {
  const drawerSelector = schemaField
    ? '[data-testid="schema-field-drawer-content"]'
    : "#entity-profile-sidebar";
  cy.waitTextVisible(name);
  cy.get(drawerSelector)
    .find(`[data-testid="proposed-${itemType}-${name}"]`)
    .should("be.visible")
    .click({ force: true });
  cy.get(`[data-testid="proposal-accept-button"]`).click({
    force: true,
  });
  cy.waitTextVisible(ACCEPT_PROPOSAL_MESSAGE);
  cy.get(`[data-testid="proposed-${itemType}-${name}"]`, {
    timeout: 10000,
  }).should("not.exist");
  cy.get(`[data-testid="${itemType}-${name}"]`, { timeout: 10000 }).should(
    "exist",
  );

  if (!itemType.startsWith("property")) {
    deletePreviousEntity(name, schemaField);
  }
};

export const rejectProposalFromAssetPage = (name, itemType, schemaField) => {
  const drawerSelector = schemaField
    ? '[data-testid="schema-field-drawer-content"]'
    : "#entity-profile-sidebar";
  cy.waitTextVisible(name);
  cy.get(drawerSelector)
    .find(`[data-testid="proposed-${itemType}-${name}"]`)
    .should("be.visible")
    .click({ force: true });
  cy.get(`[data-testid="proposal-reject-button"]`).click({
    force: true,
  });
  cy.waitTextVisible(REJECT_PROPOSAL_MESSAGE);
  cy.contains(name, { timeout: 10000 }).should("not.exist");
};

export const createSidebarStructuredProp = (
  structuredProperty,
  schemaField,
) => {
  cy.goToStructuredProperties();
  cy.createStructuredProperty(structuredProperty);
  cy.get('[data-testid="structured-props-table"]')
    .contains(structuredProperty.name)
    .click();

  const switchSelector = schemaField
    ? '[data-testid="structured-props-show-in-columns-table-switch"]'
    : '[data-testid="structured-props-show-in-asset-summary-switch"]';
  cy.get(switchSelector).click();
  cy.get('[data-testid="structured-props-create-update-button"]').click();
};

export const acceptProposalFromProposalsPage = (name, asset) => {
  cy.visit("/requests/proposals");
  cy.get('[data-testid="proposals-table"]')
    .find("tbody tr")
    .contains(name)
    .closest("tr")
    .find('[data-testid="approve-button"]')
    .click();

  cy.get('[data-testid="submit-proposal-button"]').click({ force: true });

  cy.waitTextVisible(ACCEPT_PROPOSAL_MESSAGE);
  proposalsList.clickFilterOption("status", "Completed");
  cy.get('[data-testid="proposal-completed-by"]').should("be.visible");

  goToEntityPage(asset.urn, asset.name);

  deletePreviousEntity(name);
};

export const rejectProposalFromProposalsPage = (name, rejectNote) => {
  cy.visit("/requests/proposals");
  cy.get('[data-testid="proposals-table"]')
    .contains("td", name)
    .parents("tr")
    .within(() => {
      cy.get('[data-testid="decline-button"]').click();
    });

  if (rejectNote) {
    cy.get('[placeholder="Why are you rejecting the proposal?"]').type(
      rejectNote,
    );
  }

  cy.get('[data-testid="submit-proposal-button"]').click({ force: true });

  cy.waitTextVisible(REJECT_PROPOSAL_MESSAGE);
};
