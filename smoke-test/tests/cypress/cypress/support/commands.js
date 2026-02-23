// ***********************************************
// This example commands.js shows you how to
// create various custom commands and overwrite
// existing commands.
//
// For more comprehensive examples of custom
// commands please read more here:
// https://on.cypress.io/custom-commands
// ***********************************************
//
//
// -- This is a parent command --

import dayjs from "dayjs";
import { hasOperationName, aliasGraphQLOperation } from "../e2e/utils";

function selectorWithtestId(id) {
  return `[data-testid="${id}"]`;
}

export function getTimestampMillisNumDaysAgo(numDays) {
  return dayjs().subtract(numDays, "day").valueOf();
}

const SKIP_ONBOARDING_TOUR_KEY = "skipOnboardingTour";
const SKIP_WELCOME_MODAL_KEY = "skipWelcomeModal";

function notFirstTimeVisit() {
  cy.window().then((win) => {
    win.localStorage.setItem(SKIP_ONBOARDING_TOUR_KEY, "true");
    win.localStorage.setItem(SKIP_WELCOME_MODAL_KEY, "true");
  });
}

Cypress.Commands.add("login", () => {
  cy.request({
    method: "POST",
    url: "/logIn",
    body: {
      username: Cypress.env("ADMIN_USERNAME"),
      password: Cypress.env("ADMIN_PASSWORD"),
    },
    retryOnStatusCodeFailure: true,
  }).then(() => notFirstTimeVisit());
});

Cypress.Commands.add("loginWithCredentials", (username, password) => {
  cy.visit("/login");
  cy.get("input[data-testid=username]").type(
    username || Cypress.env("ADMIN_USERNAME"),
    { delay: 0 },
  );
  cy.get("input[data-testid=password]").type(
    password || Cypress.env("ADMIN_PASSWORD"),
    { delay: 0 },
  );
  cy.get("[data-testid='sign-in']").click();
  cy.get(".ant-avatar-circle").should("be.visible");
  notFirstTimeVisit();
});

Cypress.Commands.add("visitWithLogin", (url) => {
  cy.visit(url);
  cy.get("input[data-testid=username]").type(Cypress.env("ADMIN_USERNAME"));
  cy.get("input[data-testid=password]").type(Cypress.env("ADMIN_PASSWORD"));
  notFirstTimeVisit();
  cy.get("[data-testid='sign-in']").click();
});

// Login commands for onboarding tour testing (without setting skipOnboardingTour)
Cypress.Commands.add("loginForOnboarding", () => {
  cy.request({
    method: "POST",
    url: "/logIn",
    body: {
      username: Cypress.env("ADMIN_USERNAME"),
      password: Cypress.env("ADMIN_PASSWORD"),
    },
    retryOnStatusCodeFailure: true,
  });
});

Cypress.Commands.add("deleteUrn", (urn) => {
  cy.request({
    method: "POST",
    url: "http://localhost:8080/entities?action=delete",
    body: {
      urn,
    },
    headers: {
      "X-RestLi-Protocol-Version": "2.0.0",
      "Content-Type": "application/json",
    },
  });
});

Cypress.Commands.add("logout", () => {
  cy.get(selectorWithtestId("manage-account-menu")).click();
  cy.get(selectorWithtestId("log-out-menu-item")).click({ force: true });
  cy.waitTextVisible("Username");
  cy.waitTextVisible("Password");
});

Cypress.Commands.add("logoutV2", () => {
  cy.get(selectorWithtestId("nav-sidebar-sign-out")).click({ force: true });
  cy.waitTextVisible("Username");
  cy.waitTextVisible("Password");
});

Cypress.Commands.add("goToGlossaryList", () => {
  cy.visit("/glossary");
  cy.waitTextVisible("Glossary");
});

Cypress.Commands.add("goToBusinessAttributeList", () => {
  cy.visit("/business-attribute");
  cy.waitTextVisible("Business Attribute");
  cy.wait(3000);
});

Cypress.Commands.add("goToDomainList", () => {
  cy.visit("/domains");
  cy.waitTextVisible("Domains");
});

Cypress.Commands.add("goToViewsSettings", () => {
  cy.visit("/settings/views");
  cy.waitTextVisible("Views");
});

Cypress.Commands.add("goToOwnershipTypesSettings", () => {
  cy.visit("/settings/ownership");
  cy.waitTextVisible("Manage Ownership");
});

Cypress.Commands.add("goToHomePagePostSettings", () => {
  cy.visit("/settings/posts");
  cy.waitTextVisible("Home Page Posts");
});

Cypress.Commands.add("goToHomePagePostSettingsV1", () => {
  cy.visit("/settings/posts");
  cy.waitTestIdVisible("managePostsV1");
});

Cypress.Commands.add("goToHomePagePostSettingsV2", () => {
  cy.visit("/settings/posts");
  cy.waitTestIdVisible("managePostsV2");
});

Cypress.Commands.add("goToAccessTokenSettings", () => {
  cy.visit("/settings/tokens");
  cy.waitTextVisible("Manage Access Tokens");
  cy.wait(3000);
});

Cypress.Commands.add("goToIngestionPage", () => {
  cy.visit("/ingestion");
  cy.waitTextVisible("Sources");
});

Cypress.Commands.add("goToDataset", (urn, dataset_name, login) => {
  if (login) {
    cy.visitWithLogin(`/dataset/${urn}/`);
  } else {
    cy.visit(`/dataset/${urn}/`);
  }
  cy.wait(3000);
  cy.waitTextVisible(dataset_name);
});

Cypress.Commands.add("goToBusinessAttribute", (urn, attribute_name) => {
  cy.visit(`/business-attribute/${urn}`);
  cy.wait(5000);
  cy.waitTextVisible(attribute_name);
});

Cypress.Commands.add("goToTag", (urn, tag_name) => {
  cy.visit(`/tag/${urn}`);
  cy.wait(5000);
  cy.waitTextVisible(tag_name);
});

Cypress.Commands.add("goToEntityLineageGraph", (entity_type, urn) => {
  cy.visit(`/${entity_type}/${urn}?is_lineage_mode=true`);
});

Cypress.Commands.add(
  "goToEntityLineageGraph",
  (entity_type, urn, start_time_millis, end_time_millis) => {
    cy.visit(
      `/${entity_type}/${urn}?is_lineage_mode=true&start_time_millis=${start_time_millis}&end_time_millis=${end_time_millis}`,
    );
  },
);

Cypress.Commands.add("goToEntityLineageGraphV2", (entity_type, urn) => {
  cy.visit(`/${entity_type}/${urn}/Lineage`);
});

Cypress.Commands.add(
  "goToEntityLineageGraphV2",
  (entity_type, urn, start_time_millis, end_time_millis) => {
    cy.visit(
      `/${entity_type}/${urn}/Lineage?start_time_millis=${start_time_millis}&end_time_millis=${end_time_millis}`,
    );
  },
);

Cypress.Commands.add("lineageTabClickOnUpstream", () => {
  cy.get(
    '[data-testid="lineage-tab-direction-select-option-downstream"] > b',
  ).click();
  cy.get(
    '[data-testid="lineage-tab-direction-select-option-upstream"] > b',
  ).click();
});

Cypress.Commands.add("goToChart", (urn) => {
  cy.visit(`/chart/${urn}`);
});

Cypress.Commands.add("goToContainer", (urn) => {
  cy.visit(`/container/${urn}`);
});

Cypress.Commands.add("goToDomain", (urn) => {
  cy.visit(`/domain/${urn}`);
});

Cypress.Commands.add("goToGlossaryNode", (urn) => {
  cy.visit(`/glossaryNode/${urn}`);
});

Cypress.Commands.add("goToGlossaryTerm", (urn) => {
  cy.visit(`/glossaryTerm/${urn}`);
});

Cypress.Commands.add("goToApplication", (urn) => {
  cy.visit(`/application/${urn}`);
});

Cypress.Commands.add("goToDataProduct", (urn) => {
  cy.visit(`/dataProduct/${urn}`);
});

Cypress.Commands.add("goToAnalytics", () => {
  cy.visit("/analytics");
  cy.contains("Data Landscape Summary", { timeout: 10000 });
});

Cypress.Commands.add("goToUserList", () => {
  cy.visit("/settings/identities/users");
  cy.waitTextVisible("Manage Users & Groups");
});

Cypress.Commands.add("goToStarSearchList", () => {
  cy.visit("/search?query=%2A");
  cy.waitTextVisible("Showing");
  cy.waitTextVisible("results");
});

Cypress.Commands.add("openThreeDotDropdown", () => {
  cy.clickOptionWithTestId("entity-header-dropdown");
});

Cypress.Commands.add("openThreeDotMenu", () => {
  cy.clickOptionWithTestId("three-dot-menu");
});

Cypress.Commands.add("clickOptionWithText", (text) => {
  cy.contains(text).should("be.visible").click();
});

Cypress.Commands.add("clickFirstOptionWithText", (text) => {
  cy.contains(text).first().click();
});

Cypress.Commands.add("clickOptionWithTextToScrollintoView", (text) => {
  cy.contains(text).scrollIntoView().click();
});

Cypress.Commands.add("clickOptionInScrollView", (text, selector) => {
  cy.get(selector).within(() => {
    cy.contains(text).then((el) => {
      // Scroll the element into view with options for better alignment
      el[0].scrollIntoView({ block: "center", inline: "nearest" });

      // Wrap the element for further chaining with Cypress commands
      cy.wrap(el)
        .should("be.visible") // Wait until the element is visible
        .should("not.be.disabled") // Ensure the element is not disabled
        .click({ force: true }); // Force click if necessary
    });
  });
});

Cypress.Commands.add("deleteFromDropdown", () => {
  cy.openThreeDotDropdown();
  cy.waitTextVisible("Delete");
  // Wait for button is enabled
  cy.getWithTestId("entity-menu-delete-button")
    .closest("li")
    .should("have.attr", "aria-disabled", "false");
  cy.clickOptionWithText("Delete");
  cy.waitTextVisible("Yes");
  cy.clickOptionWithText("Yes");
});

Cypress.Commands.add("addViaFormModal", (text, modelHeader) => {
  cy.waitTextVisible(modelHeader);
  cy.get(".ProseMirror-focused").type(text);
  cy.get(".ant-modal-footer > button:nth-child(2)").click();
});

Cypress.Commands.add("addViaModal", (text, modelHeader, value, dataTestId) => {
  cy.waitTextVisible(modelHeader);
  cy.get(".ant-modal-content .ant-input-affix-wrapper > input[type='text']")
    .first()
    .type(text);
  cy.get(`[data-testid="${dataTestId}"]`).click();
  cy.contains(value).should("be.visible");
});

Cypress.Commands.add(
  "addBusinessAttributeViaModal",
  (text, modelHeader, value, dataTestId) => {
    cy.waitTextVisible(modelHeader);
    cy.get(".ant-input-affix-wrapper > input[type='text']").first().type(text);
    cy.get(`[data-testid="${dataTestId}"]`).click();
    cy.wait(3000);
    cy.contains(value).should("be.visible");
  },
);

Cypress.Commands.add("ensureTextNotPresent", (text) => {
  cy.contains(text).should("not.exist");
});

Cypress.Commands.add("ensureElementPresent", (element) => {
  cy.get(element).should("be.visible");
});

Cypress.Commands.add("ensureElementWithTestIdPresent", (testId) => {
  cy.get(selectorWithtestId(testId)).should("be.visible");
});

Cypress.Commands.add("waitTextPresent", (text) => {
  cy.contains(text).should("exist");
  cy.contains(text).should("have.length.above", 0);
  return cy.contains(text);
});

Cypress.Commands.add("waitTextVisible", (text) => {
  cy.contains(text).should("exist");
  cy.contains(text).should("be.visible");
  cy.contains(text).should("have.length.above", 0);
  return cy.contains(text);
});

Cypress.Commands.add("waitTestIdVisible", (testId) => {
  cy.get(`[data-testid="${testId}"]`).should("exist");
});

Cypress.Commands.add("openMultiSelect", (data_id) => {
  const selector = `${selectorWithtestId(data_id)}`;
  cy.get(
    `.ant-select${selector} > .ant-select-selector > .ant-select-selection-search`,
  ).click();
});

Cypress.Commands.add("multiSelect", (within_data_id, text) => {
  cy.openMultiSelect(within_data_id);
  cy.waitTextVisible(text);
  cy.clickOptionWithText(text);
});

Cypress.Commands.add("getWithTestId", (id) => cy.get(selectorWithtestId(id)));

Cypress.Commands.add("clickOptionWithId", (id) => {
  cy.get(id).click();
});

Cypress.Commands.add("enterTextInSpecificTestId", (id, value, text) => {
  cy.get(selectorWithtestId(id)).eq(value).type(text);
});
Cypress.Commands.add("enterTextInTestId", (id, text) => {
  cy.get(selectorWithtestId(id)).type(text);
});
Cypress.Commands.add("clearTextInTestId", (id) => {
  cy.get(selectorWithtestId(id)).clear();
});

Cypress.Commands.add("clearTextInTestId", (id, text) => {
  cy.get(selectorWithtestId(id)).clear();
});

Cypress.Commands.add("clickOptionWithTestId", (id) => {
  cy.get(selectorWithtestId(id)).first().click({
    force: true,
  });
});

Cypress.Commands.add("clickFirstOptionWithTestId", (id) => {
  cy.get(selectorWithtestId(id)).first().click({
    force: true,
  });
});

Cypress.Commands.add("clickFirstOptionWithSpecificTestId", (id, value) => {
  cy.get(selectorWithtestId(id)).eq(value).click({
    force: true,
  });
});

Cypress.Commands.add("clickOptionWithSpecificClass", (locator, value) => {
  cy.get(locator).should("be.visible");
  cy.get(locator).eq(value).click();
});

Cypress.Commands.add("clickTextOptionWithClass", (locator, text) => {
  cy.get(locator).should("be.visible").contains(text).click({ force: true });
});

Cypress.Commands.add("hideOnboardingTour", () => {
  cy.get("body").type("{ctrl} {meta} h");
});

Cypress.Commands.add("clearView", (viewName) => {
  cy.clickOptionWithTestId("view-select");
  cy.clickOptionWithTestId("view-select-clear");
  cy.get("input[data-testid='search-input']").click();
  cy.get("[data-testid='view-select']").should("not.include.text", viewName);
});

Cypress.Commands.add("addTermToDataset", (urn, dataset_name, term) => {
  cy.goToDataset(urn, dataset_name);
  cy.clickOptionWithText("Add Term");
  cy.selectOptionInTagTermModal(term);
  cy.contains(term);
});

Cypress.Commands.add(
  "addTermToBusinessAttribute",
  (urn, attribute_name, term) => {
    cy.goToBusinessAttribute(urn, attribute_name);
    cy.clickOptionWithText("Add Terms");
    cy.selectOptionInTagTermModal(term);
    cy.contains(term);
  },
);

Cypress.Commands.add(
  "addAttributeToDataset",
  (urn, dataset_name, businessAttribute) => {
    cy.goToDataset(urn, dataset_name);
    cy.clickOptionWithText("event_name");
    cy.contains("Business Attribute");
    cy.get('[data-testid="schema-field-event_name-businessAttribute"]').within(
      () => cy.contains("Add Attribute").click(),
    );
    cy.selectOptionInAttributeModal(businessAttribute);
    cy.contains(businessAttribute);
  },
);

Cypress.Commands.add("selectOptionInTagTermModal", (text) => {
  cy.enterTextInTestId("tag-term-modal-input", text);
  cy.clickOptionWithTestId("tag-term-option");
  const btn_id = "add-tag-term-from-modal-btn";
  cy.clickOptionWithTestId(btn_id);
  cy.get(selectorWithtestId(btn_id)).should("not.exist");
});

Cypress.Commands.add("selectOptionInAttributeModal", (text) => {
  cy.enterTextInTestId("business-attribute-modal-input", text);
  cy.clickOptionWithTestId("business-attribute-option");
  const btn_id = "add-attribute-from-modal-btn";
  cy.clickOptionWithTestId(btn_id);
  cy.get(selectorWithtestId(btn_id)).should("not.exist");
});

Cypress.Commands.add(
  "removeDomainFromDataset",
  (urn, dataset_name, domain_urn) => {
    cy.goToDataset(urn, dataset_name);
    cy.get(
      `.sidebar-domain-section [href="/domain/${domain_urn}"] .anticon-close`,
    ).click();
    cy.clickOptionWithText("Yes");
  },
);

Cypress.Commands.add(
  "removeApplicationFromDataset",
  (urn, dataset_name, application_urn) => {
    cy.goToDataset(urn, dataset_name);
    cy.get(`.sidebar-application-section .anticon-close`).click();
    cy.clickOptionWithText("Yes");
  },
);

Cypress.Commands.add("openEntityTab", (tab) => {
  const selector = `div[id$="${tab}"]:nth-child(1)`;
  cy.get(selector).click();
});

Cypress.Commands.add("mouseover", (selector) =>
  cy.get(selector).trigger("mouseover", { force: true }),
);

Cypress.Commands.add("mouseHoverOnFirstElement", (selector) =>
  cy.get(selector).first().trigger("mouseover", { force: true }),
);

Cypress.Commands.add("createUser", (name, password, email) => {
  cy.visit("/settings/identities/users");
  cy.clickOptionWithText("Invite Users");
  cy.waitTextVisible(/signup\?invite_token=\w{32}/).then(($elem) => {
    const inviteLink = $elem.text();
    cy.visit("/settings/identities/users");
    cy.logout();
    cy.visit(inviteLink);
    cy.enterTextInTestId("email", email);
    cy.enterTextInTestId("name", name);
    cy.enterTextInTestId("password", password);
    cy.enterTextInTestId("confirmPassword", password);
    cy.get('[data-testid="sign-up"]').click();
    cy.waitTextVisible("Welcome back");
    cy.hideOnboardingTour();
    cy.waitTextVisible(name);
    cy.logout();
    cy.loginWithCredentials();
  });
});

Cypress.Commands.add("createGroup", (name, description, group_id) => {
  cy.visit("/settings/identities/groups");
  cy.clickOptionWithText("Create group");
  cy.waitTextVisible("Create new group");
  cy.get("#name").type(name);
  cy.get("#description").type(description);
  cy.contains("Advanced").click();
  cy.waitTextVisible("Group Id");
  cy.get("#groupId").type(group_id);
  cy.get("#createGroupButton").click();
  cy.waitTextVisible("Created group!");
  cy.waitTextVisible(name);
});

Cypress.Commands.add("addGroupMember", (group_name, group_urn, member_name) => {
  cy.visit(group_urn);
  cy.clickOptionWithText(group_name);
  cy.contains(group_name).should("be.visible");
  cy.get('[role="tab"]').contains("Members").click();
  cy.clickOptionWithText("Add Member");
  cy.contains("Search for users...").click({ force: true });
  cy.focused().type(member_name);
  cy.contains(member_name).click();
  cy.focused().blur();
  cy.contains(member_name).should("have.length", 1);
  cy.get('[role="dialog"] button').contains("Add").click({ force: true });
  cy.waitTextVisible("Group members added!");
  cy.contains(member_name, { timeout: 10000 }).should("be.visible");
});

Cypress.Commands.add("createGlossaryTermGroup", (term_group_name) => {
  cy.goToGlossaryList();
  cy.clickOptionWithText("Add Term Group");
  cy.waitTextVisible("Create Term Group");
  cy.enterTextInTestId("create-glossary-entity-modal-name", term_group_name);
  cy.clickOptionWithTestId("glossary-entity-modal-create-button");
  cy.get('[data-testid="glossary-browser-sidebar"]')
    .contains(term_group_name)
    .should("be.visible");
  cy.waitTextVisible(`Created Term Group!`);
});

const SKIP_INTRODUCE_PAGE_KEY = "skipAcrylIntroducePage";

Cypress.Commands.add("skipIntroducePage", () => {
  localStorage.setItem(SKIP_INTRODUCE_PAGE_KEY, "true");
});

Cypress.Commands.add("goToStructuredProperties", () => {
  cy.visit("/structured-properties");
  cy.waitTextVisible("Structured Properties");
});

Cypress.Commands.add("createStructuredProperty", (prop) => {
  cy.get('[data-testid="structured-props-create-button"').click();
  cy.get('[data-testid="structured-props-input-name"]').click().type(prop.name);
  cy.get('[data-testid="structured-props-select-input-type"]').click();
  cy.get('[data-testid="structured-props-property-type-options-list"]')
    .contains("Text")
    .click();
  cy.get('[data-testid="structured-props-select-input-applies-to"]').click();
  cy.get('[data-testid="applies-to-options-list"]')
    .contains(prop.entity)
    .click();
  cy.get('[data-testid="structured-props-select-input-applies-to"]').click();
  cy.get('[data-testid="structured-props-create-update-button"]').click();
});

Cypress.Commands.add("deleteStructuredProperty", (prop) => {
  cy.contains("td", prop.name)
    .siblings("td")
    .find('[data-testid="structured-props-more-options-icon"]')
    .click();
  cy.get("body .ant-dropdown-menu").contains("Delete").click();
  cy.get('[data-testid="modal-confirm-button"').click();
});

Cypress.Commands.add("setIsThemeV2Enabled", (isEnabled) => {
  // set the theme V2 enabled flag on/off to show the V2 UI or not
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.alias = "gqlappConfigQuery";

      req.on("response", (res) => {
        res.body.data.appConfig.featureFlags.themeV2Enabled = isEnabled;
        res.body.data.appConfig.featureFlags.themeV2Default = isEnabled;
        res.body.data.appConfig.featureFlags.showNavBarRedesign = isEnabled;
      });
    } else if (hasOperationName(req, "getMe")) {
      req.alias = "gqlgetMeQuery";
      req.on("response", (res) => {
        res.body.data.me.corpUser.settings.appearance.showThemeV2 = isEnabled;
      });
    }
  });
});

Cypress.Commands.add(
  "setFeatureFlags",
  (itThemeV2Enabled, updateFeatureFlags) => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      if (hasOperationName(req, "appConfig")) {
        req.alias = "gqlappConfigQuery";

        req.on("response", (res) => {
          res.body.data.appConfig.featureFlags.themeV2Enabled =
            itThemeV2Enabled;
          res.body.data.appConfig.featureFlags.themeV2Default =
            itThemeV2Enabled;
          res.body.data.appConfig.featureFlags.showNavBarRedesign =
            itThemeV2Enabled;
          updateFeatureFlags(res);
        });
      } else if (hasOperationName(req, "getMe")) {
        req.alias = "gqlgetMeQuery";
        req.on("response", (res) => {
          res.body.data.me.corpUser.settings.appearance.showThemeV2 =
            itThemeV2Enabled;
        });
      }
    });
  },
);

Cypress.Commands.add("interceptGraphQLOperation", (operationName) => {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    aliasGraphQLOperation(req, operationName);
  });
});

Cypress.Commands.add(
  "waitForGraphQLOperation",
  (operationName, options = {}) => {
    cy.wait(`@gql${operationName}`, options);
  },
);

Cypress.on("uncaught:exception", (err) => {
  const resizeObserverLoopLimitErrMessage =
    "ResizeObserver loop limit exceeded";
  const resizeObserverLoopErrMessage =
    "ResizeObserver loop completed with undelivered notifications.";

  /* returning false here prevents Cypress from failing the test */
  if (
    err.message.includes(resizeObserverLoopLimitErrMessage) ||
    err.message.includes(resizeObserverLoopErrMessage)
  ) {
    return false;
  }

  // Allow other uncaught exceptions to fail the test
  return true;
});

//
//
// -- This is a child command --
// Cypress.Commands.add('drag', { prevSubject: 'element'}, (subject, options) => { ... })
//
//
// -- This is a dual command --
// Cypress.Commands.add('dismiss', { prevSubject: 'optional'}, (subject, options) => { ... })
//
//
// -- This will overwrite an existing command --
// Cypress.Commands.overwrite('visit', (originalFn, url, options) => { ... })
