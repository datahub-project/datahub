import dayjs from "dayjs";
import { hasOperationName } from "./utils";
import { GraphQLRequest, GraphQLResponse } from "./types";

// Extend Cypress namespace with our commands
declare global {
  namespace Cypress {
    interface Chainable {
      /**
       * Sets the theme V2 enabled flag and related UI settings
       * @param isEnabled - Whether to enable theme V2
       */
      setIsThemeV2Enabled(isEnabled: boolean): Chainable<void>;

      /**
       * Skips the introduce page by setting a localStorage flag
       */
      skipIntroducePage(): Chainable<void>;

      /**
       * Logs in with default admin credentials from environment variables
       */
      login(): Chainable<void>;

      /**
       * Logs in with specific credentials
       * @param username - The username to log in with
       * @param password - The password to log in with
       */
      loginWithCredentials(username: string, password: string): Chainable<void>;

      /**
       * Visits a URL and logs in
       * @param url - The URL to visit
       */
      visitWithLogin(url: string): Chainable<void>;

      /**
       * Deletes an entity by URN
       * @param urn - The URN of the entity to delete
       */
      deleteUrn(urn: string): Chainable<void>;

      /**
       * Logs out of the application
       */
      logout(): Chainable<void>;

      /**
       * Logs out of the application (V2 version)
       */
      logoutV2(): Chainable<void>;

      /**
       * Waits for text to be visible on the page
       * @param text - The text to wait for
       */
      waitTextVisible(text: string): Chainable<void>;

      /**
       * Goes to the glossary list page
       */
      goToGlossaryList(): Chainable<void>;

      /**
       * Goes to the business attribute list page
       */
      goToBusinessAttributeList(): Chainable<void>;

      /**
       * Goes to the domain list page
       */
      goToDomainList(): Chainable<void>;

      /**
       * Goes to the views settings page
       */
      goToViewsSettings(): Chainable<void>;

      /**
       * Goes to the ownership types settings page
       */
      goToOwnershipTypesSettings(): Chainable<void>;

      /**
       * Goes to the home page post settings page
       */
      goToHomePagePostSettings(): Chainable<void>;

      /**
       * Goes to the home page post settings page (V1)
       */
      goToHomePagePostSettingsV1(): Chainable<void>;

      /**
       * Goes to the home page post settings page (V2)
       */
      goToHomePagePostSettingsV2(): Chainable<void>;

      /**
       * Goes to the access token settings page
       */
      goToAccessTokenSettings(): Chainable<void>;

      /**
       * Goes to the ingestion page
       */
      goToIngestionPage(): Chainable<void>;

      /**
       * Goes to a dataset page
       * @param urn - The URN of the dataset
       * @param dataset_name - The name of the dataset
       * @param login - Whether to log in first
       */
      goToDataset(
        urn: string,
        dataset_name: string,
        login?: boolean,
      ): Chainable<void>;

      /**
       * Goes to a business attribute page
       * @param urn - The URN of the business attribute
       * @param attribute_name - The name of the business attribute
       */
      goToBusinessAttribute(
        urn: string,
        attribute_name: string,
      ): Chainable<void>;

      /**
       * Goes to a tag page
       * @param urn - The URN of the tag
       * @param tag_name - The name of the tag
       */
      goToTag(urn: string, tag_name: string): Chainable<void>;

      /**
       * Goes to an entity lineage graph page
       * @param entity_type - The type of entity
       * @param urn - The URN of the entity
       */
      goToEntityLineageGraph(entity_type: string, urn: string): Chainable<void>;

      /**
       * Goes to an entity lineage graph page with time range
       * @param entity_type - The type of entity
       * @param urn - The URN of the entity
       * @param start_time_millis - Start time in milliseconds
       * @param end_time_millis - End time in milliseconds
       */
      goToEntityLineageGraph(
        entity_type: string,
        urn: string,
        start_time_millis: number,
        end_time_millis: number,
      ): Chainable<void>;

      /**
       * Goes to an entity lineage graph page (V2)
       * @param entity_type - The type of entity
       * @param urn - The URN of the entity
       */
      goToEntityLineageGraphV2(
        entity_type: string,
        urn: string,
      ): Chainable<void>;

      /**
       * Goes to an entity lineage graph page with time range (V2)
       * @param entity_type - The type of entity
       * @param urn - The URN of the entity
       * @param start_time_millis - Start time in milliseconds
       * @param end_time_millis - End time in milliseconds
       */
      goToEntityLineageGraphV2(
        entity_type: string,
        urn: string,
        start_time_millis: number,
        end_time_millis: number,
      ): Chainable<void>;

      /**
       * Clicks on the upstream tab in lineage
       */
      lineageTabClickOnUpstream(): Chainable<void>;

      /**
       * Goes to a chart page
       * @param urn - The URN of the chart
       */
      goToChart(urn: string): Chainable<void>;

      /**
       * Goes to a container page
       * @param urn - The URN of the container
       */
      goToContainer(urn: string): Chainable<void>;

      /**
       * Goes to a domain page
       * @param urn - The URN of the domain
       */
      goToDomain(urn: string): Chainable<void>;

      /**
       * Goes to the analytics page
       */
      goToAnalytics(): Chainable<void>;

      /**
       * Goes to the user list page
       */
      goToUserList(): Chainable<void>;

      /**
       * Goes to the star search list page
       */
      goToStarSearchList(): Chainable<void>;

      /**
       * Opens the three dot dropdown
       */
      openThreeDotDropdown(): Chainable<void>;

      /**
       * Opens the three dot menu
       */
      openThreeDotMenu(): Chainable<void>;

      /**
       * Clicks an option with the specified text
       * @param text - The text to click
       */
      clickOptionWithText(text: string): Chainable<void>;

      /**
       * Clicks the first option with the specified text
       * @param text - The text to click
       */
      clickFirstOptionWithText(text: string): Chainable<void>;

      /**
       * Clicks an option with text and scrolls it into view
       * @param text - The text to click
       */
      clickOptionWithTextToScrollintoView(text: string): Chainable<void>;

      /**
       * Clicks an option in a scroll view
       * @param text - The text to click
       * @param selector - The selector to find the element
       */
      clickOptionInScrollView(text: string, selector: string): Chainable<void>;

      /**
       * Deletes from dropdown
       */
      deleteFromDropdown(): Chainable<void>;

      /**
       * Adds via form modal
       * @param text - The text to add
       * @param modelHeader - The modal header
       */
      addViaFormModal(text: string, modelHeader: string): Chainable<void>;

      /**
       * Adds via modal
       * @param text - The text to add
       * @param modelHeader - The modal header
       * @param value - The value to add
       * @param dataTestId - The data-testid to use
       */
      addViaModal(
        text: string,
        modelHeader: string,
        value: string,
        dataTestId: string,
      ): Chainable<void>;

      /**
       * Adds a business attribute via modal
       * @param text - The text to add
       * @param modelHeader - The modal header
       * @param value - The value to add
       * @param dataTestId - The data-testid to use
       */
      addBusinessAttributeViaModal(
        text: string,
        modelHeader: string,
        value: string,
        dataTestId: string,
      ): Chainable<void>;

      /**
       * Ensures text is not present
       * @param text - The text to check
       */
      ensureTextNotPresent(text: string): Chainable<void>;

      /**
       * Ensures element is present
       * @param element - The element to check
       */
      ensureElementPresent(element: string): Chainable<void>;

      /**
       * Waits for text to be present
       * @param text - The text to wait for
       */
      waitTextPresent(text: string): Chainable<void>;

      /**
       * Waits for test ID to be visible
       * @param testId - The test ID to wait for
       */
      waitTestIdVisible(testId: string): Chainable<void>;

      /**
       * Opens a multi-select
       * @param data_id - The data ID to use
       */
      openMultiSelect(data_id: string): Chainable<void>;

      /**
       * Selects multiple options
       * @param within_data_id - The data ID to use
       * @param text - The text to select
       */
      multiSelect(within_data_id: string, text: string): Chainable<void>;

      /**
       * Gets an element with a test ID
       * @param id - The test ID to use
       */
      getWithTestId(id: string): Chainable<void>;

      /**
       * Clicks an option with an ID
       * @param id - The ID to click
       */
      clickOptionWithId(id: string): Chainable<void>;

      /**
       * Enters text in a specific test ID
       * @param id - The test ID to use
       * @param value - The value to enter
       * @param text - The text to enter
       */
      enterTextInSpecificTestId(
        id: string,
        value: number,
        text: string,
      ): Chainable<void>;

      /**
       * Enters text in a test ID
       * @param id - The test ID to use
       * @param text - The text to enter
       */
      enterTextInTestId(id: string, text: string): Chainable<void>;

      /**
       * Clicks an option with a test ID
       * @param id - The test ID to click
       */
      clickOptionWithTestId(id: string): Chainable<void>;

      /**
       * Clicks the first option with a test ID
       * @param id - The test ID to click
       */
      clickFirstOptionWithTestId(id: string): Chainable<void>;

      /**
       * Clicks the first option with a specific test ID
       * @param id - The test ID to click
       * @param value - The value to use
       */
      clickFirstOptionWithSpecificTestId(
        id: string,
        value: number,
      ): Chainable<void>;

      /**
       * Clicks an option with a specific class
       * @param locator - The locator to use
       * @param value - The value to use
       */
      clickOptionWithSpecificClass(
        locator: string,
        value: number,
      ): Chainable<void>;

      /**
       * Clicks a text option with a class
       * @param locator - The locator to use
       * @param text - The text to click
       */
      clickTextOptionWithClass(locator: string, text: string): Chainable<void>;

      /**
       * Hides the onboarding tour
       */
      hideOnboardingTour(): Chainable<void>;

      /**
       * Clears a view
       * @param viewName - The name of the view to clear
       */
      clearView(viewName: string): Chainable<void>;

      /**
       * Adds a term to a dataset
       * @param urn - The URN of the dataset
       * @param dataset_name - The name of the dataset
       * @param term - The term to add
       */
      addTermToDataset(
        urn: string,
        dataset_name: string,
        term: string,
      ): Chainable<void>;

      /**
       * Adds a term to a business attribute
       * @param urn - The URN of the business attribute
       * @param attribute_name - The name of the business attribute
       * @param term - The term to add
       */
      addTermToBusinessAttribute(
        urn: string,
        attribute_name: string,
        term: string,
      ): Chainable<void>;

      /**
       * Adds an attribute to a dataset
       * @param urn - The URN of the dataset
       * @param dataset_name - The name of the dataset
       * @param businessAttribute - The business attribute to add
       */
      addAttributeToDataset(
        urn: string,
        dataset_name: string,
        businessAttribute: string,
      ): Chainable<void>;

      /**
       * Selects an option in the tag term modal
       * @param text - The text to select
       */
      selectOptionInTagTermModal(text: string): Chainable<void>;

      /**
       * Selects an option in the attribute modal
       * @param text - The text to select
       */
      selectOptionInAttributeModal(text: string): Chainable<void>;

      /**
       * Removes a domain from a dataset
       * @param urn - The URN of the dataset
       * @param dataset_name - The name of the dataset
       * @param domain_urn - The URN of the domain
       */
      removeDomainFromDataset(
        urn: string,
        dataset_name: string,
        domain_urn: string,
      ): Chainable<void>;

      /**
       * Opens an entity tab
       * @param tab - The tab to open
       */
      openEntityTab(tab: string): Chainable<void>;

      /**
       * Mouse over an element
       * @param selector - The selector to use
       */
      mouseover(selector: string): Chainable<void>;

      /**
       * Mouse hover on first element
       * @param selector - The selector to use
       */
      mouseHoverOnFirstElement(selector: string): Chainable<void>;

      /**
       * Creates a user
       * @param name - The name of the user
       * @param password - The password of the user
       * @param email - The email of the user
       */
      createUser(
        name: string,
        password: string,
        email: string,
      ): Chainable<void>;

      /**
       * Creates a group
       * @param name - The name of the group
       * @param description - The description of the group
       * @param group_id - The ID of the group
       */
      createGroup(
        name: string,
        description: string,
        group_id: string,
      ): Chainable<void>;

      /**
       * Adds a group member
       * @param group_name - The name of the group
       * @param group_urn - The URN of the group
       * @param member_name - The name of the member
       */
      addGroupMember(
        group_name: string,
        group_urn: string,
        member_name: string,
      ): Chainable<void>;

      /**
       * Creates a glossary term group
       * @param term_group_name - The name of the term group
       */
      createGlossaryTermGroup(term_group_name: string): Chainable<void>;
    }
  }
}

// Constants
const SKIP_ONBOARDING_TOUR_KEY = "skipOnboardingTour";
const SKIP_INTRODUCE_PAGE_KEY = "skipAcrylIntroducePage";

// Helper functions
function selectorWithTestId(id: string): string {
  return `[data-testid="${id}"]`;
}

// Command implementations
Cypress.Commands.add("setIsThemeV2Enabled", (isEnabled: boolean) => {
  cy.intercept("POST", "/api/v2/graphql", (req: GraphQLRequest) => {
    if (hasOperationName(req, "appConfig")) {
      req.alias = "gqlappConfigQuery";

      req.on("response", (res: GraphQLResponse) => {
        if (res.body.data.appConfig) {
          res.body.data.appConfig.featureFlags.themeV2Enabled = isEnabled;
          res.body.data.appConfig.featureFlags.themeV2Default = isEnabled;
          res.body.data.appConfig.featureFlags.showNavBarRedesign = isEnabled;
        }
      });
    } else if (hasOperationName(req, "getMe")) {
      req.alias = "gqlgetMeQuery";
      req.on("response", (res: GraphQLResponse) => {
        if (res.body.data.me) {
          res.body.data.me.corpUser.settings.appearance.showThemeV2 = isEnabled;
        }
      });
    }
  });
});

Cypress.Commands.add("skipIntroducePage", () => {
  localStorage.setItem(SKIP_INTRODUCE_PAGE_KEY, "true");
});

Cypress.Commands.add("login", () => {
  cy.request({
    method: "POST",
    url: "/logIn",
    body: {
      username: Cypress.env("ADMIN_USERNAME"),
      password: Cypress.env("ADMIN_PASSWORD"),
    },
    retryOnStatusCodeFailure: true,
  }).then(() => localStorage.setItem(SKIP_ONBOARDING_TOUR_KEY, "true"));
});

Cypress.Commands.add(
  "loginWithCredentials",
  (username: string, password: string) => {
    cy.visit("/login");
    if (username && password) {
      cy.get("input[data-testid=username]").type(username);
      cy.get("input[data-testid=password]").type(password);
    } else {
      cy.get("input[data-testid=username]").type(Cypress.env("ADMIN_USERNAME"));
      cy.get("input[data-testid=password]").type(Cypress.env("ADMIN_PASSWORD"));
    }
    cy.contains("Sign In").click();
    cy.get(".ant-avatar-circle").should("be.visible");
    localStorage.setItem(SKIP_ONBOARDING_TOUR_KEY, "true");
  },
);

Cypress.Commands.add("visitWithLogin", (url: string) => {
  cy.visit(url);
  cy.get("input[data-testid=username]").type(Cypress.env("ADMIN_USERNAME"));
  cy.get("input[data-testid=password]").type(Cypress.env("ADMIN_PASSWORD"));
  localStorage.setItem(SKIP_ONBOARDING_TOUR_KEY, "true");
  cy.contains("Sign In").click();
});

Cypress.Commands.add("deleteUrn", (urn: string) => {
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
  cy.get(selectorWithTestId("manage-account-menu")).click();
  cy.get(selectorWithTestId("log-out-menu-item")).click({ force: true });
  cy.waitTextVisible("Username");
  cy.waitTextVisible("Password");
});

Cypress.Commands.add("logoutV2", () => {
  cy.get(selectorWithTestId("nav-sidebar-sign-out")).click({ force: true });
  cy.waitTextVisible("Username");
  cy.waitTextVisible("Password");
});

Cypress.Commands.add("waitTextVisible", (text: string) => {
  cy.contains(text).should("be.visible");
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
  cy.waitTextVisible("Manage Views");
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

Cypress.Commands.add(
  "goToDataset",
  (urn: string, dataset_name: string, login?: boolean) => {
    if (login) {
      cy.visitWithLogin(`/dataset/${urn}/`);
    } else {
      cy.visit(`/dataset/${urn}/`);
    }
    cy.wait(3000);
    cy.waitTextVisible(dataset_name);
  },
);

Cypress.Commands.add(
  "goToBusinessAttribute",
  (urn: string, attribute_name: string) => {
    cy.visit(`/business-attribute/${urn}`);
    cy.wait(5000);
    cy.waitTextVisible(attribute_name);
  },
);

Cypress.Commands.add("goToTag", (urn: string, tag_name: string) => {
  cy.visit(`/tag/${urn}`);
  cy.wait(5000);
  cy.waitTextVisible(tag_name);
});

Cypress.Commands.add(
  "goToEntityLineageGraph",
  (entity_type: string, urn: string) => {
    cy.visit(`/${entity_type}/${urn}?is_lineage_mode=true`);
  },
);

Cypress.Commands.add(
  "goToEntityLineageGraph",
  (
    entity_type: string,
    urn: string,
    start_time_millis: number,
    end_time_millis: number,
  ) => {
    cy.visit(
      `/${entity_type}/${urn}?is_lineage_mode=true&start_time_millis=${start_time_millis}&end_time_millis=${end_time_millis}`,
    );
  },
);

Cypress.Commands.add(
  "goToEntityLineageGraphV2",
  (entity_type: string, urn: string) => {
    cy.visit(`/${entity_type}/${urn}/Lineage`);
  },
);

Cypress.Commands.add(
  "goToEntityLineageGraphV2",
  (
    entity_type: string,
    urn: string,
    start_time_millis: number,
    end_time_millis: number,
  ) => {
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

Cypress.Commands.add("goToChart", (urn: string) => {
  cy.visit(`/chart/${urn}`);
});

Cypress.Commands.add("goToContainer", (urn: string) => {
  cy.visit(`/container/${urn}`);
});

Cypress.Commands.add("goToDomain", (urn: string) => {
  cy.visit(`/domain/${urn}`);
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

Cypress.Commands.add("clickOptionWithText", (text: string) => {
  cy.contains(text).should("be.visible").click();
});

Cypress.Commands.add("clickFirstOptionWithText", (text: string) => {
  cy.contains(text).first().click();
});

Cypress.Commands.add("clickOptionWithTextToScrollintoView", (text: string) => {
  cy.contains(text).scrollIntoView().click();
});

Cypress.Commands.add(
  "clickOptionInScrollView",
  (text: string, selector: string) => {
    cy.get(selector).within(() => {
      cy.contains(text).then(($el: JQuery<HTMLElement>) => {
        if ($el && $el.length > 0) {
          $el[0].scrollIntoView({ block: "center", inline: "nearest" });
          cy.wrap($el)
            .should("be.visible")
            .should("not.be.disabled")
            .click({ force: true });
        }
      });
    });
  },
);

Cypress.Commands.add("deleteFromDropdown", () => {
  cy.openThreeDotDropdown();
  cy.clickOptionWithText("Delete");
  cy.clickOptionWithText("Yes");
});

Cypress.Commands.add("addViaFormModal", (text: string, modelHeader: string) => {
  cy.waitTextVisible(modelHeader);
  cy.get(".ProseMirror-focused").type(text);
  cy.get(".ant-modal-footer > button:nth-child(2)").click();
});

Cypress.Commands.add(
  "addViaModal",
  (text: string, modelHeader: string, value: string, dataTestId: string) => {
    cy.waitTextVisible(modelHeader);
    cy.get('.ant-modal-content .ant-input-affix-wrapper > input[type="text"]')
      .first()
      .type(text);
    cy.get(`[data-testid="${dataTestId}"]`).click();
    cy.contains(value).should("be.visible");
  },
);

Cypress.Commands.add(
  "addBusinessAttributeViaModal",
  (text: string, modelHeader: string, value: string, dataTestId: string) => {
    cy.waitTextVisible(modelHeader);
    cy.get('.ant-input-affix-wrapper > input[type="text"]').first().type(text);
    cy.get(`[data-testid="${dataTestId}"]`).click();
    cy.wait(3000);
    cy.contains(value).should("be.visible");
  },
);

Cypress.Commands.add("ensureTextNotPresent", (text: string) => {
  cy.contains(text).should("not.exist");
});

Cypress.Commands.add("ensureElementPresent", (element: string) => {
  cy.get(element).should("be.visible");
});

Cypress.Commands.add("waitTextPresent", (text: string) => {
  cy.contains(text).should("exist");
  cy.contains(text).should("have.length.above", 0);
  return cy.contains(text);
});

Cypress.Commands.add("waitTestIdVisible", (testId: string) => {
  cy.get(`[data-testid="${testId}"]`).should("exist");
});

Cypress.Commands.add("openMultiSelect", (data_id: string) => {
  const selector = `${selectorWithTestId(data_id)}`;
  cy.get(
    `.ant-select${selector} > .ant-select-selector > .ant-select-selection-search`,
  ).click();
});

Cypress.Commands.add("multiSelect", (within_data_id: string, text: string) => {
  cy.openMultiSelect(within_data_id);
  cy.waitTextVisible(text);
  cy.clickOptionWithText(text);
});

Cypress.Commands.add("getWithTestId", (id: string) => {
  return cy.get(selectorWithTestId(id));
});

Cypress.Commands.add("clickOptionWithId", (id: string) => {
  cy.get(id).click();
});

Cypress.Commands.add(
  "enterTextInSpecificTestId",
  (id: string, value: number, text: string) => {
    cy.get(selectorWithTestId(id)).eq(value).type(text);
  },
);

Cypress.Commands.add("enterTextInTestId", (id: string, text: string) => {
  cy.get(selectorWithTestId(id)).type(text);
});

Cypress.Commands.add("clickOptionWithTestId", (id: string) => {
  cy.get(selectorWithTestId(id)).first().click({
    force: true,
  });
});

Cypress.Commands.add("clickFirstOptionWithTestId", (id: string) => {
  cy.get(selectorWithTestId(id)).first().click({
    force: true,
  });
});

Cypress.Commands.add(
  "clickFirstOptionWithSpecificTestId",
  (id: string, value: number) => {
    cy.get(selectorWithTestId(id)).eq(value).click({
      force: true,
    });
  },
);

Cypress.Commands.add(
  "clickOptionWithSpecificClass",
  (locator: string, value: number) => {
    cy.get(locator).should("be.visible");
    cy.get(locator).eq(value).click();
  },
);

Cypress.Commands.add(
  "clickTextOptionWithClass",
  (locator: string, text: string) => {
    cy.get(locator).should("be.visible").contains(text).click({ force: true });
  },
);

Cypress.Commands.add("hideOnboardingTour", () => {
  cy.get("body").type("{ctrl} {meta} h");
});

Cypress.Commands.add("clearView", (viewName: string) => {
  cy.clickOptionWithTestId("view-select");
  cy.clickOptionWithTestId("view-select-clear");
  cy.get('input[data-testid="search-input"]').click();
  cy.get('[data-testid="view-select"]').should("not.include.text", viewName);
});

Cypress.Commands.add(
  "addTermToDataset",
  (urn: string, dataset_name: string, term: string) => {
    cy.goToDataset(urn, dataset_name);
    cy.clickOptionWithText("Add Term");
    cy.selectOptionInTagTermModal(term);
    cy.contains(term);
  },
);

Cypress.Commands.add(
  "addTermToBusinessAttribute",
  (urn: string, attribute_name: string, term: string) => {
    cy.goToBusinessAttribute(urn, attribute_name);
    cy.clickOptionWithText("Add Terms");
    cy.selectOptionInTagTermModal(term);
    cy.contains(term);
  },
);

Cypress.Commands.add(
  "addAttributeToDataset",
  (urn: string, dataset_name: string, businessAttribute: string) => {
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

Cypress.Commands.add("selectOptionInTagTermModal", (text: string) => {
  cy.enterTextInTestId("tag-term-modal-input", text);
  cy.clickOptionWithTestId("tag-term-option");
  const btn_id = "add-tag-term-from-modal-btn";
  cy.clickOptionWithTestId(btn_id);
  cy.get(selectorWithTestId(btn_id)).should("not.exist");
});

Cypress.Commands.add("selectOptionInAttributeModal", (text: string) => {
  cy.enterTextInTestId("business-attribute-modal-input", text);
  cy.clickOptionWithTestId("business-attribute-option");
  const btn_id = "add-attribute-from-modal-btn";
  cy.clickOptionWithTestId(btn_id);
  cy.get(selectorWithTestId(btn_id)).should("not.exist");
});

Cypress.Commands.add(
  "removeDomainFromDataset",
  (urn: string, dataset_name: string, domain_urn: string) => {
    cy.goToDataset(urn, dataset_name);
    cy.get(
      `.sidebar-domain-section [href="/domain/${domain_urn}"] .anticon-close`,
    ).click();
    cy.clickOptionWithText("Yes");
  },
);

Cypress.Commands.add("openEntityTab", (tab: string) => {
  const selector = `div[id$="${tab}"]:nth-child(1)`;
  cy.get(selector).click();
});

Cypress.Commands.add("mouseover", (selector: string) => {
  return cy.get(selector).trigger("mouseover", { force: true });
});

Cypress.Commands.add("mouseHoverOnFirstElement", (selector: string) => {
  return cy.get(selector).first().trigger("mouseover", { force: true });
});

Cypress.Commands.add(
  "createUser",
  (name: string, password: string, email: string) => {
    cy.visit("/settings/identities/users");
    cy.clickOptionWithText("Invite Users");
    cy.waitTextVisible("signup?invite_token=").then(
      ($elem: JQuery<HTMLElement>) => {
        const inviteLink = $elem.text();
        cy.visit("/settings/identities/users");
        cy.logout();
        cy.visit(inviteLink);
        cy.enterTextInTestId("email", email);
        cy.enterTextInTestId("name", name);
        cy.enterTextInTestId("password", password);
        cy.enterTextInTestId("confirmPassword", password);
        cy.mouseover("#title").click();
        cy.waitTextVisible("Other").click();
        cy.get("[type=submit]").click();
        cy.waitTextVisible("Welcome back");
        cy.hideOnboardingTour();
        cy.waitTextVisible(name);
        cy.logout();
        cy.loginWithCredentials();
      },
    );
  },
);

Cypress.Commands.add(
  "createGroup",
  (name: string, description: string, group_id: string) => {
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
  },
);

Cypress.Commands.add(
  "addGroupMember",
  (group_name: string, group_urn: string, member_name: string) => {
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
  },
);

Cypress.Commands.add("createGlossaryTermGroup", (term_group_name: string) => {
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

Cypress.on("uncaught:exception", (err) => {
  const resizeObserverLoopErrMessage = "ResizeObserver loop limit exceeded";

  /* returning false here prevents Cypress from failing the test */
  if (err.message.includes(resizeObserverLoopErrMessage)) {
    return false;
  }
  return true;
});
