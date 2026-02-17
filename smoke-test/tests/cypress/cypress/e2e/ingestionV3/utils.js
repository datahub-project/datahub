import { hasOperationName } from "../utils";

const readyToTypeEditor = () =>
  cy.get(".monaco-scrollable-element").first().click().focused();

export const setThemeV2AndIngestionRedesignFlags = (isOn) => {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.reply((res) => {
        res.body.data.appConfig.featureFlags.showIngestionPageRedesign = isOn;
        res.body.data.appConfig.featureFlags.ingestionOnboardingRedesignV1 =
          isOn;
        res.body.data.appConfig.featureFlags.themeV2Enabled = isOn;
        res.body.data.appConfig.featureFlags.themeV2Default = isOn;
        res.body.data.appConfig.featureFlags.showNavBarRedesign = isOn;
      });
    }
  });
};

export const goToIngestionPage = () => {
  cy.visit("/ingestion");
};

const clickIfTestIdPresent = (dataTestId) => {
  cy.get("body").then(($body) => {
    if ($body.find(`[data-testid="${dataTestId}"]`).length) {
      cy.get(`[data-testid="${dataTestId}"]`).click();
    }
  });
};

export const verifySourceDetails = (source) => {
  cy.get("#account_id").type(source.accound_id);
  cy.get("#warehouse").type(source.warehouse_id);
  cy.get("#username").type(source.username);
  cy.get("#password").type(source.password);
  cy.get("body").click(0, 0);
  cy.get("#role").type(source.role);

  // Verify yaml recipe is generated correctly
  cy.clickOptionWithTestId("yaml-editor-tab");
  cy.waitTextVisible("account_id");
  cy.waitTextVisible(source.accound_id);
  cy.waitTextVisible(source.warehouse_id);
  cy.waitTextVisible(source.username);
  cy.waitTextVisible(source.password);
};

export const changeSchedule = (values) => {
  if (values?.hour) {
    cy.get(".cron-builder-hours").within(() => {
      cy.get(".ant-select-clear")
        .should("exist")
        .then(($clearElement) => {
          // Firstly clear the select if it has some value (clear icon is available in this case)
          if ($clearElement) {
            cy.wrap($clearElement).click();
          }
          cy.get(".ant-select").click();
          cy.document()
            .its("body")
            .within(() => {
              cy.get(`[title="${values.hour}"]`).should("be.visible").click();
            });
          // ensure the new value was applied
          cy.get(".ant-select").should("contain", values.hour);
        });
    });
  }
};

export const verifyScheduleIsAppliedOnTable = (sourceName, scheduleText) => {
  cy.getWithTestId(`row-${sourceName}`)
    .scrollIntoView()
    .within(() => {
      cy.getWithTestId("schedule").should("contain", scheduleText);
    });
};

export const createIngestionSource = (sourceName, options = undefined) => {
  cy.clickOptionWithTestId("create-ingestion-source-button");
  clickIfTestIdPresent("modal-confirm-button");
  cy.clickOptionWithTextToScrollintoView("Snowflake");
  cy.waitTextVisible("Snowflake Connection Details");

  if (options?.sourceDetails) {
    verifySourceDetails(options.sourceDetails);
  }

  cy.get('[data-testid="data-source-name"]').clear();
  cy.get('[data-testid="data-source-name"]').type(sourceName);

  // Finish creating source
  cy.clickOptionWithTestId("next-button");
  cy.waitTextVisible("Sync Schedule");
  if (options?.schedule) {
    changeSchedule(options?.schedule);
  }

  cy.clickOptionWithTestId("save-button");
  cy.waitTextVisible("Successfully created ingestion source!");
};

export const updateIngestionSource = (
  sourceName,
  updatedSourceName,
  options = undefined,
) => {
  cy.interceptGraphQLOperation("getIngestionSource");
  cy.contains("td", sourceName)
    .siblings("td")
    .find('[data-testid="ingestion-more-options"]')
    .click();
  cy.get("body .ant-dropdown-menu").contains("Edit").click();
  cy.waitForGraphQLOperation("getIngestionSource");

  cy.get('[data-testid="data-source-name"]')
    .focus()
    .type(`{selectall}{backspace}${updatedSourceName}`);
  cy.get('[data-testid="next-button"]').scrollIntoView().click();
  cy.waitTextVisible("Sync Schedule");
  if (options?.schedule) {
    changeSchedule(options?.schedule);
  }

  cy.clickOptionWithTestId("save-button");
  cy.waitTextVisible("Successfully updated ingestion source!");
};

export const deleteIngestionSource = (sourceName) => {
  cy.contains("td", sourceName)
    .siblings("td")
    .find('[data-testid="ingestion-more-options"]')
    .click({ force: true });
  cy.get("body .ant-dropdown-menu").contains("Delete").click({ force: true });
  cy.waitTextVisible("Confirm Ingestion Source Removal");
  cy.get('[data-testid="modal-confirm-button"]').filter(":visible").click();
  cy.waitTextVisible("Removed ingestion source");
};

export const createAndRunIngestionSource = (sourceName) => {
  const cli_version = "0.15.0.5";
  cy.clickOptionWithTestId("create-ingestion-source-button");
  clickIfTestIdPresent("modal-confirm-button");

  cy.get('[data-testid="search-bar-input"]').type("other");
  cy.clickOptionWithTextToScrollintoView("Custom");

  cy.waitTextVisible("source-type");
  readyToTypeEditor().type("{ctrl}a").clear();
  readyToTypeEditor().type("source:{enter}");
  readyToTypeEditor().type("    type: demo-data");
  readyToTypeEditor().type("{enter}");
  // no space because the editor starts new line at same indentation
  readyToTypeEditor().type("config: {}");
  cy.enterTextInTestId("data-source-name", sourceName);

  cy.clickOptionWithTestId("expand-collapse-button");
  cy.enterTextInTestId("cli-version-input", cli_version);
  cy.clickOptionWithText("Next");

  cy.clickOptionWithTestId("save-and-run-button");
  cy.waitTextVisible(sourceName);
  cy.contains("td", sourceName)
    .parent()
    .within(() => {
      cy.contains("Success", { timeout: 100000 });
    });
};

export const runIngestionSource = (sourceName) => {
  cy.contains("td", sourceName)
    .siblings("td")
    .find('[data-testid="run-ingestion-source-button"]')
    .click({ force: true });
  cy.waitTextVisible("Confirm Source Execution");
  cy.get('[data-testid="modal-confirm-button"').filter(":visible").click();
};

export const shouldNavigateToRunHistoryTab = () => {
  cy.get('div[data-node-key="RunHistory"]').should(
    "have.class",
    "ant-tabs-tab-active",
  );
};

export const shouldNavigateToSourcesTab = () => {
  cy.get('div[data-node-key="Sources"]').should(
    "have.class",
    "ant-tabs-tab-active",
  );
};

export const navigateToTab = (tabName) => {
  cy.get(`div[data-node-key="${tabName}"]`).click();
};

export const filterBySource = (sourceName) => {
  cy.get('[data-testid="source-name-filter"').click();
  cy.get('[data-testid="dropdown-search-bar"').type(sourceName);
  cy.get("body .ant-dropdown").contains(sourceName).click();
  cy.get('[data-testid="footer-button-update"').click();
};
