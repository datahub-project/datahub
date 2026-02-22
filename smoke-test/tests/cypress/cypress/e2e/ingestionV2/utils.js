import { hasOperationName } from "../utils";

const readyToTypeEditor = () =>
  cy.get(".monaco-scrollable-element").first().click().focused();

export const setThemeV2AndIngestionRedesignFlags = (isOn) => {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.reply((res) => {
        res.body.data.appConfig.featureFlags.showIngestionPageRedesign = isOn;
        res.body.data.appConfig.featureFlags.ingestionOnboardingRedesignV1 = false;
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

export const fillSourceDetails = (source) => {
  cy.get("#account_id").type(source.accound_id);
  cy.get("#warehouse").type(source.warehouse_id);
  cy.get("#username").type(source.username);

  // Select authentication type if provided, otherwise default to Username & Password
  if (source.authentication_type) {
    cy.get("#authentication_type").click({ force: true });
    cy.get(`[title="${source.authentication_type}"]`).click({
      force: true,
      multiple: true,
    });
  }

  // Fill in credentials based on authentication type
  if (source.authentication_type === "Private Key") {
    if (source.private_key) {
      cy.get("#private_key").type(source.private_key);
    }
    if (source.private_key_password) {
      cy.get("#private_key_password").type(source.private_key_password);
    }
  } else {
    // Default to Username & Password
    cy.get("#password").type(source.password);
  }

  cy.focused().blur();
  cy.get("#role").type(source.role);
};

export const verifySourceDetails = (source) => {
  fillSourceDetails(source);

  // Verify yaml recipe is generated correctly
  cy.clickOptionWithTestId("recipe-builder-yaml-button");
  cy.waitTextVisible("account_id");
  cy.waitTextVisible(source.accound_id);
  cy.waitTextVisible(source.warehouse_id);
  cy.waitTextVisible(source.username);

  // Verify credentials in YAML based on auth type
  if (source.authentication_type === "Private Key" && source.private_key) {
    // For private key, just verify it contains the key content (not exact format with newlines)
    cy.waitTextVisible("private_key");
    cy.waitTextVisible("BEGIN PRIVATE KEY");
  } else {
    cy.waitTextVisible(source.password);
  }

  cy.waitTextVisible(source.role);
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
    .first()
    .scrollIntoView()
    .within(() => {
      cy.getWithTestId("schedule").should("contain", scheduleText);
    });
};

export const createIngestionSource = (sourceName, options = undefined) => {
  cy.clickOptionWithTestId("create-ingestion-source-button");
  cy.clickOptionWithTextToScrollintoView("Snowflake");
  cy.waitTextVisible("Snowflake Details");

  // Provide default source details if none specified
  // Must specify authentication_type since we changed the default to KEY_PAIR
  const defaultSourceDetails = {
    accound_id: "test_account",
    warehouse_id: "test_warehouse",
    username: "test_user",
    password: "test_password",
    role: "test_role",
    authentication_type: "Username & Password", // Explicitly use password auth for tests
  };

  if (options?.sourceDetails) {
    verifySourceDetails(options.sourceDetails);
  } else {
    // Just fill in minimal details without verification
    fillSourceDetails(defaultSourceDetails);
  }

  // Finish creating source
  cy.clickOptionWithTestId("recipe-builder-next-button");
  cy.waitTextVisible("Configure an Ingestion Schedule");
  if (options?.schedule) {
    changeSchedule(options?.schedule);
  }
  cy.clickOptionWithTestId("ingestion-schedule-next-button");
  cy.waitTextVisible("Give this data source a name");
  cy.get('[data-testid="source-name-input"]').clear();
  cy.get('[data-testid="source-name-input"]').type(sourceName);
  cy.clickOptionWithTestId("ingestion-source-save-button");
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
  cy.waitTextVisible("Edit Data Source");
  cy.get("#password").type("password123");
  cy.get('[data-testid="recipe-builder-next-button"]').scrollIntoView().click();

  cy.waitTextVisible("Configure an Ingestion Schedule");
  if (options?.schedule) {
    changeSchedule(options?.schedule);
  }
  cy.clickOptionWithTestId("ingestion-schedule-next-button");
  cy.get('[data-testid="source-name-input"]')
    .focus()
    .type(`{selectall}{backspace}${updatedSourceName}`);
  cy.clickOptionWithTestId("ingestion-source-save-button");
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

  // Wait for the source selection modal to appear and any loading to finish
  // Multi-step builder uses "Search..." while old builder uses "Search data sources..."
  cy.wait(1000); // Wait for modal animations to complete
  cy.get('[placeholder="Search..."], [placeholder="Search data sources..."]', {
    timeout: 10000,
  })
    .first()
    .should("be.visible")
    .type("custom", { force: true });
  cy.clickOptionWithTextToScrollintoView("Custom");

  cy.waitTextVisible("source-type");
  readyToTypeEditor().type("{ctrl}a").clear();
  readyToTypeEditor().type("source:{enter}");
  readyToTypeEditor().type("    type: demo-data");
  readyToTypeEditor().type("{enter}");
  // no space because the editor starts new line at same indentation
  readyToTypeEditor().type("config: {}");
  cy.clickOptionWithText("Next");
  cy.clickOptionWithText("Next");

  cy.enterTextInTestId("source-name-input", sourceName);
  cy.clickOptionWithText("Advanced");
  cy.enterTextInTestId("cli-version-input", cli_version);
  cy.clickOptionWithTextToScrollintoView("Save & Run");
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
