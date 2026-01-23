// Use the frontend proxy which handles authentication
const MAINTENANCE_API_PATH = "/openapi/operations/maintenance";

const TEST_MESSAGE = "Cypress test: Scheduled maintenance in progress";

/**
 * Enable maintenance mode via the OpenAPI endpoint (through frontend proxy).
 */
const enableMaintenanceMode = (message, severity = "WARNING") => {
  cy.request({
    method: "POST",
    url: `${MAINTENANCE_API_PATH}/enable`,
    body: {
      message,
      severity,
      linkUrl: "https://status.example.com",
      linkText: "Status Page",
    },
    headers: {
      "Content-Type": "application/json",
    },
    failOnStatusCode: false,
  }).then((response) => {
    expect(response.status).to.eq(200);
    expect(response.body.enabled).to.eq(true);
  });
};

/**
 * Disable maintenance mode via the OpenAPI endpoint (through frontend proxy).
 */
const disableMaintenanceMode = () => {
  cy.request({
    method: "POST",
    url: `${MAINTENANCE_API_PATH}/disable`,
    headers: {
      "Content-Type": "application/json",
    },
    failOnStatusCode: false,
  }).then((response) => {
    expect(response.status).to.eq(200);
  });
};

describe("maintenance banner", () => {
  beforeEach(() => {
    cy.login();
    // Ensure maintenance mode is disabled before each test
    disableMaintenanceMode();
  });

  afterEach(() => {
    // Clean up: disable maintenance mode after each test
    disableMaintenanceMode();
  });

  it("displays maintenance banner when maintenance mode is enabled", () => {
    // Enable maintenance mode
    enableMaintenanceMode(TEST_MESSAGE, "WARNING");

    // Visit the home page
    cy.visit("/");

    // Verify the maintenance banner appears with the correct message
    cy.get('[data-testid="maintenance-banner"]').should("be.visible");
    cy.get('[data-testid="maintenance-banner"]').should(
      "contain",
      TEST_MESSAGE,
    );

    // Verify the link is present
    cy.get('[data-testid="maintenance-banner"]')
      .find("a")
      .should("contain", "Status Page")
      .and("have.attr", "href", "https://status.example.com");
  });

  it("hides maintenance banner when maintenance mode is disabled", () => {
    // First enable maintenance mode
    enableMaintenanceMode(TEST_MESSAGE, "INFO");

    // Visit the home page and verify banner is visible
    cy.visit("/");
    cy.get('[data-testid="maintenance-banner"]').should("be.visible");

    // Now disable maintenance mode
    disableMaintenanceMode();

    // Reload and verify banner is no longer visible
    cy.reload();
    cy.get('[data-testid="maintenance-banner"]').should("not.exist");
  });

  it("displays different severity levels correctly", () => {
    // Test INFO severity
    enableMaintenanceMode("Info maintenance message", "INFO");
    cy.visit("/");
    cy.get('[data-testid="maintenance-banner"]').should("be.visible");
    cy.get('[data-testid="maintenance-banner"]').should(
      "have.attr",
      "data-severity",
      "info",
    );

    // Test WARNING severity
    disableMaintenanceMode();
    enableMaintenanceMode("Warning maintenance message", "WARNING");
    cy.reload();
    cy.get('[data-testid="maintenance-banner"]').should("be.visible");
    cy.get('[data-testid="maintenance-banner"]').should(
      "have.attr",
      "data-severity",
      "warning",
    );

    // Test CRITICAL severity
    disableMaintenanceMode();
    enableMaintenanceMode("Critical maintenance message", "CRITICAL");
    cy.reload();
    cy.get('[data-testid="maintenance-banner"]').should("be.visible");
    cy.get('[data-testid="maintenance-banner"]').should(
      "have.attr",
      "data-severity",
      "critical",
    );
  });
});
