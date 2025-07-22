import { aliasQuery, hasOperationName } from "../utils";

describe("WelcomeToDataHubModal", () => {
  const SKIP_ONBOARDING_TOUR_KEY = "skipOnboardingTour";
  const THEME_V2_STATUS_KEY = "isThemeV2Enabled";

  beforeEach(() => {
    cy.window().then((win) => {
      win.localStorage.removeItem(SKIP_ONBOARDING_TOUR_KEY);
      cy.skipIntroducePage();
      win.localStorage.setItem(THEME_V2_STATUS_KEY, "true");
    });

    // Intercept GraphQL requests if needed
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");

      // Mock app config to enable theme V2
      if (hasOperationName(req, "appConfig")) {
        req.alias = "gqlappConfigQuery";
        req.continue((res) => {
          res.body.data.appConfig.featureFlags.themeV2Enabled = true;
          res.body.data.appConfig.featureFlags.themeV2Default = true;
        });
      }
    });
  });

  afterEach(() => {
    cy.window().then((win) => {
      win.localStorage.removeItem(SKIP_ONBOARDING_TOUR_KEY);
      win.localStorage.removeItem(THEME_V2_STATUS_KEY);
    });
  });

  it("should display the modal for first-time users", () => {
    cy.loginForOnboarding();
    cy.visit("/");

    cy.findByRole("dialog").should("be.visible");

    // Click the last carousel dot
    cy.findByRole("dialog").find("li").last().click();

    // Click Get Started button
    cy.findByRole("button", { name: /get started/i }).click();
    cy.findByRole("dialog").should("not.exist");

    cy.window().then((win) => {
      expect(win.localStorage.getItem(SKIP_ONBOARDING_TOUR_KEY)).to.equal(
        "true",
      );
    });
  });

  it("should not display the modal if user has already seen it", () => {
    cy.window().then((win) => {
      win.localStorage.setItem(SKIP_ONBOARDING_TOUR_KEY, "true");
    });

    cy.loginForOnboarding();
    cy.visit("/");

    cy.findByRole("dialog").should("not.exist");
  });

  it("should handle user interactions and track events", () => {
    // Set up intercept that captures all track events
    cy.intercept("POST", "**/track**", { statusCode: 200 }).as("trackEvents");

    cy.loginForOnboarding();
    cy.visit("/");

    cy.findByRole("dialog").should("be.visible");
    cy.findByLabelText(/close/i).click();
    cy.findByRole("dialog").should("not.exist");

    // Wait specifically for the Exit event to appear and verify all properties
    cy.get("@trackEvents.all").should((interceptions) => {
      const modalExitEvent = interceptions.find(
        (interception) =>
          interception.request.body.type === "WelcomeToDataHubModalExitEvent",
      );

      // Verify the event exists and has all expected properties
      expect(modalExitEvent.request.body.exitMethod).to.equal("close_button");
      expect(modalExitEvent.request.body.currentSlide).to.be.a("number");
      expect(modalExitEvent.request.body.totalSlides).to.be.a("number");
    });
    cy.window().then((win) => {
      expect(win.localStorage.getItem(SKIP_ONBOARDING_TOUR_KEY)).to.equal(
        "true",
      );
    });
  });
});
