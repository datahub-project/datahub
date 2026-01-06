import { aliasQuery, hasOperationName } from "../utils";

describe("WelcomeToDataHubModal", () => {
  const SKIP_WELCOME_MODAL_KEY = "skipWelcomeModal";
  const THEME_V2_STATUS_KEY = "isThemeV2Enabled";

  const setupInterceptWithTrialConfig = (trialEnabled = false) => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");

      // Mock app config to enable theme V2 and set trial config
      if (hasOperationName(req, "appConfig")) {
        req.alias = "gqlappConfigQuery";
        req.continue((res) => {
          res.body.data.appConfig.featureFlags.themeV2Enabled = true;
          res.body.data.appConfig.featureFlags.themeV2Default = true;
          res.body.data.appConfig.trialConfig.trialEnabled = trialEnabled;
        });
      }
    });
  };

  beforeEach(() => {
    cy.window().then((win) => {
      win.localStorage.removeItem(SKIP_WELCOME_MODAL_KEY);
      cy.skipIntroducePage();
      win.localStorage.setItem(THEME_V2_STATUS_KEY, "true");
    });

    // Default to non-trial config
    setupInterceptWithTrialConfig(false);
  });

  afterEach(() => {
    cy.window().then((win) => {
      win.localStorage.removeItem(SKIP_WELCOME_MODAL_KEY);
      win.localStorage.removeItem(THEME_V2_STATUS_KEY);
    });
  });

  it("should display the modal for first-time users", () => {
    cy.intercept("POST", "**/track**", { statusCode: 200 }).as("trackEvents");

    cy.loginForOnboarding();
    cy.visit("/");

    cy.findByRole("dialog").should("be.visible");

    // Click the last carousel dot
    cy.findByRole("dialog").find("li").last().click();

    // Click Get Started button
    cy.findByRole("button", { name: /get started/i }).click();
    cy.findByRole("dialog").should("not.exist");

    cy.window().then((win) => {
      expect(win.localStorage.getItem(SKIP_WELCOME_MODAL_KEY)).to.equal("true");
    });
  });

  it("should not display the modal if user has already seen it", () => {
    cy.window().then((win) => {
      win.localStorage.setItem(SKIP_WELCOME_MODAL_KEY, "true");
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
      expect(win.localStorage.getItem(SKIP_WELCOME_MODAL_KEY)).to.equal("true");
    });
  });

  describe("Free Trial Instance", () => {
    beforeEach(() => {
      setupInterceptWithTrialConfig(true);
    });

    it("should display Ask DataHub slide as first slide for free trial users", () => {
      cy.intercept("POST", "**/track**", { statusCode: 200 }).as("trackEvents");

      cy.loginForOnboarding();
      cy.visit("/");

      cy.findByRole("dialog").should("be.visible");

      // Verify first slide contains "Your Instance is Ready" heading
      cy.findByRole("dialog").should("contain.text", "Your Instance is Ready");
      cy.findByRole("dialog").should(
        "contain.text",
        "Ask DataHub can answer questions",
      );
    });

    it("should not display Impact Analysis slide for free trial users", () => {
      cy.intercept("POST", "**/track**", { statusCode: 200 }).as("trackEvents");

      cy.loginForOnboarding();
      cy.visit("/");

      cy.findByRole("dialog").should("be.visible");

      // Verify Impact Analysis heading is not present (check all slides by looking at the DOM)
      // Rather than trying to navigate through autoplay carousel, just check the entire dialog
      cy.get("body").should(
        "not.contain.text",
        "Manage Breaking Changes Confidently",
      );
    });

    it("should have correct number of slides for free trial (4 slides)", () => {
      cy.intercept("POST", "**/track**", { statusCode: 200 }).as("trackEvents");

      cy.loginForOnboarding();
      cy.visit("/");

      cy.findByRole("dialog").should("be.visible");

      // Count the carousel dots (indicators)
      cy.findByRole("dialog").find("li").should("have.length", 4);
    });

    it("should track events with correct totalSlides count for free trial", () => {
      cy.intercept("POST", "**/track**", { statusCode: 200 }).as("trackEvents");

      cy.loginForOnboarding();
      cy.visit("/");

      cy.findByRole("dialog").should("be.visible");
      cy.findByLabelText(/close/i).click();
      cy.findByRole("dialog").should("not.exist");

      // Verify the exit event has correct totalSlides for free trial
      cy.get("@trackEvents.all").should((interceptions) => {
        const modalExitEvent = interceptions.find(
          (interception) =>
            interception.request.body.type === "WelcomeToDataHubModalExitEvent",
        );

        expect(modalExitEvent.request.body.exitMethod).to.equal("close_button");
        expect(modalExitEvent.request.body.totalSlides).to.equal(4);
      });
    });
  });

  describe("Non-Trial Instance", () => {
    it("should display all standard slides including Impact Analysis", () => {
      cy.intercept("POST", "**/track**", { statusCode: 200 }).as("trackEvents");

      cy.loginForOnboarding();
      cy.visit("/");

      cy.findByRole("dialog").should("be.visible");

      // Wait for carousel to auto-advance to show Impact Analysis slide
      // The slide should appear within the autoplay duration (10 seconds)
      cy.findByRole("dialog").should(
        "contain.text",
        "Manage Breaking Changes Confidently",
      );
    });

    it("should not display Ask DataHub slide for non-trial users", () => {
      cy.intercept("POST", "**/track**", { statusCode: 200 }).as("trackEvents");

      cy.loginForOnboarding();
      cy.visit("/");

      cy.findByRole("dialog").should("be.visible");

      // Verify Ask DataHub heading is not present (check the entire page)
      cy.get("body").should("not.contain.text", "Your Instance is Ready");
    });
  });
});
