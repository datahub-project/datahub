const MFE_CONTAINER_CSS_SELECTOR = '[data-testid="mfe-configurable-container"]';

describe("MFE YAML Config - Valid Config", () => {
  beforeEach(() => {
    cy.intercept("GET", "/mfe/config", {
      statusCode: 200,
      body: `
subNavigationMode: false
microFrontends:
  - id: HelloWorld
    label: HelloWorld Cypress
    path: /helloworld-mfe
    remoteEntry: http://localhost-cypress:3002/remoteEntry.js
    module: helloWorldMFE/mount
    flags:
      enabled: true
      showInNav: true
    navIcon: HandWaving      
      `,
    }).as("mfeConfig");

    cy.setIsThemeV2Enabled(true);
    cy.skipIntroducePage();
    cy.on("uncaught:exception", (err, runnable) => false);
    cy.visitWithLogin("/");
    cy.wait("@mfeConfig");
    // Example of taking screenshots of key moments, for debugging or documentation:
    // cy.screenshot("after-visit");
  });

  it("shows all MFE items from YAML in the sidebar", () => {
    cy.clickOptionWithTestId("nav-bar-item-home");
    cy.contains("HelloWorld Cypress").should("exist");
  });

  it("navigates to MFE route when sidebar item is clicked and shows error if mounting fails", () => {
    cy.intercept("GET", "http://localhost-cypress:3002/remoteEntry.js", {
      statusCode: 503,
    }).as("remoteEntry");
    cy.clickOptionWithTestId("nav-bar-item-home");
    cy.contains("HelloWorld Cypress").should("exist");
    cy.contains("HelloWorld Cypress").click();
    cy.url().should("include", "/helloworld-mfe");
    cy.wait("@remoteEntry");
    cy.contains("HelloWorld Cypress is not available");
  });

  it("navigates to MFE route when sidebar item is clicked and shows MFE container on success", () => {
    /*
   Configures the right GET interception so that loading from remote server is faked to work successfully.
   Real flow
   window.varToHoldRemote = result of calling GET childhost.com/remoteEntry.js
   await window.varToHoldRemote.init(scopes);
   const factory = await window.varToHoldRemote.get('./mount');
   const module = factory();
   const { mount } = module;
   Fake flow
   cy.intercept returns a fake object to mimic above interactions without error
   */
    cy.intercept("GET", "http://localhost-cypress:3002/remoteEntry.js", {
      statusCode: 200,
      body: `
      window.helloWorldMFE = {
        init: function(shared) { 
          console.log('helloWorldMFE.init() called'); 
        },
        get: function(module) {
          console.log('helloWorldMFE.get() called for:', module);
          return Promise.resolve(() => {
            return function mount(containerElement, props) {
              console.log('mount function called directly');
              if (containerElement) {
                containerElement.innerHTML = \`
                  <div>
                    <h1>HelloWorld Cypress MFE Mounted</h1>
                  </div>
                \`;
              }
              return () => console.log('cleanup called');
            };
          });
        }
      };
    `,
      headers: { "content-type": "application/javascript" },
    }).as("remoteEntry");
    cy.clickOptionWithTestId("nav-bar-item-home");
    cy.contains("HelloWorld Cypress").should("exist");
    cy.contains("HelloWorld Cypress").click();
    cy.url().should("include", "/helloworld-mfe");
    cy.wait("@remoteEntry");
    cy.get(MFE_CONTAINER_CSS_SELECTOR).should("exist");
    cy.get(MFE_CONTAINER_CSS_SELECTOR).contains(
      "HelloWorld Cypress MFE Mounted",
    );
  });
});
