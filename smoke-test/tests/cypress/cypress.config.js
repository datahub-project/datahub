// eslint-disable-next-line global-require
const { defineConfig } = require("cypress");

module.exports = defineConfig({
  chromeWebSecurity: false,
  viewportHeight: 960,
  viewportWidth: 1536,
  projectId: "s6gjkt",
  defaultCommandTimeout: 10000,
  retries: {
    runMode: 5,
    openMode: 0,
  },
  video: false,
  env: {
    ADMIN_USERNAME: "datahub",
    ADMIN_PASSWORD: "datahub",
  },
  e2e: {
    // We've imported your old cypress plugins here.
    // You may want to clean this up later by importing these.
    setupNodeEvents(on, config) {
      // Must be registered first so it can attach after:spec / after:run hooks before
      // other plugins consume the same events.
      // eslint-disable-next-line global-require
      require("cypress-mochawesome-reporter/plugin")(on);
      // eslint-disable-next-line global-require
      return require("./cypress/plugins/index")(on, config);
    },
    baseUrl: "http://localhost:9002/",
    specPattern: "cypress/e2e/**/*.{js,jsx,ts,tsx}",
    experimentalStudio: true,
    experimentalMemoryManagement: true,
    numTestsKeptInMemory: 0,
  },
  reporter: "cypress-multi-reporters",
  reporterOptions: {
    reporterEnabled: "cypress-mochawesome-reporter, cypress-junit-reporter",
    cypressMochawesomeReporterReporterOptions: {
      // cypress-mochawesome-reporter hooks into Cypress after:spec (not mocha end),
      // writing one JSON per spec as it completes. Plain mochawesome only writes at
      // the end of the full run, so a mid-run Electron crash produces no output at all.
      reportDir: "build/mochawesome-report",
      overwrite: false,
      html: false,
      json: true,
      embeddedScreenshots: true,
    },
    cypressJunitReporterReporterOptions: {
      mochaFile: "build/smoke-test-results/cypress-test-[hash].xml",
      toConsole: true,
      testCaseSwitchClassnameAndName: true,
      suiteNameTemplate: "{dirpath}",
      classNameTemplate: "{filepath}",
    },
  },
});
