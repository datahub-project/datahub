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
    reporterEnabled: "cypress-junit-reporter, mochawesome",
    cypressJunitReporterReporterOptions: {
      mochaFile: "build/smoke-test-results/cypress-test-[hash].xml",
      toConsole: true,
      testCaseSwitchClassnameAndName: true,
      suiteNameTemplate: "{dirpath}",
      classNameTemplate: "{filepath}",
    },
    mochawesomeReporterOptions: {
      // Write one JSON file per spec so partial results survive an Electron crash
      reportDir: "build/mochawesome-report",
      overwrite: false,
      html: false,
      json: true,
    },
  },
});
