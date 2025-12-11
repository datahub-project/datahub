/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
  reporter: "cypress-junit-reporter",
  reporterOptions: {
    mochaFile: "build/smoke-test-results/cypress-test-[hash].xml",
    toConsole: true,
    testCaseSwitchClassnameAndName: true,
    suiteNameTemplate: "{dirpath}",
    classNameTemplate: "{filepath}",
  },
});
