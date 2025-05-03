import { defineConfig } from "cypress";

export default defineConfig({
  chromeWebSecurity: false,
  viewportHeight: 960,
  viewportWidth: 1536,
  projectId: "hkrxk5",
  defaultCommandTimeout: 10000,
  retries: {
    runMode: 2,
    openMode: 0,
  },
  video: false,
  e2e: {
    async setupNodeEvents(on, config) {
      const { default: setupPlugins } = await import(
        "./cypress/plugins/index.js"
      );
      return setupPlugins(on, config);
    },
    baseUrl: "http://localhost:9002/",
    specPattern: "cypress/e2e/**/*.{js,jsx,ts,tsx}",
    experimentalStudio: true,
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
