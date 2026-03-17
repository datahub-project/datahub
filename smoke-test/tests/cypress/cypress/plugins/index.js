/// <reference types="cypress" />
// ***********************************************************
// This example plugins/index.js can be used to load plugins
//
// You can change the location of this file or turn off loading
// the plugins file with the 'pluginsFile' configuration option.
//
// You can read more here:
// https://on.cypress.io/plugins-guide
// ***********************************************************

// This function is called when a project is opened or re-opened (e.g. due to
// the project's config changing)

/**
 * @type {Cypress.PluginConfig}
 */
// eslint-disable-next-line no-unused-vars
module.exports = (on, config) => {
  // `on` is used to hook into various events Cypress emits
  // `config` is the resolved Cypress config

  // eslint-disable-next-line global-require
  require("./memoryUsageLogger")(on);

  // eslint-disable-next-line global-require
  require("cypress-timestamps/plugin")(on);

  // Prevent Electron renderer process OOM crashes in CI by disabling GPU
  // acceleration, relaxing sandbox restrictions, and raising the V8 heap limit.
  on("before:browser:launch", (browser, launchOptions) => {
    if (browser.name === "electron") {
      launchOptions.args.push("--disable-dev-shm-usage");
      launchOptions.args.push("--disable-gpu");
      launchOptions.args.push("--no-sandbox");
      launchOptions.args.push("--js-flags=--max-old-space-size=8192");
    }
    return launchOptions;
  });
};
