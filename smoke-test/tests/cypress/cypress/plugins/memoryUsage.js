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

  // Add a task to log to the terminal
  on("task", {
    logMemoryUsage(browserMemoryUsage) {
      const formatBytes = (bytes) =>
        (bytes / 1024 / 1024 / 1024).toFixed(3) + " GB";

      if (browserMemoryUsage) {
        console.log("=== Browser Tab Memory Usage ===");
        console.log(`Used: ${formatBytes(browserMemoryUsage.usedJSHeapSize)}`);
        console.log(
          `Total: ${formatBytes(browserMemoryUsage.totalJSHeapSize)}`,
        );
        console.log(
          `Limit: ${formatBytes(browserMemoryUsage.jsHeapSizeLimit)}`,
        );
      }

      const cypressMemoryUsage = process.memoryUsage();
      console.log("=== Cypress Memory Usage ===");
      console.log(`rss: ${formatBytes(cypressMemoryUsage.rss)}`);
      console.log(`heapTotal: ${formatBytes(cypressMemoryUsage.heapTotal)}`);
      console.log(`heapUsed: ${formatBytes(cypressMemoryUsage.heapUsed)}`);
      console.log(`external: ${formatBytes(cypressMemoryUsage.external)}`);
      console.log(
        `arrayBuffers: ${formatBytes(cypressMemoryUsage.arrayBuffers)}`,
      );

      return null;
    },
  });
};
