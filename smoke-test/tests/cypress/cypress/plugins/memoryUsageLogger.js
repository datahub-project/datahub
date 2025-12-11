/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

/**
 * @type {Cypress.PluginConfig}
 */
// eslint-disable-next-line no-unused-vars
module.exports = (on, config) => {
  // Add a task to log memory usage to the terminal
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
