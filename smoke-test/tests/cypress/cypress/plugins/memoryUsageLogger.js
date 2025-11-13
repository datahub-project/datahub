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
