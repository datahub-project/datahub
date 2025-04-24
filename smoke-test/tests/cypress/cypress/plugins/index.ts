import { defineConfig } from "cypress";
import timestamps from "cypress-timestamps/plugin";

export default (
  on: Cypress.PluginEvents,
  config: Cypress.PluginConfigOptions,
) => {
  timestamps(on);
  return config;
};
