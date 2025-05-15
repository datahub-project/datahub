declare module "cypress-timestamps/plugin" {
  const plugin: (on: Cypress.PluginEvents) => void;
  export default plugin;
}
