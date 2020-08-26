'use strict';

const mirageAddon = require('../../configs/import-mirage-tree-from-addon');
const buildCliOptionsFor = require('../../configs/ember-cli-build-options');
const EmberApp = require('ember-cli/lib/broccoli/ember-app');
const { options } = buildCliOptionsFor(EmberApp.env());

module.exports = {
  name: require('./package').name,
  isDevelopingAddon: () => true,
  options: {
    ace: options.ace,
    autoImport: options.autoImport
  },
  ...mirageAddon,
  included(app) {
    // Mirage Addon will include _super call
    mirageAddon.included.call(this, app);
    app.import('node_modules/@aduh95/viz.js/dist/render.wasm', { destDir: 'assets/viz.js' });
    app.import('node_modules/@aduh95/viz.js/dist/render.browser.js', { outputFile: 'assets/viz.js/render.browser.js' });
  }
};
