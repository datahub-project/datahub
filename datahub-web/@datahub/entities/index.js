'use strict';
const buildCliOptionsFor = require('../../configs/ember-cli-build-options');
const EmberApp = require('ember-cli/lib/broccoli/ember-app');
const { options } = buildCliOptionsFor(EmberApp.env());

module.exports = {
  name: require('./package').name,
  isDevelopingAddon: () => true,
  ...require('../../configs/import-mirage-tree-from-addon'),

  options: {
    ace: options.ace
  }
};
