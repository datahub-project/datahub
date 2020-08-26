'use strict';

module.exports = {
  // Valid use-case: Required addon, is already at latest version, but depends on an older
  // dependency. PS create an add-on PR, if possible, to fix the issue first
  allowedVersions: {
    'ember-compatibility-helpers': '0.1.3 || 1.0.0-beta.1 || ^1.0.0',
    'ember-fetch': '^6.0.0 || 7.0.0', //https://github.com/simplabs/ember-simple-auth/issues/1705,
    'ember-inflector': '^3.0.0 || 2.3.0',
    'ember-maybe-in-element': '0.4.0 || 0.2.0',
    'ember-lodash': '4.18.0 || 4.19.4',
    'ember-sinon': '2.2.0 || ^3.1.0', // TODO: META-8261 bump some sub deps
    'ember-cli-typescript': '2.0.2 || ^3.0.0', // pending upgrade to typescript in ember-nacho,
    'ember-power-select-typeahead': '0.7.4', // pending upgrade to typescript in ember-nacho
    'ember-power-select': '2.3.5', // pending upgrade in ember-nacho
    'ember-basic-dropdown': '1.1.3' // pending upgrade in ember-nacho
  }
};
