'use strict';

module.exports = {
  // Valid use-case: Required addon, is already at latest version, but depends on an older
  // dependency. PS create an add-on PR, if possible, to fix the issue first
  allowedVersions: {
    'ember-compatibility-helpers': '0.1.3 || 1.0.0-beta.1 || ^1.0.0',
    'ember-getowner-polyfill': '1.2.5 || ^2.2.0', //https://github.com/asross/dynamic-link/pull/10
    'ember-fetch': '^6.0.0 || 5.1.1', //https://github.com/simplabs/ember-simple-auth/issues/1705,
    'ember-inflector': '^3.0.0 || 2.3.0',
    'ember-maybe-in-element': '0.1.3 || 0.2.0',
    'ember-sinon': '2.2.0 || ^3.1.0' // TODO: META-8261 bump some sub deps
  }
};
