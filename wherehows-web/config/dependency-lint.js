'use strict';

module.exports = {
  // Valid use-case: Required addon, is already at latest version, but depends on an older
  // dependency. PS create an add-on PR, if possible, to fix the issue first
  allowedVersions: {
    'ember-concurrency': '^0.8.18', //https://github.com/cibernox/ember-power-calendar/pull/135
    'ember-compatibility-helpers': '0.1.3 || 1.0.0-beta.1 || ^1.0.0',
    'ember-getowner-polyfill': '1.2.5 || ^2.2.0', //https://github.com/asross/dynamic-link/pull/10
  }
};
