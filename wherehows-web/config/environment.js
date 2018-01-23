'use strict';

module.exports = function(environment) {
  const ENV = {
    modulePrefix: 'wherehows-web',
    environment,
    rootURL: '/',
    locationType: 'hash',
    EmberENV: {
      FEATURES: {
        // Here you can enable experimental features on an ember canary build
        // e.g. 'with-controller': true
      },
      EXTEND_PROTOTYPES: {
        // Prevent Ember Data from overriding Date.parse.
        Date: false
      }
    },

    APP: {
      // Here you can pass flags/options to your application instance
      // when it is created
    },

    eyeglass: {
      cssDir: 'assets'
    },

    moment: {
      allowEmpty: true,
      outputFormat: 'llll' // fallback format e.g. Thu, Sep 21 1984 8:30 PM
    },

    // Since ember-metrics automatically removes all unused adapters, which
    //   will happen because we are using lazy initialization for API keys
    //   and not specifying adapter props at build time, the ffg forces the
    //   inclusion of the adapter's we currently support.
    'ember-metrics': {
      includeAdapters: ['piwik']
    },

    // ember-aupac-typeahead is dependent on twitter typeahead
    //   unfortunately, there are several issues with the library since it's
    //   also no longer maintained including failing to render async results
    //   due to incorrectly specified order of operations.
    //   https://github.com/twitter/typeahead.js/pull/1212
    //   The PR above had not been merged at the time of this writing.
    //   Created a PR to fix this in ember-aupac
    //   https://github.com/aupac/ember-aupac-typeahead/pull/46

    //   This disables the default dep on twitter/typeahead
    'ember-aupac-typeahead': {
      includeTypeahead: false
    }
  };

  if (environment === 'development') {
    // ENV.APP.LOG_RESOLVER = true;
    ENV.APP.LOG_ACTIVE_GENERATION = true;
    ENV.APP.LOG_TRANSITIONS = true;
    ENV.APP.LOG_TRANSITIONS_INTERNAL = true;
    ENV.APP.LOG_VIEW_LOOKUPS = true;
    ENV['ember-cli-mirage'] = {
      enabled: false
    };
  }

  if (environment === 'test') {
    // Testem prefers this...
    ENV.locationType = 'none';

    // keep test console output quieter
    ENV.APP.LOG_ACTIVE_GENERATION = false;
    ENV.APP.LOG_VIEW_LOOKUPS = false;

    ENV.APP.rootElement = '#ember-testing';
    ENV.APP.autoboot = false;
  }

  if (environment === 'production') {
    ENV.rootURL = '/assets';
  }

  return ENV;
};
