'use strict';

module.exports = function(environment) {
  const ENV = {
    modulePrefix: 'datahub-web',
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
      useSecureRedirect: true
    },

    eyeglass: {
      cssDir: 'assets'
    },

    fontawesome: {
      icons: {
        'free-solid-svg-icons': 'all',
        'free-regular-svg-icons': 'all',
        'free-brands-svg-icons': 'all'
      }
    },

    moment: {
      allowEmpty: true,
      outputFormat: 'llll' // fallback format e.g. Thu, Sep 21 1984 8:30 PM
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
    ENV.RAISE_ON_DEPRECATION = false;
    ENV.LOG_STACKTRACE_ON_DEPRECATION = false;

    ENV.APP.rootElement = '#ember-testing';
    ENV.APP.autoboot = false;
    ENV.APP.notificationsTimeout = 1;
    ENV['ember-cli-mirage'] = {
      enabled: true,
      autostart: true
    };
  }

  return ENV;
};
