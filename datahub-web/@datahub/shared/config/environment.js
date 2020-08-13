'use strict';

module.exports = function(/* environment, appConfig */) {
  return {
    // Since ember-metrics automatically removes all unused adapters, which
    //   will happen because we are using lazy initialization for API keys
    //   and not specifying adapter props at build time, the ffg forces the
    //   inclusion of the adapter's we currently support.
    'ember-metrics': {
      includeAdapters: ['piwik']
    }
  };
};
