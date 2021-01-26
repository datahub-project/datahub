'use strict';

/* eslint-disable @typescript-eslint/no-var-requires */
const osEnvironment = require('./environment');
/* eslint-disable @typescript-eslint/no-var-requires */

module.exports = function(/* environment, appConfig */) {
  const osEnvironmenConfs = osEnvironment();
  return {
    // Since ember-metrics automatically removes all unused adapters, which
    //   will happen because we are using lazy initialization for API keys
    //   and not specifying adapter props at build time, the ffg forces the
    //   inclusion of the adapter's we currently support.
    'ember-metrics': {
      includeAdapters: [...osEnvironmenConfs['ember-metrics'].includeAdapters, 'li-tracking']
    }
  };
};
