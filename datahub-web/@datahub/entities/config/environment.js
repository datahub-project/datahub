'use strict';
/* eslint-disable @typescript-eslint/no-var-requires */
const environment = require('../../../configs/environment');
/* eslint-disable @typescript-eslint/no-var-requires */

module.exports = function(/* environment, appConfig */) {
  return environment();
};
