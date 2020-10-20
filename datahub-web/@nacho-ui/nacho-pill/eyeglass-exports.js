/* eslint-env node */

'use strict';

const path = require('path');

module.exports = function eyeglassExports(/* eyeglass, sass */) {
  return {
    sassDir: path.join(__dirname, 'addon', 'styles')
  };
};
