'use strict';

module.exports = {
  name: require('./package').name,

  isDevelopingAddon: () => true,

  ...require('../../configs/import-mirage-tree-from-addon')
};
