'use strict';

module.exports = {
  name: require('./package').name,
  isDevelopingAddon: () => false,
  options: {
    ace: {
      modes: ['json'],
      workers: ['json'],
      exts: ['searchbox']
    }
  }
};
