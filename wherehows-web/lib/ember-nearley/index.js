/* eslint-env node */
'use strict';
const BroccoliNearley = require('./broccoli-nearley');

module.exports = {
  name: 'ember-nearley',

  isDevelopingAddon() {
    return false;
  },

  setupPreprocessorRegistry(type, registry) {
    registry.add('js', {
      name: 'ember-nearley',
      toTree(tree) {
        return new BroccoliNearley(tree);
      }
    });

    if (type === 'parent') {
      this.parentRegistry = registry;
    }
  }
};
