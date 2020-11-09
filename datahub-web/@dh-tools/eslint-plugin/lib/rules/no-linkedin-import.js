/**
 * @fileoverview
 */
'use strict';

//------------------------------------------------------------------------------
// Rule Definition
//------------------------------------------------------------------------------
module.exports = {
  meta: {
    docs: {
      description: 'Open source modules should not import from @linkedin',
      category: 'Open source modules should not import from @linkedin',
      recommended: true
    },
    fixable: null,
    schema: []
  },

  create: function(context) {
    return {
      ImportDeclaration(node) {
        if (node.source.value.indexOf('@linkedin') >= 0) {
          context.report(node, 'Open source modules should not import from @linkedin');
        }
      }
    };
  }
};
