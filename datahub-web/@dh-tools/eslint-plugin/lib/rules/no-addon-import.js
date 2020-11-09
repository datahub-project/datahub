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
      description: 'Imports should not have /addon/ for ember addons as ember wont compile and it is not needed',
      category: 'Addon module import paths should not have /addon/. This can be safely removed',
      recommended: true
    },
    fixable: null,
    schema: []
  },

  create: function(context) {
    return {
      ImportDeclaration(node) {
        if (node.source.value.indexOf('/addon/') >= 0) {
          context.report(node, 'Addon module import paths should not have /addon/. This can be safely removed');
        }
      }
    };
  }
};
