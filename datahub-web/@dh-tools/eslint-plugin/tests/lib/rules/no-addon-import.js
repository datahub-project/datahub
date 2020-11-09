/**
 * @fileoverview Imports should not containt /addon/
 * @author Ignacio
 */
'use strict';

//------------------------------------------------------------------------------
// Requirements
//------------------------------------------------------------------------------

const rule = require('../../../lib/rules/no-addon-import'),
  RuleTester = require('eslint').RuleTester;

RuleTester.setDefaultConfig({
  parser: 'babel-eslint'
});
//------------------------------------------------------------------------------
// Tests
//------------------------------------------------------------------------------

const ruleTester = new RuleTester();
ruleTester.run('no-addon-import', rule, {
  valid: [
    {
      code: "import aa from 'someaddon/somethingelse'"
    }
  ],

  invalid: [
    {
      code: "import aa from 'someaddon/addon/'",
      errors: [
        {
          message: 'Addon module import paths should not have /addon/. This can be safely removed',
          type: 'ImportDeclaration'
        }
      ]
    }
  ]
});
