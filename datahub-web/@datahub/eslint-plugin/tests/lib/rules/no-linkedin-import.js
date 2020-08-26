/**
 * @fileoverview Imports should not containt /addon/
 * @author Ignacio
 */
'use strict';

//------------------------------------------------------------------------------
// Requirements
//------------------------------------------------------------------------------

const rule = require('../../../lib/rules/no-linkedin-import'),
  RuleTester = require('eslint').RuleTester;

RuleTester.setDefaultConfig({
  parser: 'babel-eslint'
});
//------------------------------------------------------------------------------
// Tests
//------------------------------------------------------------------------------

const ruleTester = new RuleTester();
ruleTester.run('no-linkedin-import', rule, {
  valid: [
    {
      code: "import aa from '@datahub/somethingelse'"
    }
  ],

  invalid: [
    {
      code: "import aa from '@linkedin/something'",
      errors: [
        {
          message: 'Open source modules should not import from @linkedin',
          type: 'ImportDeclaration'
        }
      ]
    }
  ]
});
