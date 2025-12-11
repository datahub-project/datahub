/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

module.exports = {
  env: {
    es2021: true,
    node: true,
  },
  plugins: ["cypress"],
  extends: ["airbnb-base", "plugin:cypress/recommended", "prettier"],
  overrides: [
    {
      env: {
        node: true,
      },
      files: [".eslintrc.{js,cjs}"],
      parserOptions: {
        sourceType: "script",
      },
    },
  ],
  parserOptions: {
    ecmaVersion: "latest",
    sourceType: "module",
  },
  rules: {
    camelcase: "off",
    "import/prefer-default-export": "off",
    // TODO: These should be upgraded to warnings and fixed.
    "cypress/no-unnecessary-waiting": "off",
    "cypress/unsafe-to-chain-command": "off",
    "no-unused-vars": "off",
  },
};
