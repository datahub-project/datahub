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
