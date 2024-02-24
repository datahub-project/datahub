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
    "cypress/no-unnecessary-waiting": "warn",
  },
};
