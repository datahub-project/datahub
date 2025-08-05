module.exports = {
  env: {
    browser: true,
    es2021: true,
    node: true,
  },
  extends: ["airbnb-base", "plugin:playwright/playwright-test", "prettier"],
  parser: "@typescript-eslint/parser",
  parserOptions: {
    ecmaVersion: "latest",
    sourceType: "module",
  },
  plugins: ["playwright", "@typescript-eslint"],
  rules: {
    "import/extensions": "off",
    "import/no-unresolved": "off",
    "no-console": "off",
    "import/prefer-default-export": "off",
    "import/no-extraneous-dependencies": ["error", { devDependencies: true }],
    "no-shadow": "off",
    "@typescript-eslint/no-shadow": "error",
    "@typescript-eslint/no-unused-vars": "error",
    "no-unused-vars": "off",
    "no-param-reassign": ["error", { props: false }],
    "playwright/no-wait-for-timeout": "warn",
  },
  overrides: [
    {
      files: ["**/*.ts", "**/*.tsx"],
      rules: {
        "no-undef": "off", // TypeScript handles this
      },
    },
  ],
};
