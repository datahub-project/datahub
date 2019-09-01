module.exports = {
  root: true,
  globals: {
    server: true,
    define: true
  },
  extends: [
    'plugin:ember/recommended',
    'plugin:@typescript-eslint/recommended',
    'prettier',
    'prettier/@typescript-eslint'
  ],
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: 2018,
    sourceType: 'module'
  },
  env: {
    browser: true,
    node: true
  },
  rules: {
    'no-implicit-globals': [1],
    'no-console': 'error',
    'no-debugger': 'error',
    'require-await': 'error',
    '@typescript-eslint/class-name-casing': 'error',
    '@typescript-eslint/interface-name-prefix': ['error', 'always'],
    '@typescript-eslint/member-delimiter-style': 'error',
    '@typescript-eslint/no-empty-interface': 'error',
    '@typescript-eslint/no-angle-bracket-type-assertion': 'warn',
    '@typescript-eslint/array-type': ['error', 'generic'],
    '@typescript-eslint/no-non-null-assertion': 'warn',
    '@typescript-eslint/explicit-member-accessibility': 'off',
    '@typescript-eslint/no-parameter-properties': 'off',
    '@typescript-eslint/no-object-literal-type-assertion': 'warn',
    '@typescript-eslint/explicit-function-return-type': 'warn',
    '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
    'prettier/prettier': ['error', { singleQuote: true, printWidth: 120 }],
    'ember/no-ember-super-in-es-classes': 'error'
  },
  plugins: ['prettier', 'ember', '@typescript-eslint'],
  overrides: [
    // node files
    {
      files: [
        '**/.eslintrc.js',
        '**/.template-lintrc.js',
        '**/ember-cli-build.js',
        '**/testem.js',
        '**/blueprints/*/index.js',
        '**/config/**/*.js',
        '**/lib/*/**.js'
      ],
      parserOptions: {
        sourceType: 'script',
        ecmaVersion: 2015
      },
      env: {
        browser: false,
        node: true
      },
      rules: {
        '@typescript-eslint/no-var-requires': 'off'
      }
    }
  ]
};
