module.exports = {
  globals: {
    server: true,
    define: true,
  },
  "extends": [
    "eslint:recommended",
    "prettier"
  ],
  "parser": "babel-eslint",
  "parserOptions": {
    "ecmaVersion": 8,
    "sourceType": "module",
    "ecmaFeatures": {
      "experimentalObjectRestSpread": true
    }
  },
  "env": {
    "browser": true,
    "es6": true,
    "node": true,
    "embertest": true
  },
  "rules": {
    "no-implicit-globals": [
      1
    ],
    "prettier/prettier": ["error", { "singleQuote": true, "printWidth": 120 }]
  },
  "plugins": [
    "prettier"
  ]
};
