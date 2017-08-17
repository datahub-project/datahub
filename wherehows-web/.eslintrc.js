module.exports = {
  globals: {
    server: true,
  },
  "extends": "eslint:recommended",
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
    "embertest": true
  },
  "rules": {
    "no-implicit-globals": [
      1
    ]
  }
};
