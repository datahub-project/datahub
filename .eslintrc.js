module.exports = {
  "extends": "eslint:recommended",
  "parserOptions": {
    "ecmaVersion": 6,
    "sourceType": "module"
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
