export const selectors = {
  // Auth
  login: {
    usernameInput: 'input[name="username"]',
    passwordInput: 'input[name="password"]',
    submitButton: 'button[type="submit"]',
  },

  // Search
  search: {
    input: '[data-testid="search-input"]',
    button: '[data-testid="search-button"]',
    results: '[data-testid="search-results"]',
    resultItem: '[data-testid="search-result-item"]',
  },

  // Dataset
  dataset: {
    name: '[data-testid="dataset-name"]',
    schemaTab: '[data-testid="schema-tab"]',
    lineageTab: '[data-testid="lineage-tab"]',
    propertiesTab: '[data-testid="properties-tab"]',
  },

  // Business Attributes
  businessAttribute: {
    createButton: '[data-testid="create-attribute-button"]',
    nameInput: '[data-testid="attribute-name-input"]',
    descriptionInput: '[data-testid="attribute-description-input"]',
    saveButton: '[data-testid="save-attribute-button"]',
  },

  // Common
  common: {
    sidebar: '[data-testid="sidebar"]',
    header: '[data-testid="header"]',
    loading: '[data-testid="loading"]',
    error: '[data-testid="error"]',
  },
};
