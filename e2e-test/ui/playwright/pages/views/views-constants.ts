// Test data
export const TEST_DATA = {
  FILTER_FIELD: 'fieldGlossaryTerms',
  FILTER_OPERATOR: 'equals',
  FILTER_VALUE: 'PlaywrightViewsTestTerm',
  EXPECTED_RESULT_COUNT: 2,
  EXPECTED_DATASET: 'PlaywrightViewsTest1',
} as const;

// URLs
export const URLS = {
  SEARCH: '/search?query=%2A',
  SETTINGS_VIEWS: '/settings/views',
} as const;

// Text patterns
export const TEXT_PATTERNS = {
  RESULTS_COUNT: (count: number) => `of ${count} results`,
} as const;
