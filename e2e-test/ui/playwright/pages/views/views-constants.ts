// Test data
export const TEST_DATA = {
  FILTER_FIELD: 'fieldGlossaryTerms',
  FILTER_OPERATOR: 'equals',
  FILTER_VALUE: 'PlaywrightColumnInfoType',
  EXPECTED_RESULT_COUNT: 3,
  EXPECTED_DATASET: 'Playwright_logging_events',
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
