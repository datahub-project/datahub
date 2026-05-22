export const TIMEOUTS = {
  NORMAL: 10000,
  LONG: 15000,
  SHORT: 5000,
  NETWORK_IDLE: 3000,
} as const;

export const WAITS = {
  INPUT_DEBOUNCE: 1500,
  UI_ANIMATION: 300,
  OPERATION: 2000,
  SEARCH: 3000,
} as const;

export const URLS = {
  CONTEXT_DOCUMENTS: '/context/documents',
  HOME: '/',
  DOCUMENT: (urn: string) => `/document/${encodeURIComponent(urn)}`,
} as const;
