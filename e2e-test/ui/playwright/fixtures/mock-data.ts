/**
 * @deprecated
 *
 * Static mock data should live in a feature's own data directory:
 *   tests/{feature}/data/{feature}.json
 *
 * This file is kept temporarily to avoid breaking any remaining references.
 * Remove it once all test files have been updated to load data via the
 * `featureData` fixture provided by base-test.ts.
 */

export const mockDataset = {
  urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,test.dataset,PROD)',
  name: 'test.dataset',
  description: 'Test dataset for E2E testing',
  platform: 'hive',
  properties: {
    name: 'test.dataset',
    description: 'Test dataset',
  },
};

export const mockUser = {
  username: 'test-user',
  email: 'test@example.com',
  firstName: 'Test',
  lastName: 'User',
};

export const mockSearchResults = {
  searchResults: [
    {
      entity: mockDataset,
      matchedFields: [],
    },
  ],
  total: 1,
};

export const mockBusinessAttribute = {
  urn: 'urn:li:businessAttribute:test-attribute',
  name: 'Test Attribute',
  description: 'Test business attribute',
};
