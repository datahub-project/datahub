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
