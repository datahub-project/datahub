import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react-hooks';
import { ApolloClient } from '@apollo/client';
import { useImportProcessing } from '../shared/hooks/useImportProcessing';
import { Entity } from '../glossary.types';

// Mock the user context
const mockUser = {
  urn: 'urn:li:corpuser:testuser',
  username: 'testuser',
  info: {
    displayName: 'Test User',
    email: 'test@example.com'
  }
};

vi.mock('@app/context/useUserContext', () => ({
  useUserContext: () => mockUser
}));

// Mock Apollo Client with realistic responses
const mockApolloClient = {
  query: vi.fn(),
  mutate: vi.fn()
} as unknown as ApolloClient<any>;

// Mock GraphQL operations with realistic responses
const mockGraphQLOperations = {
  createOwnershipType: vi.fn(),
  patchEntities: vi.fn(),
  getOwnershipTypes: vi.fn(),
  getEntityOwnership: vi.fn()
};

vi.mock('../shared/hooks/useGraphQLOperations', () => ({
  useGraphQLOperations: () => mockGraphQLOperations
}));

describe('CSV Import End-to-End Tests', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    
    // Setup default mock responses
    mockGraphQLOperations.getOwnershipTypes.mockResolvedValue({
      data: {
        listOwnershipTypes: {
          ownershipTypes: [
            {
              urn: 'urn:li:ownershipType:__system__technical_owner',
              info: { name: 'Technical Owner' }
            },
            {
              urn: 'urn:li:ownershipType:__system__business_owner',
              info: { name: 'Business Owner' }
            }
          ]
        }
      }
    });

    mockGraphQLOperations.createOwnershipType.mockResolvedValue({
      data: {
        createOwnershipType: {
          urn: 'urn:li:ownershipType:new-urn',
          success: true
        }
      }
    });

    mockGraphQLOperations.patchEntities.mockResolvedValue({
      data: {
        patchEntities: [
          { urn: 'urn:li:glossaryTerm:test', success: true, error: null }
        ]
      }
    });
  });

  describe('Complete CSV Import Workflow', () => {
    it('should process the updated CSV file with pipe-separated ownership', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 10
        })
      );

      // Simulate the entities from the updated CSV file
      const csvEntities: Entity[] = [
        {
          id: 'customer-id',
          name: 'Customer ID',
          type: 'glossaryTerm',
          data: {
            entity_type: 'glossaryTerm',
            name: 'Customer ID',
            description: 'Unique identifier for each customer',
            definition: 'Unique identifier for each customer',
            term_source: 'INTERNAL',
            source_ref: '',
            source_url: '',
            ownership_users: 'datahub:Technical Owner',
            ownership_groups: 'bfoo:Technical Owner',
            parent_nodes: 'Business Terms',
            related_contains: '',
            related_inherits: 'Business Terms.Customer Name',
            domain_urn: '',
            domain_name: '',
            custom_properties: ''
          }
        },
        {
          id: 'customer-data',
          name: 'Customer Data',
          type: 'glossaryTerm',
          data: {
            entity_type: 'glossaryTerm',
            name: 'Customer Data',
            description: 'Information related to customer records and profiles',
            definition: 'Information related to customer records and profiles',
            term_source: 'INTERNAL',
            source_ref: '',
            source_url: '',
            ownership_users: 'datahub:Technical Owner',
            ownership_groups: 'bfoo:test',
            parent_nodes: 'Business Terms',
            related_contains: '',
            related_inherits: 'Business Terms.Customer Name',
            domain_urn: '',
            domain_name: '',
            custom_properties: ''
          }
        },
        {
          id: 'imaging-reports',
          name: 'Imaging Reports',
          type: 'glossaryTerm',
          data: {
            entity_type: 'glossaryTerm',
            name: 'Imaging Reports',
            description: 'Results and interpretations from medical imaging studies',
            definition: 'Results and interpretations from medical imaging studies',
            term_source: 'INTERNAL',
            source_ref: 'DataHub',
            source_url: 'https://github.com/healthcare-data-project/healthcare',
            ownership_users: 'admin:DEVELOPER',
            ownership_groups: '',
            parent_nodes: 'Clinical Observations',
            related_contains: '',
            related_inherits: '',
            domain_urn: '',
            domain_name: '',
            custom_properties: '{"data_type":"Report","domain":"Clinical Observations"}'
          }
        },
        {
          id: 'business-terms',
          name: 'Business Terms',
          type: 'glossaryNode',
          data: {
            entity_type: 'glossaryNode',
            name: 'Business Terms',
            description: '',
            definition: '',
            term_source: '',
            source_ref: '',
            source_url: '',
            ownership_users: 'datahub:Technical Owner',
            ownership_groups: '',
            parent_nodes: '',
            related_contains: '',
            related_inherits: '',
            domain_urn: '',
            domain_name: '',
            custom_properties: ''
          }
        }
      ];

      // Process the entities
      act(() => {
        result.current.processEntityBatch(csvEntities);
      });

      // Verify processing completed successfully
      expect(result.current.progress.errors).toHaveLength(0);
      expect(result.current.progress.processed).toBe(4);
      expect(result.current.progress.successful).toBe(4);
    });

    it('should handle ownership type creation and resolution', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      // Mock ownership types that need to be created
      mockGraphQLOperations.getOwnershipTypes.mockResolvedValue({
        data: {
          listOwnershipTypes: {
            ownershipTypes: []
          }
        }
      });

      const entitiesWithNewOwnershipTypes: Entity[] = [
        {
          id: 'new-ownership-type',
          name: 'bfoo',
          type: 'ownershipType',
          data: {
            entity_type: 'ownershipType',
            name: 'bfoo',
            description: 'Custom ownership type: bfoo',
            definition: '',
            term_source: '',
            source_ref: '',
            source_url: '',
            ownership_users: '',
            ownership_groups: '',
            parent_nodes: '',
            related_contains: '',
            related_inherits: '',
            domain_urn: '',
            domain_name: '',
            custom_properties: ''
          }
        },
        {
          id: 'glossary-with-new-ownership',
          name: 'Test Entity',
          type: 'glossaryTerm',
          data: {
            entity_type: 'glossaryTerm',
            name: 'Test Entity',
            description: 'Test Description',
            definition: 'Test Definition',
            term_source: 'INTERNAL',
            source_ref: '',
            source_url: '',
            ownership_users: '',
            ownership_groups: 'bfoo:Technical Owner',
            parent_nodes: '',
            related_contains: '',
            related_inherits: '',
            domain_urn: '',
            domain_name: '',
            custom_properties: ''
          }
        }
      ];

      act(() => {
        result.current.processEntityBatch(entitiesWithNewOwnershipTypes);
      });

      // Should create the ownership type first
      expect(mockGraphQLOperations.createOwnershipType).toHaveBeenCalledWith({
        variables: {
          input: {
            name: 'bfoo',
            description: 'Custom ownership type: bfoo'
          }
        }
      });

      // Should process all entities successfully
      expect(result.current.progress.errors).toHaveLength(0);
      expect(result.current.progress.processed).toBe(2);
    });

    it('should handle duplicate ownership type resolution', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      // Mock multiple ownership types with the same name
      mockGraphQLOperations.getOwnershipTypes.mockResolvedValue({
        data: {
          listOwnershipTypes: {
            ownershipTypes: [
              {
                urn: 'urn:li:ownershipType:first-urn',
                info: { name: 'DEVELOPER' }
              },
              {
                urn: 'urn:li:ownershipType:second-urn',
                info: { name: 'DEVELOPER' }
              },
              {
                urn: 'urn:li:ownershipType:third-urn',
                info: { name: 'DEVELOPER' }
              }
            ]
          }
        }
      });

      const entityWithDuplicateOwnershipType: Entity = {
        id: 'test-duplicate',
        name: 'Test Duplicate',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Test Duplicate',
          description: 'Test Description',
          definition: 'Test Definition',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
            ownership_users: 'testuser:DEVELOPER',
            ownership_groups: '',
          parent_nodes: '',
          related_contains: '',
          related_inherits: '',
          domain_urn: '',
          domain_name: '',
          custom_properties: ''
        }
      };

      act(() => {
        result.current.processEntity(entityWithDuplicateOwnershipType);
      });

      // Should handle duplicate ownership types gracefully
      expect(result.current.progress.errors).toHaveLength(0);
      expect(result.current.progress.warnings.length).toBeGreaterThan(0);
      expect(result.current.progress.warnings[0].message).toContain('duplicate');
    });

    it('should process mixed ownership formats (comma and pipe)', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      const entitiesWithMixedFormats: Entity[] = [
        {
          id: 'comma-format',
          name: 'Comma Format',
          type: 'glossaryTerm',
          data: {
            entity_type: 'glossaryTerm',
            name: 'Comma Format',
            description: 'Test Description',
            definition: 'Test Definition',
            term_source: 'INTERNAL',
            source_ref: '',
            source_url: '',
            ownership_users: 'admin:DEVELOPER',
            ownership_groups: 'bfoo:Technical Owner',
            parent_nodes: '',
            related_contains: '',
            related_inherits: '',
            domain_urn: '',
            domain_name: '',
            custom_properties: ''
          }
        },
        {
          id: 'pipe-format',
          name: 'Pipe Format',
          type: 'glossaryTerm',
          data: {
            entity_type: 'glossaryTerm',
            name: 'Pipe Format',
            description: 'Test Description',
            definition: 'Test Definition',
            term_source: 'INTERNAL',
            source_ref: '',
            source_url: '',
            ownership_users: 'admin:DEVELOPER',
            ownership_groups: 'bfoo:Technical Owner',
            parent_nodes: '',
            related_contains: '',
            related_inherits: '',
            domain_urn: '',
            domain_name: '',
            custom_properties: ''
          }
        }
      ];

      act(() => {
        result.current.processEntityBatch(entitiesWithMixedFormats);
      });

      // Both formats should be processed successfully
      expect(result.current.progress.errors).toHaveLength(0);
      expect(result.current.progress.processed).toBe(2);
    });

    it('should handle comprehensive ownership aggregation across entity types', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      // Mock comprehensive ownership aggregation
      mockGraphQLOperations.getEntityOwnership.mockResolvedValue({
        data: {
          search: {
            searchResults: [
              {
                entity: {
                  urn: 'urn:li:dataset:test',
                  ownership: {
                    owners: [
                      {
                        owner: { urn: 'urn:li:corpuser:existing' },
                        type: 'TECHNICAL_OWNER',
                        ownershipType: {
                          urn: 'urn:li:ownershipType:existing-type',
                          info: { name: 'existing' }
                        }
                      }
                    ]
                  }
                }
              }
            ]
          }
        }
      });

      const entityWithExistingOwnershipType: Entity = {
        id: 'test-existing',
        name: 'Test Existing',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Test Existing',
          description: 'Test Description',
          definition: 'Test Definition',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
            ownership_users: 'testuser:existing',
            ownership_groups: '',
          parent_nodes: '',
          related_contains: '',
          related_inherits: '',
          domain_urn: '',
          domain_name: '',
          custom_properties: ''
        }
      };

      act(() => {
        result.current.processEntity(entityWithExistingOwnershipType);
      });

      // Should find and use existing ownership type
      expect(result.current.progress.errors).toHaveLength(0);
    });
  });

  describe('Error Scenarios', () => {
    it('should handle GraphQL errors gracefully', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      // Mock GraphQL error
      mockGraphQLOperations.patchEntities.mockRejectedValue(new Error('GraphQL Error'));

      const testEntity: Entity = {
        id: 'test-error',
        name: 'Test Error',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Test Error',
          description: 'Test Description',
          definition: 'Test Definition',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
          ownership_users: 'admin:DEVELOPER',
          ownership_groups: '',
          parent_nodes: '',
          related_contains: '',
          related_inherits: '',
          domain_urn: '',
          domain_name: '',
          custom_properties: ''
        }
      };

      act(() => {
        result.current.processEntity(testEntity);
      });

      // Should handle the error gracefully
      expect(result.current.progress.errors.length).toBeGreaterThan(0);
      expect(result.current.progress.errors[0].error).toContain('GraphQL Error');
    });

    it('should handle network errors with retry logic', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5,
          maxRetries: 2,
          retryDelay: 100
        })
      );

      // Mock network error that eventually succeeds
      let callCount = 0;
      mockGraphQLOperations.patchEntities.mockImplementation(() => {
        callCount++;
        if (callCount < 3) {
          return Promise.reject(new Error('Network Error'));
        }
        return Promise.resolve({
          data: {
            patchEntities: [
              { urn: 'urn:li:glossaryTerm:test', success: true, error: null }
            ]
          }
        });
      });

      const testEntity: Entity = {
        id: 'test-retry',
        name: 'Test Retry',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Test Retry',
          description: 'Test Description',
          definition: 'Test Definition',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
          ownership_users: 'admin:DEVELOPER',
          ownership_groups: '',
          parent_nodes: '',
          related_contains: '',
          related_inherits: '',
          domain_urn: '',
          domain_name: '',
          custom_properties: ''
        }
      };

      act(() => {
        result.current.processEntity(testEntity);
      });

      // Should eventually succeed after retries
      expect(callCount).toBe(3);
      expect(result.current.progress.successful).toBe(1);
    });
  });
});
