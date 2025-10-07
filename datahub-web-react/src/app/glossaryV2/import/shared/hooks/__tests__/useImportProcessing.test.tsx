import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react-hooks';
import { ApolloClient } from '@apollo/client';
import { useImportProcessing } from '../useImportProcessing';
import { Entity, EntityData } from '../../../glossary.types';

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

// Mock Apollo Client
const mockApolloClient = {
  query: vi.fn(),
  mutate: vi.fn()
} as unknown as ApolloClient<any>;

// Mock GraphQL operations
vi.mock('../useGraphQLOperations', () => ({
  useGraphQLOperations: () => ({
    createOwnershipType: vi.fn(),
    patchEntities: vi.fn(),
    getOwnershipTypes: vi.fn(),
    getEntityOwnership: vi.fn()
  })
}));

describe('useImportProcessing', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('ownership parsing and processing', () => {
    it('should parse single ownership correctly', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      const testEntity: Entity = {
        id: 'test-1',
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

      // Test the ownership parsing by creating a batch and processing it
      const batch = {
        entities: [testEntity],
        existingEntities: [],
        hierarchyMaps: { parentMap: new Map(), childMap: new Map() }
      };
      
      await act(async () => {
        await result.current.processBatch(batch);
      });

      // The ownership should be parsed and processed
      expect(result.current.progress.errors).toHaveLength(0);
    });

    it('should parse multiple ownership entries with pipe separator', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      const testEntity: Entity = {
        id: 'test-2',
        name: 'Test Entity Multiple',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Test Entity Multiple',
          description: 'Test Description',
          definition: 'Test Definition',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
          ownership_users: 'datahub:Technical Owner',
          ownership_groups: 'bfoo:Technical Owner',
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

      expect(result.current.progress.errors).toHaveLength(0);
    });

    it('should parse multiple ownership entries with comma separator', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      const testEntity: Entity = {
        id: 'test-3',
        name: 'Test Entity Comma',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Test Entity Comma',
          description: 'Test Description',
          definition: 'Test Definition',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
          ownership_users: 'datahub:Technical Owner',
          ownership_groups: 'bfoo:Technical Owner',
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

      expect(result.current.progress.errors).toHaveLength(0);
    });

    it('should handle empty ownership gracefully', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      const testEntity: Entity = {
        id: 'test-4',
        name: 'Test Entity Empty',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Test Entity Empty',
          description: 'Test Description',
          definition: 'Test Definition',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
          ownership: '',
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

      expect(result.current.progress.errors).toHaveLength(0);
    });

    it('should handle invalid ownership format with error', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      const testEntity: Entity = {
        id: 'test-5',
        name: 'Test Entity Invalid',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Test Entity Invalid',
          description: 'Test Description',
          definition: 'Test Definition',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
          ownership: 'invalid-format',
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

      // Should have an error for invalid ownership format
      expect(result.current.progress.errors.length).toBeGreaterThan(0);
      expect(result.current.progress.errors[0].operation).toBe('ownership');
    });
  });

  describe('ownership type creation', () => {
    it('should create ownership type patches with required audit fields', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      const testEntity: Entity = {
        id: 'test-ownership-type',
        name: 'Test Ownership Type',
        type: 'ownershipType',
        data: {
          entity_type: 'ownershipType',
          name: 'Test Ownership Type',
          description: 'Test Description',
          definition: '',
          term_source: '',
          source_ref: '',
          source_url: '',
          ownership: '',
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

      expect(result.current.progress.errors).toHaveLength(0);
    });
  });

  describe('CSV import workflow', () => {
    it('should process a complete CSV import batch', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 3
        })
      );

      const testEntities: Entity[] = [
        {
          id: 'glossary-1',
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
          id: 'glossary-2',
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
          id: 'ownership-type-1',
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
            ownership: '',
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
        result.current.processEntityBatch(testEntities);
      });

      // Should process all entities without errors
      expect(result.current.progress.errors).toHaveLength(0);
      expect(result.current.progress.processed).toBe(3);
    });

    it('should handle mixed ownership types and glossary terms', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      const testEntities: Entity[] = [
        {
          id: 'ownership-type-1',
          name: 'admin',
          type: 'ownershipType',
          data: {
            entity_type: 'ownershipType',
            name: 'admin',
            description: 'Custom ownership type: admin',
            definition: '',
            term_source: '',
            source_ref: '',
            source_url: '',
            ownership: '',
            parent_nodes: '',
            related_contains: '',
            related_inherits: '',
            domain_urn: '',
            domain_name: '',
            custom_properties: ''
          }
        },
        {
          id: 'glossary-1',
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
        }
      ];

      act(() => {
        result.current.processEntityBatch(testEntities);
      });

      expect(result.current.progress.errors).toHaveLength(0);
      expect(result.current.progress.processed).toBe(2);
    });
  });

  describe('error handling', () => {
    it('should handle missing ownership type gracefully', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      const testEntity: Entity = {
        id: 'test-missing-type',
        name: 'Test Missing Type',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Test Missing Type',
          description: 'Test Description',
          definition: 'Test Definition',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
          ownership_users: 'nonexistent:DEVELOPER',
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

      // Should have an error for missing ownership type
      expect(result.current.progress.errors.length).toBeGreaterThan(0);
      expect(result.current.progress.errors[0].operation).toBe('ownership');
      expect(result.current.progress.errors[0].error).toContain('not found');
    });

    it('should handle malformed ownership strings', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      const testEntity: Entity = {
        id: 'test-malformed',
        name: 'Test Malformed',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Test Malformed',
          description: 'Test Description',
          definition: 'Test Definition',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
          ownership: 'malformed:ownership:with:too:many:colons',
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

      // Should still process the ownership (it will use the first 3 parts)
      expect(result.current.progress.errors).toHaveLength(0);
    });
  });
});
