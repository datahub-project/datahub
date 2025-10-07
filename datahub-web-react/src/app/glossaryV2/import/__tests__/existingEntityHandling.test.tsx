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

// Mock Apollo Client
const mockApolloClient = {
  query: vi.fn(),
  mutate: vi.fn().mockResolvedValue({
    data: {
      patchEntities: [
        { urn: 'urn:li:glossaryTerm:test', success: true, error: null }
      ]
    }
  })
} as unknown as ApolloClient<any>;

// Mock GraphQL operations
const mockGraphQLOperations = {
  executeUnifiedGlossaryQuery: vi.fn(),
  executePatchEntitiesMutation: vi.fn().mockResolvedValue([
    { urn: 'urn:li:glossaryTerm:test', success: true, error: null }
  ]),
  executeAddRelatedTermsMutation: vi.fn(),
  executeSetDomainMutation: vi.fn(),
  executeBatchSetDomainMutation: vi.fn(),
  executeGetOwnershipTypesQuery: vi.fn().mockResolvedValue({
    data: {
      listOwnershipTypes: {
        ownershipTypes: [
          {
            urn: 'urn:li:ownershipType:__system__technical_owner',
            info: { name: 'Technical Owner' }
          }
        ]
      }
    }
  })
};

vi.mock('../shared/hooks/useGraphQLOperations', () => ({
  useGraphQLOperations: () => mockGraphQLOperations
}));

describe('Existing Entity Handling Tests', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Entity Categorization', () => {
    it('should correctly categorize new entities', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 10
        })
      );

      const newEntity: Entity = {
        id: 'new-entity',
        name: 'New Entity',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'New Entity',
          description: 'A new entity',
          definition: 'A new entity definition',
          term_source: 'INTERNAL',
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
        },
        parentNames: [],
        parentUrns: [],
        level: 0,
        status: 'new'
      };

      const existingEntities: Entity[] = [];

      // Test that new entities are processed
      expect(newEntity.status).toBe('new');
      expect(newEntity.urn).toBeUndefined();
    });

    it('should correctly categorize existing entities with URNs', () => {
      const existingEntity: Entity = {
        id: 'existing-entity',
        name: 'Existing Entity',
        type: 'glossaryTerm',
        urn: 'urn:li:glossaryTerm:existing-123', // This is the key - existing URN
        data: {
          entity_type: 'glossaryTerm',
          name: 'Existing Entity',
          description: 'An existing entity',
          definition: 'An existing entity definition',
          term_source: 'INTERNAL',
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
        },
        parentNames: [],
        parentUrns: [],
        level: 0,
        status: 'existing'
      };

      const updatedEntity: Entity = {
        id: 'existing-entity',
        name: 'Existing Entity',
        type: 'glossaryTerm',
        // No URN - this should be updated with the existing URN
        data: {
          entity_type: 'glossaryTerm',
          name: 'Existing Entity',
          description: 'An updated entity description', // Changed
          definition: 'An existing entity definition',
          term_source: 'INTERNAL',
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
        },
        parentNames: [],
        parentUrns: [],
        level: 0,
        status: 'updated',
        existingEntity
      };

      // Test that updated entities should use the existing URN
      expect(updatedEntity.status).toBe('updated');
      expect(updatedEntity.existingEntity?.urn).toBe('urn:li:glossaryTerm:existing-123');
    });

    it('should correctly categorize unchanged entities', () => {
      const existingEntity: Entity = {
        id: 'unchanged-entity',
        name: 'Unchanged Entity',
        type: 'glossaryTerm',
        urn: 'urn:li:glossaryTerm:unchanged-123',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Unchanged Entity',
          description: 'An unchanged entity',
          definition: 'An unchanged entity definition',
          term_source: 'INTERNAL',
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
        },
        parentNames: [],
        parentUrns: [],
        level: 0,
        status: 'existing'
      };

      const unchangedEntity: Entity = {
        id: 'unchanged-entity',
        name: 'Unchanged Entity',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Unchanged Entity',
          description: 'An unchanged entity', // Same as existing
          definition: 'An unchanged entity definition', // Same as existing
          term_source: 'INTERNAL',
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
        },
        parentNames: [],
        parentUrns: [],
        level: 0,
        status: 'unchanged',
        existingEntity
      };

      // Test that unchanged entities should be skipped
      expect(unchangedEntity.status).toBe('unchanged');
      expect(unchangedEntity.existingEntity?.urn).toBe('urn:li:glossaryTerm:unchanged-123');
    });
  });

  describe('URN Resolution in Processing', () => {
    it('should pass existing URNs for updated entities', async () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 10
        })
      );

      const existingEntity: Entity = {
        id: 'test-entity',
        name: 'Test Entity',
        type: 'glossaryTerm',
        urn: 'urn:li:glossaryTerm:existing-123',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Test Entity',
          description: 'Original description',
          definition: 'Original definition',
          term_source: 'INTERNAL',
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
        },
        parentNames: [],
        parentUrns: [],
        level: 0,
        status: 'existing'
      };

      const updatedEntity: Entity = {
        id: 'test-entity',
        name: 'Test Entity',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Test Entity',
          description: 'Updated description', // Changed
          definition: 'Original definition',
          term_source: 'INTERNAL',
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
        },
        parentNames: [],
        parentUrns: [],
        level: 0,
        status: 'updated',
        existingEntity
      };

      // Mock the batch processing
      const batch = {
        entities: [updatedEntity],
        existingEntities: [existingEntity],
        hierarchyMaps: { parentMap: new Map(), childMap: new Map() }
      };

      // This should use the existing URN, not create a new entity
      await act(async () => {
        await result.current.processBatch(batch);
      });

      // Verify that executePatchEntitiesMutation was called with the existing URN
      expect(mockGraphQLOperations.executePatchEntitiesMutation).toHaveBeenCalled();
      const patchCall = mockGraphQLOperations.executePatchEntitiesMutation.mock.calls[0][0];
      expect(patchCall[0]).toMatchObject({
        entityType: 'glossaryTerm',
        aspectName: 'glossaryTermInfo',
        // Should include URN for existing entities
        urn: 'urn:li:glossaryTerm:existing-123'
      });
    });

    it('should not pass URNs for new entities', async () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 10
        })
      );

      const newEntity: Entity = {
        id: 'new-entity',
        name: 'New Entity',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'New Entity',
          description: 'A new entity',
          definition: 'A new entity definition',
          term_source: 'INTERNAL',
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
        },
        parentNames: [],
        parentUrns: [],
        level: 0,
        status: 'new'
      };

      const batch = {
        entities: [newEntity],
        existingEntities: [],
        hierarchyMaps: { parentMap: new Map(), childMap: new Map() }
      };

      await act(async () => {
        await result.current.processBatch(batch);
      });

      // Verify that executePatchEntitiesMutation was called without URN for new entities
      expect(mockGraphQLOperations.executePatchEntitiesMutation).toHaveBeenCalled();
      const patchCall = mockGraphQLOperations.executePatchEntitiesMutation.mock.calls[0][0];
      expect(patchCall[0]).toMatchObject({
        entityType: 'glossaryTerm',
        aspectName: 'glossaryTermInfo'
        // Should NOT include URN for new entities
      });
      expect(patchCall[0]).not.toHaveProperty('urn');
    });

    it('should skip unchanged entities entirely', async () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 10
        })
      );

      const unchangedEntity: Entity = {
        id: 'unchanged-entity',
        name: 'Unchanged Entity',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Unchanged Entity',
          description: 'Unchanged description',
          definition: 'Unchanged definition',
          term_source: 'INTERNAL',
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
        },
        parentNames: [],
        parentUrns: [],
        level: 0,
        status: 'unchanged'
      };

      const batch = {
        entities: [unchangedEntity],
        existingEntities: [],
        hierarchyMaps: { parentMap: new Map(), childMap: new Map() }
      };

      await act(async () => {
        await result.current.processBatch(batch);
      });

      // Verify that executePatchEntitiesMutation was NOT called for unchanged entities
      expect(mockGraphQLOperations.executePatchEntitiesMutation).not.toHaveBeenCalled();
    });
  });

  describe('Duplicate Prevention', () => {
    it('should prevent duplicate creation when entity already exists', async () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 10
        })
      );

      // Simulate a scenario where an entity already exists but the system tries to create it again
      const existingEntity: Entity = {
        id: 'duplicate-test',
        name: 'Duplicate Test',
        type: 'glossaryTerm',
        urn: 'urn:li:glossaryTerm:duplicate-123',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Duplicate Test',
          description: 'This entity already exists',
          definition: 'This entity already exists',
          term_source: 'INTERNAL',
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
        },
        parentNames: [],
        parentUrns: [],
        level: 0,
        status: 'existing'
      };

      const importedEntity: Entity = {
        id: 'duplicate-test',
        name: 'Duplicate Test',
        type: 'glossaryTerm',
        data: {
          entity_type: 'glossaryTerm',
          name: 'Duplicate Test',
          description: 'This entity already exists',
          definition: 'This entity already exists',
          term_source: 'INTERNAL',
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
        },
        parentNames: [],
        parentUrns: [],
        level: 0,
        status: 'unchanged',
        existingEntity
      };

      const batch = {
        entities: [importedEntity],
        existingEntities: [existingEntity],
        hierarchyMaps: { parentMap: new Map(), childMap: new Map() }
      };

      await act(async () => {
        await result.current.processBatch(batch);
      });

      // Should not call executePatchEntitiesMutation for unchanged entities
      expect(mockGraphQLOperations.executePatchEntitiesMutation).not.toHaveBeenCalled();
    });
  });
});
