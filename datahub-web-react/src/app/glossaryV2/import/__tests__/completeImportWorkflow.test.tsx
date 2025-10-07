import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react-hooks';
import { ApolloClient } from '@apollo/client';
import { useImportProcessing } from '../shared/hooks/useImportProcessing';
import { useHierarchyManagement } from '../shared/hooks/useHierarchyManagement';
import { useEntityManagement } from '../shared/hooks/useEntityManagement';
import { useEntityComparison } from '../shared/hooks/useEntityComparison';
import { Entity, EntityData, HierarchyMaps } from '../glossary.types';

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
  executeUnifiedGlossaryQuery: vi.fn(),
  executePatchEntitiesMutation: vi.fn(),
  executeSetDomainMutation: vi.fn(),
  executeBatchSetDomainMutation: vi.fn(),
  executeGetOwnershipTypesQuery: vi.fn(),
  executeCreateOwnershipTypeMutation: vi.fn(),
  executeGetEntityOwnershipQuery: vi.fn()
};

vi.mock('../shared/hooks/useGraphQLOperations', () => ({
  useGraphQLOperations: () => mockGraphQLOperations
}));

describe('Complete Import Workflow Tests', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    
    // Setup default mock responses
    mockGraphQLOperations.executeUnifiedGlossaryQuery.mockResolvedValue({
      data: {
        search: {
          searchResults: []
        }
      }
    });

    mockGraphQLOperations.executeGetOwnershipTypesQuery.mockResolvedValue({
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

    mockGraphQLOperations.executePatchEntitiesMutation.mockResolvedValue({
      data: {
        patchEntities: [
          { urn: 'urn:li:glossaryTerm:test', success: true, error: null }
        ]
      }
    });

    mockGraphQLOperations.executeCreateOwnershipTypeMutation.mockResolvedValue({
      data: {
        createOwnershipType: {
          urn: 'urn:li:ownershipType:new-urn',
          success: true
        }
      }
    });
  });

  describe('Entity Management and Normalization', () => {
    it('should normalize CSV data to Entity objects correctly', () => {
      const { result: entityManagement } = renderHook(() => useEntityManagement());
      
      const csvData: EntityData[] = [
        {
          entity_type: 'glossaryTerm',
          urn: '',
          name: 'Customer ID',
          description: 'Unique identifier for each customer',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
          ownership_users: 'admin:DEVELOPER',
          ownership_groups: '',
          parent_nodes: 'Business Terms',
          related_contains: '',
          related_inherits: 'Business Terms.Customer Name',
          domain_urn: '',
          domain_name: '',
          custom_properties: ''
        },
        {
          entity_type: 'glossaryNode',
          urn: '',
          name: 'Business Terms',
          description: 'Business terminology and definitions',
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
      ];

      const { normalizeCsvData } = entityManagement.current;
      const entities = normalizeCsvData(csvData);

      expect(entities).toHaveLength(2);
      expect(entities[0].name).toBe('Customer ID');
      expect(entities[0].type).toBe('glossaryTerm');
      expect(entities[0].parentNames).toEqual(['Business Terms']);
      expect(entities[0].status).toBe('new');
      expect(entities[1].name).toBe('Business Terms');
      expect(entities[1].type).toBe('glossaryNode');
      expect(entities[1].parentNames).toEqual([]);
    });

    it('should handle hierarchical parent relationships correctly', () => {
      const { result: entityManagement } = renderHook(() => useEntityManagement());
      
      const csvData: EntityData[] = [
        {
          entity_type: 'glossaryNode',
          urn: '',
          name: 'Root Node',
          description: 'Root level node',
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
        },
        {
          entity_type: 'glossaryNode',
          urn: '',
          name: 'Child Node',
          description: 'Child level node',
          term_source: '',
          source_ref: '',
          source_url: '',
          ownership_users: '',
          ownership_groups: '',
          parent_nodes: 'Root Node',
          related_contains: '',
          related_inherits: '',
          domain_urn: '',
          domain_name: '',
          custom_properties: ''
        },
        {
          entity_type: 'glossaryTerm',
          urn: '',
          name: 'Grandchild Term',
          description: 'Grandchild level term',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
          ownership_users: '',
          ownership_groups: '',
          parent_nodes: 'Root Node,Child Node',
          related_contains: '',
          related_inherits: '',
          domain_urn: '',
          domain_name: '',
          custom_properties: ''
        }
      ];

      const { normalizeCsvData } = entityManagement.current;
      const entities = normalizeCsvData(csvData);
      const hierarchyMaps = entityManagement.buildHierarchyMaps(entities);
      const sortedEntities = entityManagement.sortEntitiesByHierarchy(entities);

      expect(sortedEntities[0].name).toBe('Root Node');
      expect(sortedEntities[0].level).toBe(0);
      expect(sortedEntities[1].name).toBe('Child Node');
      expect(sortedEntities[1].level).toBe(1);
      expect(sortedEntities[2].name).toBe('Grandchild Term');
      expect(sortedEntities[2].level).toBe(2);
    });
  });

  describe('Entity Comparison and Change Detection', () => {
    it('should detect new entities correctly', () => {
      const { result: entityComparison } = renderHook(() => useEntityComparison());
      
      const existingEntity: EntityData = {
        entity_type: 'glossaryTerm',
        urn: '',
        name: 'Existing Term',
        description: 'Existing description',
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
      };

      const newEntity: EntityData = {
        entity_type: 'glossaryTerm',
        urn: '',
        name: 'New Term',
        description: 'New description',
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
      };

      const isEqual = entityComparison.compareEntityData(existingEntity, newEntity);
      expect(isEqual).toBe(false);

      const changes = entityComparison.identifyChanges(existingEntity, newEntity);
      expect(changes).toContain('name');
      expect(changes).toContain('description');
      expect(changes).toContain('definition');
    });

    it('should detect updated entities correctly', () => {
      const { result: entityComparison } = renderHook(() => useEntityComparison());
      
      const originalEntity: EntityData = {
        entity_type: 'glossaryTerm',
        urn: '',
        name: 'Test Term',
        description: 'Original description',
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
      };

      const updatedEntity: EntityData = {
        entity_type: 'glossaryTerm',
          urn: ''
        name: 'Test Term',
        description: 'Updated description',
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
      };

      const isEqual = entityComparison.compareEntityData(originalEntity, updatedEntity);
      expect(isEqual).toBe(false);

      const changes = entityComparison.identifyChanges(originalEntity, updatedEntity);
      expect(changes).toContain('description');
      expect(changes).toContain('definition');
      expect(changes).not.toContain('name');
    });

    it('should detect no changes correctly', () => {
      const { result: entityComparison } = renderHook(() => useEntityComparison());
      
      const entity1: EntityData = {
        entity_type: 'glossaryTerm',
          urn: ''
        name: 'Test Term',
        description: 'Test description',
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
      };

      const entity2: EntityData = {
        entity_type: 'glossaryTerm',
          urn: ''
        name: 'Test Term',
        description: 'Test description',
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
      };

      const isEqual = entityComparison.compareEntityData(entity1, entity2);
      expect(isEqual).toBe(true);

      const changes = entityComparison.identifyChanges(entity1, entity2);
      expect(changes).toHaveLength(0);
    });

    it('should handle custom properties comparison correctly', () => {
      const { result: entityComparison } = renderHook(() => useEntityComparison());
      
      const entity1: EntityData = {
        entity_type: 'glossaryTerm',
          urn: ''
        name: 'Test Term',
        description: 'Test description',
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
        custom_properties: '{"data_type":"Dataset","domain":"Test Domain"}'
      };

      const entity2: EntityData = {
        entity_type: 'glossaryTerm',
          urn: ''
        name: 'Test Term',
        description: 'Test description',
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
        custom_properties: '{"data_type":"Dataset","domain":"Test Domain"}'
      };

      const isEqual = entityComparison.compareEntityData(entity1, entity2);
      expect(isEqual).toBe(true);
    });
  });

  describe('Hierarchy Management', () => {
    it('should create correct processing order for hierarchical entities', () => {
      const { result: hierarchyManagement } = renderHook(() => useHierarchyManagement());
      
      const entities: Entity[] = [
        {
          id: '1',
          name: 'Grandchild',
          type: 'glossaryTerm',
          parentNames: ['Child', 'Root'],
          parentUrns: [],
          level: 0,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        },
        {
          id: '2',
          name: 'Child',
          type: 'glossaryNode',
          parentNames: ['Root'],
          parentUrns: [],
          level: 0,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        },
        {
          id: '3',
          name: 'Root',
          type: 'glossaryNode',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        }
      ];

      const processingOrder = hierarchyManagement.createProcessingOrder(entities);
      
      // Root should be processed first, then Child, then Grandchild
      expect(processingOrder[0].name).toBe('Root');
      expect(processingOrder[1].name).toBe('Child');
      expect(processingOrder[2].name).toBe('Grandchild');
    });

    it('should detect circular dependencies', () => {
      const { result: hierarchyManagement } = renderHook(() => useHierarchyManagement());
      
      const entities: Entity[] = [
        {
          id: '1',
          name: 'Entity A',
          type: 'glossaryNode',
          parentNames: ['Entity B'],
          parentUrns: [],
          level: 0,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        },
        {
          id: '2',
          name: 'Entity B',
          type: 'glossaryNode',
          parentNames: ['Entity A'],
          parentUrns: [],
          level: 0,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        }
      ];

      const validation = hierarchyManagement.validateHierarchy(entities);
      expect(validation.isValid).toBe(false);
      expect(validation.errors).toContain('Circular dependency detected');
    });

    it('should find orphaned entities', () => {
      const { result: hierarchyManagement } = renderHook(() => useHierarchyManagement());
      
      const entities: Entity[] = [
        {
          id: '1',
          name: 'Orphaned Entity',
          type: 'glossaryTerm',
          parentNames: ['Non-existent Parent'],
          parentUrns: [],
          level: 0,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        },
        {
          id: '2',
          name: 'Valid Entity',
          type: 'glossaryTerm',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        }
      ];

      const orphaned = hierarchyManagement.findOrphanedEntities(entities);
      expect(orphaned).toHaveLength(1);
      expect(orphaned[0].name).toBe('Orphaned Entity');
    });
  });

  describe('Relationship Management', () => {
    it('should handle HasA relationships correctly', () => {
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
        parentNames: [],
        parentUrns: [],
        level: 0,
        data: {
          entity_type: 'glossaryTerm',
          urn: ''
          name: 'Test Entity',
          description: 'Test Description',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
          ownership_users: '',
          ownership_groups: '',
          parent_nodes: '',
          related_contains: 'Related Term 1,Related Term 2',
          related_inherits: '',
          domain_urn: '',
          domain_name: '',
          custom_properties: ''
        },
        status: 'new',
        originalRow: {} as EntityData
      };

      // Mock the entity URN map
      result.current['entityUrnMap'] = {
        current: new Map([
          ['Related Term 1', 'urn:li:glossaryTerm:related1'],
          ['Related Term 2', 'urn:li:glossaryTerm:related2']
        ])
      };

      act(() => {
        result.current.processEntity(testEntity);
      });

      expect(result.current.progress.errors).toHaveLength(0);
    });

    it('should handle IsA relationships correctly', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      const testEntity: Entity = {
        id: 'test-2',
        name: 'Test Entity',
        type: 'glossaryTerm',
        parentNames: [],
        parentUrns: [],
        level: 0,
        data: {
          entity_type: 'glossaryTerm',
          urn: ''
          name: 'Test Entity',
          description: 'Test Description',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
          ownership_users: '',
          ownership_groups: '',
          parent_nodes: '',
          related_contains: '',
          related_inherits: 'Parent Term 1,Parent Term 2',
          domain_urn: '',
          domain_name: '',
          custom_properties: ''
        },
        status: 'new',
        originalRow: {} as EntityData
      };

      // Mock the entity URN map
      result.current['entityUrnMap'] = {
        current: new Map([
          ['Parent Term 1', 'urn:li:glossaryTerm:parent1'],
          ['Parent Term 2', 'urn:li:glossaryTerm:parent2']
        ])
      };

      act(() => {
        result.current.processEntity(testEntity);
      });

      expect(result.current.progress.errors).toHaveLength(0);
    });

    it('should handle mixed relationships correctly', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      const testEntity: Entity = {
        id: 'test-3',
        name: 'Test Entity',
        type: 'glossaryTerm',
        parentNames: [],
        parentUrns: [],
        level: 0,
        data: {
          entity_type: 'glossaryTerm',
          urn: ''
          name: 'Test Entity',
          description: 'Test Description',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
          ownership_users: '',
          ownership_groups: '',
          parent_nodes: '',
          related_contains: 'Related Term 1',
          related_inherits: 'Parent Term 1',
          domain_urn: '',
          domain_name: '',
          custom_properties: ''
        },
        status: 'new',
        originalRow: {} as EntityData
      };

      // Mock the entity URN map
      result.current['entityUrnMap'] = {
        current: new Map([
          ['Related Term 1', 'urn:li:glossaryTerm:related1'],
          ['Parent Term 1', 'urn:li:glossaryTerm:parent1']
        ])
      };

      act(() => {
        result.current.processEntity(testEntity);
      });

      expect(result.current.progress.errors).toHaveLength(0);
    });
  });

  describe('Domain Management', () => {
    it('should handle domain assignment correctly', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      const testEntity: Entity = {
        id: 'test-domain',
        name: 'Test Entity with Domain',
        type: 'glossaryTerm',
        parentNames: [],
        parentUrns: [],
        level: 0,
        data: {
          entity_type: 'glossaryTerm',
          urn: ''
          name: 'Test Entity with Domain',
          description: 'Test Description',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
          ownership_users: '',
          ownership_groups: '',
          parent_nodes: '',
          related_contains: '',
          related_inherits: '',
          domain_urn: 'urn:li:domain:test-domain',
          domain_name: 'Test Domain',
          custom_properties: ''
        },
        status: 'new',
        originalRow: {} as EntityData
      };

      act(() => {
        result.current.processEntity(testEntity);
      });

      expect(result.current.progress.errors).toHaveLength(0);
    });

    it('should handle domain name resolution correctly', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      const testEntity: Entity = {
        id: 'test-domain-name',
        name: 'Test Entity with Domain Name',
        type: 'glossaryTerm',
        parentNames: [],
        parentUrns: [],
        level: 0,
        data: {
          entity_type: 'glossaryTerm',
          urn: ''
          name: 'Test Entity with Domain Name',
          description: 'Test Description',
          term_source: 'INTERNAL',
          source_ref: '',
          source_url: '',
          ownership_users: '',
          ownership_groups: '',
          parent_nodes: '',
          related_contains: '',
          related_inherits: '',
          domain_urn: '',
          domain_name: 'Test Domain',
          custom_properties: ''
        },
        status: 'new',
        originalRow: {} as EntityData
      };

      // Mock domain lookup
      mockGraphQLOperations.executeUnifiedGlossaryQuery.mockResolvedValue({
        data: {
          search: {
            searchResults: [
              {
                entity: {
                  urn: 'urn:li:domain:test-domain',
                  name: 'Test Domain'
                }
              }
            ]
          }
        }
      });

      act(() => {
        result.current.processEntity(testEntity);
      });

      expect(result.current.progress.errors).toHaveLength(0);
    });
  });

  describe('Complete Import Workflow', () => {
    it('should process a complete CSV import with all features', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 10
        })
      );

      const csvEntities: Entity[] = [
        {
          id: 'business-terms',
          name: 'Business Terms',
          type: 'glossaryNode',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: {
            entity_type: 'glossaryNode',
            name: 'Business Terms',
            description: 'Business terminology and definitions',
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
          },
          status: 'new',
          originalRow: {} as EntityData
        },
        {
          id: 'customer-id',
          name: 'Customer ID',
          type: 'glossaryTerm',
          parentNames: ['Business Terms'],
          parentUrns: [],
          level: 1,
          data: {
            entity_type: 'glossaryTerm',
          urn: ''
            name: 'Customer ID',
            description: 'Unique identifier for each customer',
            term_source: 'INTERNAL',
            source_ref: '',
            source_url: '',
            ownership_users: 'datahub:Technical Owner',
            ownership_groups: 'bfoo:Technical Owner',
            parent_nodes: 'Business Terms',
            related_contains: 'Customer Name',
            related_inherits: 'Business Terms.Customer Name',
            domain_urn: '',
            domain_name: 'Customer Domain',
            custom_properties: '{"data_type":"Dataset","domain":"Customer Domain"}'
          },
          status: 'new',
          originalRow: {} as EntityData
        },
        {
          id: 'customer-name',
          name: 'Customer Name',
          type: 'glossaryTerm',
          parentNames: ['Business Terms'],
          parentUrns: [],
          level: 1,
          data: {
            entity_type: 'glossaryTerm',
          urn: ''
            name: 'Customer Name',
            description: 'Full name of the customer',
            term_source: 'INTERNAL',
            source_ref: '',
            source_url: '',
            ownership_users: 'datahub:Technical Owner',
          ownership_groups: '',
            parent_nodes: 'Business Terms',
            related_contains: '',
            related_inherits: '',
            domain_urn: '',
            domain_name: 'Customer Domain',
            custom_properties: '{"data_type":"Dataset","domain":"Customer Domain"}'
          },
          status: 'new',
          originalRow: {} as EntityData
        }
      ];

      // Mock domain lookup
      mockGraphQLOperations.executeUnifiedGlossaryQuery.mockResolvedValue({
        data: {
          search: {
            searchResults: [
              {
                entity: {
                  urn: 'urn:li:domain:customer-domain',
                  name: 'Customer Domain'
                }
              }
            ]
          }
        }
      });

      act(() => {
        result.current.startImport(csvEntities, []);
      });

      expect(result.current.progress.errors).toHaveLength(0);
      expect(result.current.progress.processed).toBe(3);
      expect(result.current.progress.successful).toBe(3);
    });

    it('should handle mixed new and updated entities correctly', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      const existingEntities: Entity[] = [
        {
          id: 'existing-1',
          name: 'Existing Term',
          type: 'glossaryTerm',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: {
            entity_type: 'glossaryTerm',
          urn: ''
            name: 'Existing Term',
            description: 'Original description',
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
          },
          status: 'existing',
          originalRow: {} as EntityData
        }
      ];

      const newEntities: Entity[] = [
        {
          id: 'new-1',
          name: 'New Term',
          type: 'glossaryTerm',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: {
            entity_type: 'glossaryTerm',
          urn: ''
            name: 'New Term',
            description: 'New description',
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
          },
          status: 'new',
          originalRow: {} as EntityData
        }
      ];

      act(() => {
        result.current.startImport(newEntities, existingEntities);
      });

      expect(result.current.progress.errors).toHaveLength(0);
      expect(result.current.progress.processed).toBe(1);
      expect(result.current.progress.successful).toBe(1);
    });
  });

  describe('Error Handling and Recovery', () => {
    it('should handle GraphQL errors gracefully', () => {
      const { result } = renderHook(() => 
        useImportProcessing({
          apolloClient: mockApolloClient,
          batchSize: 5
        })
      );

      // Mock GraphQL error
      mockGraphQLOperations.executePatchEntitiesMutation.mockRejectedValue(new Error('GraphQL Error'));

      const testEntity: Entity = {
        id: 'test-error',
        name: 'Test Error',
        type: 'glossaryTerm',
        parentNames: [],
        parentUrns: [],
        level: 0,
        data: {
          entity_type: 'glossaryTerm',
          urn: ''
          name: 'Test Error',
          description: 'Test Description',
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
        },
        status: 'new',
        originalRow: {} as EntityData
      };

      act(() => {
        result.current.processEntity(testEntity);
      });

      expect(result.current.progress.errors.length).toBeGreaterThan(0);
      expect(result.current.progress.errors[0].error).toContain('GraphQL Error');
    });

    it('should handle retry logic correctly', () => {
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
      mockGraphQLOperations.executePatchEntitiesMutation.mockImplementation(() => {
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
        parentNames: [],
        parentUrns: [],
        level: 0,
        data: {
          entity_type: 'glossaryTerm',
          urn: ''
          name: 'Test Retry',
          description: 'Test Description',
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
        },
        status: 'new',
        originalRow: {} as EntityData
      };

      act(() => {
        result.current.processEntity(testEntity);
      });

      expect(callCount).toBe(3);
      expect(result.current.progress.successful).toBe(1);
    });
  });
});
