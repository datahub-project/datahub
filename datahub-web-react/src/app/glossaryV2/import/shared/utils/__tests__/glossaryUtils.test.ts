import { describe, it, expect } from 'vitest';
import {
  generateEntityId,
  parseCommaSeparated,
  convertGraphQLEntityToEntity,
  calculateHierarchyLevel,
  sortEntitiesByHierarchy,
  detectCircularDependencies,
  findOrphanedEntities
} from '../../../glossary.utils';
import { Entity, EntityData, GraphQLEntity } from '../../../glossary.types';

describe('Glossary Utils', () => {
  describe('generateEntityId', () => {
    it('should generate unique IDs for different entities', () => {
      const entityData1: EntityData = {
        entity_type: 'glossaryTerm',
        name: 'Test Term 1',
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
      };

      const entityData2: EntityData = {
        entity_type: 'glossaryTerm',
        name: 'Test Term 2',
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
      };

      const id1 = generateEntityId(entityData1, []);
      const id2 = generateEntityId(entityData2, []);

      expect(id1).toBeDefined();
      expect(id2).toBeDefined();
      expect(id1).not.toBe(id2);
    });

    it('should generate same ID for same entity data', () => {
      const entityData: EntityData = {
        entity_type: 'glossaryTerm',
        name: 'Test Term',
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
      };

      const id1 = generateEntityId(entityData, []);
      const id2 = generateEntityId(entityData, []);

      expect(id1).toBe(id2);
    });

    it('should include parent names in ID generation', () => {
      const entityData: EntityData = {
        entity_type: 'glossaryTerm',
        name: 'Test Term',
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
      };

      const id1 = generateEntityId(entityData, ['Parent1']);
      const id2 = generateEntityId(entityData, ['Parent2']);

      expect(id1).not.toBe(id2);
    });
  });

  describe('parseCommaSeparated', () => {
    it('should parse comma-separated values correctly', () => {
      const result = parseCommaSeparated('value1,value2,value3');
      expect(result).toEqual(['value1', 'value2', 'value3']);
    });

    it('should handle empty string', () => {
      const result = parseCommaSeparated('');
      expect(result).toEqual([]);
    });

    it('should handle single value', () => {
      const result = parseCommaSeparated('single');
      expect(result).toEqual(['single']);
    });

    it('should trim whitespace', () => {
      const result = parseCommaSeparated(' value1 , value2 , value3 ');
      expect(result).toEqual(['value1', 'value2', 'value3']);
    });

    it('should handle empty values', () => {
      const result = parseCommaSeparated('value1,,value3');
      expect(result).toEqual(['value1', 'value3']);
    });
  });

  describe('convertGraphQLEntityToEntity', () => {
    it('should convert GraphQL entity to Entity correctly', () => {
      const graphQLEntity: GraphQLEntity = {
        __typename: 'GlossaryTerm',
        urn: 'urn:li:glossaryTerm:test',
        name: 'Test Term',
        hierarchicalName: 'Test Term',
        properties: {
          name: 'Test Term',
          description: 'Test Description',
          termSource: 'INTERNAL',
          sourceRef: 'Test Source',
          sourceUrl: 'https://example.com',
          customProperties: [
            {
              key: 'data_type',
              value: 'Dataset'
            }
          ],
          __typename: 'GlossaryTermProperties'
        },
        parentNodes: {
          nodes: [
            {
              urn: 'urn:li:glossaryNode:parent',
              properties: {
                name: 'Parent Node',
                description: 'Parent Description',
                termSource: 'INTERNAL',
                sourceRef: null,
                sourceUrl: null,
                customProperties: null,
                __typename: 'GlossaryNodeProperties'
              },
              __typename: 'GlossaryNode'
            }
          ],
          __typename: 'ParentNodesResult'
        },
        relatedTerms: {
          relationships: [
            {
              entity: {
                urn: 'urn:li:glossaryTerm:related',
                name: 'Related Term',
                __typename: 'GlossaryTerm'
              },
              __typename: 'EntityRelationship'
            }
          ],
          __typename: 'EntityRelationshipsResult'
        },
        domain: {
          domain: {
            urn: 'urn:li:domain:test',
            properties: {
              name: 'Test Domain',
              description: 'Test Domain Description',
              __typename: 'DomainProperties'
            },
            __typename: 'Domain'
          },
          __typename: 'DomainAssociation'
        },
        __typename: 'GlossaryTerm'
      };

      const entity = convertGraphQLEntityToEntity(graphQLEntity);

      expect(entity.urn).toBe('urn:li:glossaryTerm:test');
      expect(entity.name).toBe('Test Term');
      expect(entity.type).toBe('glossaryTerm');
      expect(entity.parentNames).toEqual(['Parent Node']);
      expect(entity.status).toBe('existing');
    });
  });

  describe('calculateHierarchyLevel', () => {
    it('should calculate correct hierarchy levels', () => {
      const entities: Entity[] = [
        {
          id: '1',
          name: 'Root',
          type: 'glossaryNode',
          parentNames: [],
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
          name: 'Grandchild',
          type: 'glossaryTerm',
          parentNames: ['Root', 'Child'],
          parentUrns: [],
          level: 0,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        }
      ];

      const entitiesWithLevels = entities.map(entity => ({
        ...entity,
        level: calculateHierarchyLevel(entity, entities)
      }));

      expect(entitiesWithLevels[0].level).toBe(0); // Root
      expect(entitiesWithLevels[1].level).toBe(1); // Child
      expect(entitiesWithLevels[2].level).toBe(2); // Grandchild
    });

    it('should handle entities with no parents', () => {
      const entities: Entity[] = [
        {
          id: '1',
          name: 'Orphan',
          type: 'glossaryTerm',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        }
      ];

      const entitiesWithLevels = entities.map(entity => ({
        ...entity,
        level: calculateHierarchyLevel(entity, entities)
      }));
      expect(entitiesWithLevels[0].level).toBe(0);
    });
  });

  describe('sortEntitiesByHierarchy', () => {
    it('should sort entities by hierarchy level', () => {
      const entities: Entity[] = [
        {
          id: '3',
          name: 'Grandchild',
          type: 'glossaryTerm',
          parentNames: ['Root', 'Child'],
          parentUrns: [],
          level: 2,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        },
        {
          id: '1',
          name: 'Root',
          type: 'glossaryNode',
          parentNames: [],
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
          level: 1,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        }
      ];

      const sortedEntities = sortEntitiesByHierarchy(entities);

      expect(sortedEntities[0].name).toBe('Root');
      expect(sortedEntities[1].name).toBe('Child');
      expect(sortedEntities[2].name).toBe('Grandchild');
    });
  });

  describe('detectCircularDependencies', () => {
    it('should detect circular dependencies', () => {
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

      const result = detectCircularDependencies(entities);
      expect(result.isValid).toBe(false);
      expect(result.errors.length).toBeGreaterThan(0);
    });

    it('should not detect circular dependencies in valid hierarchy', () => {
      const entities: Entity[] = [
        {
          id: '1',
          name: 'Root',
          type: 'glossaryNode',
          parentNames: [],
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
          level: 1,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        }
      ];

      const result = detectCircularDependencies(entities);
      expect(result.isValid).toBe(true);
    });

    it('should detect complex circular dependencies', () => {
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
          parentNames: ['Entity C'],
          parentUrns: [],
          level: 0,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        },
        {
          id: '3',
          name: 'Entity C',
          type: 'glossaryNode',
          parentNames: ['Entity A'],
          parentUrns: [],
          level: 0,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        }
      ];

      const result = detectCircularDependencies(entities);
      expect(result.isValid).toBe(false);
      expect(result.errors.length).toBeGreaterThan(0);
    });
  });

  describe('findOrphanedEntities', () => {
    it('should find orphaned entities', () => {
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

      const result = findOrphanedEntities(entities);
      expect(result.isValid).toBe(true);
      expect(result.warnings.length).toBeGreaterThan(0);
    });

    it('should not find orphaned entities when all parents exist', () => {
      const entities: Entity[] = [
        {
          id: '1',
          name: 'Parent',
          type: 'glossaryNode',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        },
        {
          id: '2',
          name: 'Child',
          type: 'glossaryTerm',
          parentNames: ['Parent'],
          parentUrns: [],
          level: 1,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        }
      ];

      const result = findOrphanedEntities(entities);
      expect(result.isValid).toBe(true);
    });

    it('should handle entities with multiple parents', () => {
      const entities: Entity[] = [
        {
          id: '1',
          name: 'Parent 1',
          type: 'glossaryNode',
          parentNames: [],
          parentUrns: [],
          level: 0,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        },
        {
          id: '2',
          name: 'Child',
          type: 'glossaryTerm',
          parentNames: ['Parent 1', 'Non-existent Parent'],
          parentUrns: [],
          level: 1,
          data: {} as EntityData,
          status: 'new',
          originalRow: {} as EntityData
        }
      ];

      const result = findOrphanedEntities(entities);
      expect(result.isValid).toBe(true);
      expect(result.warnings.length).toBeGreaterThan(0);
    });
  });
});
