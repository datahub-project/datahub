import { useCallback } from 'react';
import { Entity, EntityData, UseEntityManagementReturn } from '../../glossary.types';
import { mockEntities, mockExistingEntities } from '../mocks/mockData';

export function useMockEntityManagement(): UseEntityManagementReturn {
  const normalizeCsvData = useCallback((csvData: EntityData[]): Entity[] => {
    // Return mock normalized entities
    return mockEntities;
  }, []);

  const compareEntities = useCallback((entity1: Entity, entity2: Entity): boolean => {
    // Mock comparison logic
    return entity1.name === entity2.name && 
           entity1.data.description === entity2.data.description;
  }, []);

  const generateEntityId = useCallback((entityData: EntityData): string => {
    // Mock ID generation
    return `mock-${entityData.name.toLowerCase().replace(/\s+/g, '-')}`;
  }, []);

  const calculateHierarchyLevel = useCallback((entity: Entity, allEntities: Entity[]): number => {
    // Mock hierarchy calculation
    if (entity.data.parent_nodes) {
      return 1;
    }
    return 0;
  }, []);

  const sortEntitiesByHierarchy = useCallback((entities: Entity[]): Entity[] => {
    // Mock sorting - just return as is
    return entities;
  }, []);

  const detectCircularDependencies = useCallback((entities: Entity[]): string[] => {
    // Mock circular dependency detection - return empty array
    return [];
  }, []);

  const findOrphanedEntities = useCallback((entities: Entity[]): Entity[] => {
    // Mock orphan detection - return empty array
    return [];
  }, []);

  const validateEntityData = useCallback((entityData: EntityData): { isValid: boolean; errors: string[] } => {
    // Mock validation
    const errors: string[] = [];
    if (!entityData.name.trim()) {
      errors.push('Name is required');
    }
    return {
      isValid: errors.length === 0,
      errors
    };
  }, []);

  const convertGraphQLEntityToEntity = useCallback((graphqlEntity: any): Entity => {
    // Mock GraphQL conversion
    return {
      id: `mock-${graphqlEntity.urn}`,
      name: graphqlEntity.name || 'Mock Entity',
      type: 'glossaryTerm',
      urn: graphqlEntity.urn,
      parentNames: [],
      parentUrns: [],
      level: 0,
      data: {
        entity_type: 'glossaryTerm',
        urn: graphqlEntity.urn,
        name: graphqlEntity.name || 'Mock Entity',
        description: graphqlEntity.description || '',
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
      status: 'existing'
    };
  }, []);

  return {
    normalizeCsvData,
    compareEntities,
    generateEntityId,
    calculateHierarchyLevel,
    sortEntitiesByHierarchy,
    detectCircularDependencies,
    findOrphanedEntities,
    validateEntityData,
    convertGraphQLEntityToEntity
  };
}
