/**
 * Hook for entity management and normalization
 */

import { useCallback } from 'react';
import { 
  EntityData, 
  Entity, 
  GraphQLEntity, 
  ComparisonResult, 
  HierarchyMaps, 
  ValidationResult,
  ValidationError,
  ValidationWarning,
  UseEntityManagementReturn 
} from '../../glossary.types';
import {
  generateEntityId,
  parseCommaSeparated,
  convertGraphQLEntityToEntity,
  calculateHierarchyLevel,
  sortEntitiesByHierarchy,
  detectCircularDependencies,
  findOrphanedEntities
} from '../../glossary.utils';

export function useEntityManagement(): UseEntityManagementReturn {
  /**
   * Convert CSV data to Entity objects
   */
  const normalizeCsvData = useCallback((data: EntityData[]): Entity[] => {
    // First pass: create entities without levels
    const entities = data.map(entityData => {
      const parentNames = parseCommaSeparated(entityData.parent_nodes);
      const id = generateEntityId(entityData, parentNames);
      
      return {
        id,
        name: entityData.name,
        type: entityData.entity_type,
        urn: entityData.urn || undefined,
        parentNames,
        parentUrns: [], // Will be resolved later
        level: 0, // Will be calculated below
        data: entityData,
        status: 'new' as const,
        originalRow: entityData
      };
    });

    // Second pass: calculate hierarchy levels
    return entities.map(entity => ({
      ...entity,
      level: calculateHierarchyLevel(entity, entities)
    }));
  }, []);

  /**
   * Convert GraphQL entities to Entity objects
   */
  const normalizeExistingEntities = useCallback((entities: GraphQLEntity[]): Entity[] => {
    return entities.map(convertGraphQLEntityToEntity);
  }, []);

  /**
   * Compare existing vs imported entities
   */
  const compareEntities = useCallback((imported: Entity[], existing: Entity[]): ComparisonResult => {
    const newEntities: Entity[] = [];
    const existingEntities: Entity[] = [];
    const updatedEntities: Entity[] = [];
    const conflicts: Entity[] = [];

    const existingByName = new Map<string, Entity>();
    existing.forEach(entity => {
      existingByName.set(entity.name.toLowerCase(), entity);
    });

    imported.forEach(importedEntity => {
      const existingEntity = existingByName.get(importedEntity.name.toLowerCase());
      
      if (!existingEntity) {
        // New entity
        newEntities.push(importedEntity);
      } else {
        // Entity exists, check for conflicts and updates
        if (importedEntity.type !== existingEntity.type) {
          // Conflict: same name but different type
          conflicts.push({
            ...importedEntity,
            status: 'conflict'
          });
        } else {
          // Check if data has changed
          const hasChanges = compareEntityData(importedEntity.data, existingEntity.data);
          if (hasChanges) {
            updatedEntities.push({
              ...importedEntity,
              status: 'updated'
            });
          } else {
            existingEntities.push({
              ...importedEntity,
              status: 'existing'
            });
          }
        }
      }
    });

    return {
      newEntities,
      existingEntities,
      updatedEntities,
      conflicts
    };
  }, []);

  /**
   * Create lookup maps for hierarchy management
   */
  const buildHierarchyMaps = useCallback((entities: Entity[]): HierarchyMaps => {
    const entitiesByLevel = new Map<number, Entity[]>();
    const entitiesByName = new Map<string, Entity>();
    const entitiesById = new Map<string, Entity>();
    const parentChildMap = new Map<string, string[]>();

    // First pass: create basic maps
    entities.forEach(entity => {
      entitiesByName.set(entity.name.toLowerCase(), entity);
      entitiesById.set(entity.id, entity);
      
      // Initialize parent-child map
      parentChildMap.set(entity.name.toLowerCase(), []);
    });

    // Second pass: build parent-child relationships
    entities.forEach(entity => {
      entity.parentNames.forEach(parentName => {
        const parentKey = parentName.toLowerCase();
        if (parentChildMap.has(parentKey)) {
          parentChildMap.get(parentKey)!.push(entity.name.toLowerCase());
        }
      });
    });

    // Third pass: calculate levels and group by level
    const entitiesWithLevels = entities.map(entity => ({
      ...entity,
      level: calculateHierarchyLevel(entity, entities)
    }));

    entitiesWithLevels.forEach(entity => {
      if (!entitiesByLevel.has(entity.level)) {
        entitiesByLevel.set(entity.level, []);
      }
      entitiesByLevel.get(entity.level)!.push(entity);
    });

    return {
      entitiesByLevel,
      entitiesByName,
      entitiesById,
      parentChildMap
    };
  }, []);

  /**
   * Validate individual entity data
   */
  const validateEntity = useCallback((entity: Entity): ValidationResult => {
    const errors: ValidationError[] = [];
    const warnings: ValidationWarning[] = [];

    // Validate required fields
    if (!entity.name || entity.name.trim() === '') {
      errors.push({
        field: 'name',
        message: 'Entity name is required',
        code: 'REQUIRED'
      });
    }

    if (!entity.type || (entity.type !== 'glossaryTerm' && entity.type !== 'glossaryNode')) {
      errors.push({
        field: 'type',
        message: 'Entity type must be either "glossaryTerm" or "glossaryNode"',
        code: 'INVALID_TYPE'
      });
    }

    // Validate URN format if provided
    if (entity.urn && !entity.urn.startsWith('urn:li:')) {
      errors.push({
        field: 'urn',
        message: 'URN must start with "urn:li:"',
        code: 'INVALID_URN'
      });
    }

    // Validate parent relationships
    if (entity.parentNames.length > 10) {
      warnings.push({
        field: 'parentNames',
        message: 'Having more than 10 parent nodes may cause performance issues',
        code: 'TOO_MANY_PARENTS'
      });
    }

    // Validate domain URN if provided
    if (entity.data.domain_urn && !entity.data.domain_urn.startsWith('urn:li:domain:')) {
      errors.push({
        field: 'domain_urn',
        message: 'Domain URN must start with "urn:li:domain:"',
        code: 'INVALID_DOMAIN_URN'
      });
    }

    // Validate source URL if provided
    if (entity.data.source_url) {
      try {
        new URL(entity.data.source_url);
      } catch (error) {
        errors.push({
          field: 'source_url',
          message: 'Source URL must be a valid URL format',
          code: 'INVALID_URL'
        });
      }
    }

    return {
      isValid: errors.length === 0,
      errors,
      warnings
    };
  }, []);

  return {
    normalizeCsvData,
    normalizeExistingEntities,
    compareEntities,
    buildHierarchyMaps,
    validateEntity
  };
}

/**
 * Helper function to compare two EntityData objects
 */
function compareEntityData(data1: EntityData, data2: EntityData): boolean {
  const fieldsToCompare: (keyof EntityData)[] = [
    'entity_type',
    'name',
    'description',
    'term_source',
    'source_ref',
    'source_url',
    'ownership_users',
    'ownership_groups',
    'parent_nodes',
    'related_contains',
    'related_inherits',
    'domain_urn',
    'domain_name',
    'custom_properties'
  ];

  return fieldsToCompare.every(field => {
    const value1 = data1[field] || '';
    const value2 = data2[field] || '';
    return value1.trim() === value2.trim();
  });
}
