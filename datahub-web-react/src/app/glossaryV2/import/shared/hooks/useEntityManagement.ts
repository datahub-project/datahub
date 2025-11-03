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
  UseEntityManagementReturn, 
} from '@app/glossaryV2/import/glossary.types';
import {
  generateEntityId,
  parseCommaSeparated,
  convertGraphQLEntityToEntity,
  calculateHierarchyLevel,
  sortEntitiesByHierarchy,
  detectCircularDependencies,
  findOrphanedEntities,
} from '@app/glossaryV2/import/glossary.utils';
import { useEntityComparison } from '@app/glossaryV2/import/shared/hooks/useEntityComparison';

export function useEntityManagement(): UseEntityManagementReturn {
  const { compareEntityData } = useEntityComparison();

  const normalizeCsvData = useCallback((data: EntityData[]): Entity[] => {
    const entities = data.map(entityData => {
      const parentNames = parseCommaSeparated(entityData.parent_nodes);
      const id = generateEntityId(entityData, parentNames);
      
      return {
        id,
        name: entityData.name,
        type: entityData.entity_type,
        urn: entityData.urn || undefined,
        parentNames,
        parentUrns: [],
        level: 0,
        data: entityData,
        status: 'new' as const,
        originalRow: entityData,
      };
    });

    return entities.map(entity => ({
      ...entity,
      level: calculateHierarchyLevel(entity, entities),
    }));
  }, []);

  const normalizeExistingEntities = useCallback((entities: GraphQLEntity[]): Entity[] => {
    return entities.map(convertGraphQLEntityToEntity);
  }, []);

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
        newEntities.push(importedEntity);
      } else if (importedEntity.type !== existingEntity.type) {
          conflicts.push({
            ...importedEntity,
            status: 'conflict',
          });
        } else {
          // compareEntityData returns true if entities are the SAME
          const areEqual = compareEntityData(importedEntity.data, existingEntity.data);
          if (!areEqual) {
            updatedEntities.push({
              ...importedEntity,
              status: 'updated',
            });
          } else {
            existingEntities.push({
              ...importedEntity,
              status: 'existing',
            });
          }
        }
    });

    return {
      newEntities,
      existingEntities,
      updatedEntities,
      conflicts,
    };
  }, [compareEntityData]);

  const buildHierarchyMaps = useCallback((entities: Entity[]): HierarchyMaps => {
    const entitiesByLevel = new Map<number, Entity[]>();
    const entitiesByName = new Map<string, Entity>();
    const entitiesById = new Map<string, Entity>();
    const parentChildMap = new Map<string, string[]>();

    entities.forEach(entity => {
      entitiesByName.set(entity.name.toLowerCase(), entity);
      entitiesById.set(entity.id, entity);
      parentChildMap.set(entity.name.toLowerCase(), []);
    });

    entities.forEach(entity => {
      entity.parentNames.forEach(parentName => {
        const parentKey = parentName.toLowerCase();
        if (parentChildMap.has(parentKey)) {
          parentChildMap.get(parentKey)!.push(entity.name.toLowerCase());
        }
      });
    });

    const entitiesWithLevels = entities.map(entity => ({
      ...entity,
      level: calculateHierarchyLevel(entity, entities),
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
      parentChildMap,
    };
  }, []);

  const validateEntity = useCallback((entity: Entity): ValidationResult => {
    const errors: ValidationError[] = [];
    const warnings: ValidationWarning[] = [];

    if (!entity.name || entity.name.trim() === '') {
      errors.push({
        field: 'name',
        message: 'Entity name is required',
        code: 'REQUIRED',
      });
    }

    if (!entity.type || (entity.type !== 'glossaryTerm' && entity.type !== 'glossaryNode')) {
      errors.push({
        field: 'type',
        message: 'Entity type must be either "glossaryTerm" or "glossaryNode"',
        code: 'INVALID_TYPE',
      });
    }

    if (entity.urn && !entity.urn.startsWith('urn:li:')) {
      errors.push({
        field: 'urn',
        message: 'URN must start with "urn:li:"',
        code: 'INVALID_URN',
      });
    }

    if (entity.parentNames.length > 10) {
      warnings.push({
        field: 'parentNames',
        message: 'Having more than 10 parent nodes may cause performance issues',
        code: 'TOO_MANY_PARENTS',
      });
    }

    if (entity.data.domain_urn && !entity.data.domain_urn.startsWith('urn:li:domain:')) {
      errors.push({
        field: 'domain_urn',
        message: 'Domain URN must start with "urn:li:domain:"',
        code: 'INVALID_DOMAIN_URN',
      });
    }

    if (entity.data.source_url) {
      try {
        new URL(entity.data.source_url);
      } catch (error) {
        errors.push({
          field: 'source_url',
          message: 'Source URL must be a valid URL format',
          code: 'INVALID_URL',
        });
      }
    }

    return {
      isValid: errors.length === 0,
      errors,
      warnings,
    };
  }, []);

  return {
    normalizeCsvData,
    normalizeExistingEntities,
    compareEntities,
    buildHierarchyMaps,
    validateEntity,
  };
}
