/**
 * Hook for hierarchy management and validation
 */

import { useCallback } from 'react';
import { 
  Entity, 
  HierarchyMaps, 
  ValidationResult,
  UseHierarchyManagementReturn, 
} from '@app/glossaryV2/import/glossary.types';
import {
  sortEntitiesByHierarchy,
  detectCircularDependencies,
  findOrphanedEntities,
  parseCommaSeparated,
} from '@app/glossaryV2/import/glossary.utils';

export function useHierarchyManagement(): UseHierarchyManagementReturn {
  const createProcessingOrder = useCallback((entities: Entity[]): Entity[] => {
    const sortedEntities = sortEntitiesByHierarchy(entities);
    const validation = validateHierarchy(sortedEntities);
    if (!validation.isValid) {
      console.warn('Hierarchy validation failed:', validation.errors);
    }

    return sortedEntities;
  }, []);

  const resolveParentUrns = useCallback((entities: Entity[], hierarchyMaps: HierarchyMaps): Entity[] => {
    return entities.map(entity => {
      const parentUrns: string[] = [];
      
      entity.parentNames.forEach(parentName => {
        const parentEntity = hierarchyMaps.entitiesByName.get(parentName.toLowerCase());
        if (parentEntity && parentEntity.urn) {
          parentUrns.push(parentEntity.urn);
        }
      });

      return {
        ...entity,
        parentUrns,
      };
    });
  }, []);

  const resolveParentUrnsForLevel = useCallback((
    entities: Entity[], 
    level: number, 
    hierarchyMaps: HierarchyMaps,
  ): Entity[] => {
    const entitiesAtLevel = entities.filter(entity => entity.level === level);
    return resolveParentUrns(entitiesAtLevel, hierarchyMaps);
  }, [resolveParentUrns]);

  const validateHierarchy = useCallback((entities: Entity[]): ValidationResult => {
    const errors: ValidationError[] = [];
    const warnings: ValidationWarning[] = [];

    const circularValidation = detectCircularDependencies(entities);
    if (!circularValidation.isValid) {
      errors.push(...circularValidation.errors);
    }
    warnings.push(...circularValidation.warnings);

    const orphanValidation = findOrphanedEntities(entities);
    if (!orphanValidation.isValid) {
      errors.push(...orphanValidation.errors);
    }
    warnings.push(...orphanValidation.warnings);

    validateHierarchyDepth(entities, errors, warnings);
    validateParentChildConsistency(entities, errors, warnings);

    return {
      isValid: errors.length === 0,
      errors,
      warnings,
    };
  }, []);

  return {
    createProcessingOrder,
    resolveParentUrns,
    resolveParentUrnsForLevel,
    validateHierarchy,
  };
}

function validateHierarchyDepth(
  entities: Entity[], 
  errors: ValidationError[], 
  warnings: ValidationWarning[],
): void {
  const maxDepth = 10;
  
  entities.forEach(entity => {
    if (entity.level > maxDepth) {
      warnings.push({
        field: 'parent_nodes',
        message: `Entity "${entity.name}" has hierarchy depth of ${entity.level}, which may cause performance issues`,
        code: 'DEEP_HIERARCHY',
      });
    }
  });
}

function validateParentChildConsistency(
  entities: Entity[], 
  errors: ValidationError[], 
  warnings: ValidationWarning[],
): void {
  const entityMap = new Map<string, Entity>();
  entities.forEach(entity => {
    entityMap.set(entity.name.toLowerCase(), entity);
  });

  const calculateDepth = (entityName: string, visited = new Set<string>()): number => {
    const entity = entityMap.get(entityName.toLowerCase());
    if (!entity) return 0;
    
    if (visited.has(entityName.toLowerCase())) {
      return 0;
    }
    visited.add(entityName.toLowerCase());
    
    if (entity.parentNames.length === 0) {
      return 0;
    }
    
    const parentDepths = entity.parentNames.map(parentName => 
      calculateDepth(parentName, new Set(visited)),
    );
    return 1 + Math.max(...parentDepths, 0);
  };

  entities.forEach(entity => {
    const entityDepth = calculateDepth(entity.name);
    
    entity.parentNames.forEach(parentName => {
      const parentEntity = entityMap.get(parentName.toLowerCase());
      if (parentEntity) {
        const parentDepth = calculateDepth(parentName);
        
        if (parentDepth >= entityDepth) {
          errors.push({
            field: 'parent_nodes',
            message: `Entity "${entity.name}" has parent "${parentName}" at same or deeper level, creating invalid hierarchy`,
            code: 'INVALID_PARENT_LEVEL',
          });
        }
      }
    });
  });
}

interface ValidationError {
  field: string;
  message: string;
  code: string;
}

interface ValidationWarning {
  field: string;
  message: string;
  code: string;
}
