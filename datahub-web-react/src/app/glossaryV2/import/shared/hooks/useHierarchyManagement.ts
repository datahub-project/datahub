/**
 * Hook for hierarchy management and validation
 */

import { useCallback } from 'react';
import { 
  Entity, 
  HierarchyMaps, 
  ValidationResult,
  UseHierarchyManagementReturn 
} from '../../glossary.types';
import {
  sortEntitiesByHierarchy,
  detectCircularDependencies,
  findOrphanedEntities,
  parseCommaSeparated
} from '../../glossary.utils';

export function useHierarchyManagement(): UseHierarchyManagementReturn {
  /**
   * Create processing order respecting hierarchy levels
   */
  const createProcessingOrder = useCallback((entities: Entity[]): Entity[] => {
    // First sort entities by hierarchy level (this calculates the levels)
    const sortedEntities = sortEntitiesByHierarchy(entities);
    
    // Then validate hierarchy with calculated levels
    const validation = validateHierarchy(sortedEntities);
    if (!validation.isValid) {
      console.warn('Hierarchy validation failed:', validation.errors);
    }

    return sortedEntities;
  }, []);

  /**
   * Resolve parent URNs for all entities
   */
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
        parentUrns
      };
    });
  }, []);

  /**
   * Resolve parent URNs for entities at a specific hierarchy level
   */
  const resolveParentUrnsForLevel = useCallback((
    entities: Entity[], 
    level: number, 
    hierarchyMaps: HierarchyMaps
  ): Entity[] => {
    const entitiesAtLevel = entities.filter(entity => entity.level === level);
    return resolveParentUrns(entitiesAtLevel, hierarchyMaps);
  }, [resolveParentUrns]);

  /**
   * Validate hierarchy for circular dependencies and orphaned entities
   */
  const validateHierarchy = useCallback((entities: Entity[]): ValidationResult => {
    const errors: ValidationError[] = [];
    const warnings: ValidationWarning[] = [];

    // Check for circular dependencies
    const circularValidation = detectCircularDependencies(entities);
    if (!circularValidation.isValid) {
      errors.push(...circularValidation.errors);
    }
    warnings.push(...circularValidation.warnings);

    // Check for orphaned entities
    const orphanValidation = findOrphanedEntities(entities);
    if (!orphanValidation.isValid) {
      errors.push(...orphanValidation.errors);
    }
    warnings.push(...orphanValidation.warnings);

    // Additional hierarchy validations
    validateHierarchyDepth(entities, errors, warnings);
    validateParentChildConsistency(entities, errors, warnings);

    return {
      isValid: errors.length === 0,
      errors,
      warnings
    };
  }, []);

  return {
    createProcessingOrder,
    resolveParentUrns,
    resolveParentUrnsForLevel,
    validateHierarchy
  };
}

/**
 * Validate hierarchy depth (prevent too deep nesting)
 */
function validateHierarchyDepth(
  entities: Entity[], 
  errors: ValidationError[], 
  warnings: ValidationWarning[]
): void {
  const maxDepth = 10; // Configurable maximum depth
  
  entities.forEach(entity => {
    if (entity.level > maxDepth) {
      warnings.push({
        field: 'parent_nodes',
        message: `Entity "${entity.name}" has hierarchy depth of ${entity.level}, which may cause performance issues`,
        code: 'DEEP_HIERARCHY'
      });
    }
  });
}

/**
 * Validate parent-child consistency
 */
function validateParentChildConsistency(
  entities: Entity[], 
  errors: ValidationError[], 
  warnings: ValidationWarning[]
): void {
  const entityMap = new Map<string, Entity>();
  entities.forEach(entity => {
    entityMap.set(entity.name.toLowerCase(), entity);
  });

  entities.forEach(entity => {
    entity.parentNames.forEach(parentName => {
      const parentEntity = entityMap.get(parentName.toLowerCase());
      if (parentEntity) {
        // Check if parent is actually a parent (not a child)
        if (parentEntity.level >= entity.level) {
          errors.push({
            field: 'parent_nodes',
            message: `Entity "${entity.name}" has parent "${parentName}" at same or deeper level, creating invalid hierarchy`,
            code: 'INVALID_PARENT_LEVEL'
          });
        }
      }
    });
  });
}

// Import types that are used in this file
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
