/**
 * Utility functions for Glossary Import feature
 */

import { EntityData, Entity, GraphQLEntity, ValidationResult, ValidationError, ValidationWarning } from './glossary.types';

/**
 * Generate a unique ID for an entity based on its hierarchy path
 */
export function generateEntityId(entity: EntityData, parentNames: string[] = []): string {
  const hierarchyPath = [...parentNames, entity.name].join(' > ');
  return hierarchyPath.toLowerCase().replace(/[^a-z0-9\s>]/g, '').replace(/\s+/g, '-');
}

/**
 * Parse comma-separated string into array
 */
export function parseCommaSeparated(value: string): string[] {
  if (!value || value.trim() === '') return [];
  return value.split(',').map(item => item.trim()).filter(item => item.length > 0);
}

/**
 * Convert array to comma-separated string
 */
export function toCommaSeparated(values: string[]): string {
  return values.filter(value => value && value.trim() !== '').join(', ');
}

/**
 * Validate URN format
 */
export function isValidUrn(urn: string): boolean {
  if (!urn || urn.trim() === '') return true; // Empty URN is valid (will be generated)
  return urn.startsWith('urn:li:') && urn.includes(':');
}

/**
 * Generate URN for glossary entity
 */
export function generateGlossaryUrn(entityType: 'glossaryTerm' | 'glossaryNode', name: string): string {
  const encodedName = encodeURIComponent(name);
  return `urn:li:${entityType}:${encodedName}`;
}

/**
 * Extract entity type from URN
 */
export function extractEntityTypeFromUrn(urn: string): 'glossaryTerm' | 'glossaryNode' | null {
  if (!urn.startsWith('urn:li:')) return null;
  const parts = urn.split(':');
  if (parts.length < 4) return null;
  const entityType = parts[2];
  return entityType === 'glossaryTerm' || entityType === 'glossaryNode' ? entityType : null;
}

/**
 * Validate entity name
 */
export function validateEntityName(name: string): ValidationResult {
  const errors: ValidationError[] = [];
  const warnings: ValidationWarning[] = [];

  if (!name || name.trim() === '') {
    errors.push({
      field: 'name',
      message: 'Entity name is required',
      code: 'REQUIRED'
    });
  } else if (name.length > 100) {
    errors.push({
      field: 'name',
      message: 'Entity name must be less than 100 characters',
      code: 'MAX_LENGTH'
    });
  } else if (name.trim() !== name) {
    warnings.push({
      field: 'name',
      message: 'Entity name has leading or trailing whitespace',
      code: 'WHITESPACE'
    });
  }

  return {
    isValid: errors.length === 0,
    errors,
    warnings
  };
}

/**
 * Validate entity type
 */
export function validateEntityType(entityType: string): ValidationResult {
  const errors: ValidationError[] = [];
  const warnings: ValidationWarning[] = [];

  if (!entityType || entityType.trim() === '') {
    errors.push({
      field: 'entity_type',
      message: 'Entity type is required',
      code: 'REQUIRED'
    });
  } else if (entityType !== 'glossaryTerm' && entityType !== 'glossaryNode') {
    errors.push({
      field: 'entity_type',
      message: 'Entity type must be either "glossaryTerm" or "glossaryNode"',
      code: 'INVALID_TYPE'
    });
  }

  return {
    isValid: errors.length === 0,
    errors,
    warnings
  };
}

/**
 * Validate parent nodes string
 */
export function validateParentNodes(parentNodes: string): ValidationResult {
  const errors: ValidationError[] = [];
  const warnings: ValidationWarning[] = [];

  if (parentNodes && parentNodes.trim() !== '') {
    const parents = parseCommaSeparated(parentNodes);
    if (parents.length > 10) {
      warnings.push({
        field: 'parent_nodes',
        message: 'Having more than 10 parent nodes may cause performance issues',
        code: 'TOO_MANY_PARENTS'
      });
    }
  }

  return {
    isValid: errors.length === 0,
    errors,
    warnings
  };
}

/**
 * Validate domain URN
 */
export function validateDomainUrn(domainUrn: string): ValidationResult {
  const errors: ValidationError[] = [];
  const warnings: ValidationWarning[] = [];

  if (domainUrn && domainUrn.trim() !== '') {
    if (!domainUrn.startsWith('urn:li:domain:')) {
      errors.push({
        field: 'domain_urn',
        message: 'Domain URN must start with "urn:li:domain:"',
        code: 'INVALID_DOMAIN_URN'
      });
    }
  }

  return {
    isValid: errors.length === 0,
    errors,
    warnings
  };
}

/**
 * Validate ownership string
 */
export function validateOwnership(ownership: string): ValidationResult {
  const errors: ValidationError[] = [];
  const warnings: ValidationWarning[] = [];

  if (ownership && ownership.trim() !== '') {
    // Basic validation for ownership format
    // This could be enhanced to validate specific ownership patterns
    const owners = parseCommaSeparated(ownership);
    if (owners.length > 20) {
      warnings.push({
        field: 'ownership',
        message: 'Having more than 20 owners may cause performance issues',
        code: 'TOO_MANY_OWNERS'
      });
    }
  }

  return {
    isValid: errors.length === 0,
    errors,
    warnings
  };
}

/**
 * Validate custom properties string
 */
export function validateCustomProperties(customProperties: string): ValidationResult {
  const errors: ValidationError[] = [];
  const warnings: ValidationWarning[] = [];

  if (customProperties && customProperties.trim() !== '') {
    try {
      // Try to parse as JSON if it looks like JSON
      if (customProperties.trim().startsWith('{') || customProperties.trim().startsWith('[')) {
        JSON.parse(customProperties);
      }
    } catch (error) {
      warnings.push({
        field: 'custom_properties',
        message: 'Custom properties should be valid JSON format',
        code: 'INVALID_JSON'
      });
    }
  }

  return {
    isValid: errors.length === 0,
    errors,
    warnings
  };
}

/**
 * Validate URL format
 */
export function validateUrl(url: string): ValidationResult {
  const errors: ValidationError[] = [];
  const warnings: ValidationWarning[] = [];

  if (url && url.trim() !== '') {
    try {
      new URL(url);
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
}

/**
 * Check for duplicate names in entity data
 */
export function findDuplicateNames(entities: EntityData[]): ValidationResult {
  const errors: ValidationError[] = [];
  const warnings: ValidationWarning[] = [];
  const nameCounts = new Map<string, number[]>();

  entities.forEach((entity, index) => {
    const name = entity.name?.toLowerCase();
    if (name) {
      if (!nameCounts.has(name)) {
        nameCounts.set(name, []);
      }
      nameCounts.get(name)!.push(index);
    }
  });

  nameCounts.forEach((indices, name) => {
    if (indices.length > 1) {
      indices.forEach(rowIndex => {
        errors.push({
          field: 'name',
          message: `Duplicate name "${name}" found in rows ${indices.join(', ')}`,
          code: 'DUPLICATE_NAME'
        });
      });
    }
  });

  return {
    isValid: errors.length === 0,
    errors,
    warnings
  };
}

/**
 * Convert GraphQL entity to our Entity format
 */
export function convertGraphQLEntityToEntity(graphqlEntity: GraphQLEntity): Entity {
  const isGlossaryTerm = graphqlEntity.__typename === 'GlossaryTerm';
  const name = isGlossaryTerm 
    ? graphqlEntity.hierarchicalName || graphqlEntity.properties?.name || ''
    : graphqlEntity.properties?.name || '';

  const parentNames = graphqlEntity.parentNodes?.nodes?.map(node => node.properties.name) || [];

  return {
    id: generateEntityId({
      entity_type: isGlossaryTerm ? 'glossaryTerm' : 'glossaryNode',
      urn: graphqlEntity.urn,
      name,
      description: graphqlEntity.properties?.description || '',
      term_source: graphqlEntity.properties?.termSource || '',
      source_ref: graphqlEntity.properties?.sourceRef || '',
      source_url: graphqlEntity.properties?.sourceUrl || '',
      ownership: '',
      parent_nodes: toCommaSeparated(parentNames),
      related_contains: '',
      related_inherits: '',
      domain_urn: graphqlEntity.domain?.domain.urn || '',
      domain_name: graphqlEntity.domain?.domain.properties.name || '',
      custom_properties: graphqlEntity.properties?.customProperties?.map(cp => `${cp.key}:${cp.value}`).join(',') || ''
    }, parentNames),
    name,
    type: isGlossaryTerm ? 'glossaryTerm' : 'glossaryNode',
    urn: graphqlEntity.urn,
    parentNames,
    parentUrns: graphqlEntity.parentNodes?.nodes?.map(node => node.urn) || [],
    level: 0, // Will be calculated later
    data: {
      entity_type: isGlossaryTerm ? 'glossaryTerm' : 'glossaryNode',
      urn: graphqlEntity.urn,
      name,
      description: graphqlEntity.properties?.description || '',
      term_source: graphqlEntity.properties?.termSource || '',
      source_ref: graphqlEntity.properties?.sourceRef || '',
      source_url: graphqlEntity.properties?.sourceUrl || '',
      ownership: '',
      parent_nodes: toCommaSeparated(parentNames),
      related_contains: '',
      related_inherits: '',
      domain_urn: graphqlEntity.domain?.domain.urn || '',
      domain_name: graphqlEntity.domain?.domain.properties.name || '',
      custom_properties: graphqlEntity.properties?.customProperties?.map(cp => `${cp.key}:${cp.value}`).join(',') || ''
    },
    status: 'existing'
  };
}

/**
 * Calculate hierarchy level for an entity
 * Simple approach: level = number of parents
 */
export function calculateHierarchyLevel(entity: Entity, allEntities: Entity[]): number {
  return entity.parentNames.length;
}

/**
 * Sort entities by hierarchy level
 */
export function sortEntitiesByHierarchy(entities: Entity[]): Entity[] {
  // First calculate levels for all entities
  const entitiesWithLevels = entities.map(entity => ({
    ...entity,
    level: calculateHierarchyLevel(entity, entities)
  }));

  // Sort by level, then by name
  return entitiesWithLevels.sort((a, b) => {
    if (a.level !== b.level) {
      return a.level - b.level;
    }
    return a.name.localeCompare(b.name);
  });
}

/**
 * Check for circular dependencies in hierarchy
 */
export function detectCircularDependencies(entities: Entity[]): ValidationResult {
  const errors: ValidationError[] = [];
  const warnings: ValidationWarning[] = [];

  entities.forEach(entity => {
    const visited = new Set<string>();
    const stack = new Set<string>();
    
    function hasCycle(entityName: string): boolean {
      if (stack.has(entityName)) return true;
      if (visited.has(entityName)) return false;
      
      visited.add(entityName);
      stack.add(entityName);
      
      const entity = entities.find(e => e.name === entityName);
      if (entity) {
        for (const parentName of entity.parentNames) {
          if (hasCycle(parentName)) return true;
        }
      }
      
      stack.delete(entityName);
      return false;
    }
    
    if (hasCycle(entity.name)) {
      errors.push({
        field: 'parent_nodes',
        message: `Circular dependency detected involving entity "${entity.name}"`,
        code: 'CIRCULAR_DEPENDENCY'
      });
    }
  });

  return {
    isValid: errors.length === 0,
    errors,
    warnings
  };
}

/**
 * Find orphaned entities (entities with parents that don't exist)
 */
export function findOrphanedEntities(entities: Entity[]): ValidationResult {
  const errors: ValidationError[] = [];
  const warnings: ValidationWarning[] = [];

  const entityNames = new Set(entities.map(e => e.name));

  entities.forEach(entity => {
    entity.parentNames.forEach(parentName => {
      if (!entityNames.has(parentName)) {
        warnings.push({
          field: 'parent_nodes',
          message: `Entity "${entity.name}" references non-existent parent "${parentName}"`,
          code: 'ORPHANED_ENTITY'
        });
      }
    });
  });

  return {
    isValid: errors.length === 0,
    errors,
    warnings
  };
}

/**
 * Debounce function for performance optimization
 */
export function debounce<T extends (...args: any[]) => any>(
  func: T,
  wait: number
): (...args: Parameters<T>) => void {
  let timeout: NodeJS.Timeout;
  return (...args: Parameters<T>) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => func(...args), wait);
  };
}

/**
 * Throttle function for performance optimization
 */
export function throttle<T extends (...args: any[]) => any>(
  func: T,
  limit: number
): (...args: Parameters<T>) => void {
  let inThrottle: boolean;
  return (...args: Parameters<T>) => {
    if (!inThrottle) {
      func(...args);
      inThrottle = true;
      setTimeout(() => inThrottle = false, limit);
    }
  };
}
