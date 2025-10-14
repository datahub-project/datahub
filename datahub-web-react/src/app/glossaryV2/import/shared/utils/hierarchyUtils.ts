/**
 * Centralized hierarchy management utilities
 * Consolidates all hierarchical name parsing and parent lookup logic
 */

import { Entity } from '../../glossary.types';

/**
 * Centralized class for handling hierarchical name parsing and parent lookups
 * Fixes the bug where "Business Terms.Business Terms Nested" fails to find parent "Business Terms Nested"
 */
export class HierarchyNameResolver {
  /**
   * Parse hierarchical name to extract the actual entity name
   * Handles cases like "Business Terms.Business Terms Nested" -> "Business Terms Nested"
   * 
   * @param hierarchicalName - The hierarchical name from CSV (e.g., "Parent.Child")
   * @returns The actual entity name (the last part of the hierarchy)
   */
  static parseHierarchicalName(hierarchicalName: string): string {
    if (!hierarchicalName || hierarchicalName.trim() === '') {
      return '';
    }

    const pathParts = hierarchicalName
      .split('.')
      .map(part => part.trim())
      .filter(part => part.length > 0);
    
    // Return the last part (actual entity name) or the original if no dots
    return pathParts.length > 1 ? pathParts[pathParts.length - 1] : hierarchicalName;
  }

  /**
   * Find parent entity by hierarchical name
   * Handles both simple names and hierarchical paths
   * 
   * @param parentName - The parent name (simple or hierarchical)
   * @param entities - Array of entities to search in
   * @returns The parent entity if found, undefined otherwise
   */
  static findParentEntity(parentName: string, entities: Entity[]): Entity | undefined {
    if (!parentName || !entities || entities.length === 0) {
      return undefined;
    }

    // Parse the hierarchical name to get the actual entity name
    const actualParentName = this.parseHierarchicalName(parentName);
    
    // Find entity by exact name match (case-sensitive for now)
    return entities.find(entity => entity.name === actualParentName);
  }

  /**
   * Find parent entity by hierarchical name with case-insensitive search
   * 
   * @param parentName - The parent name (simple or hierarchical)
   * @param entities - Array of entities to search in
   * @returns The parent entity if found, undefined otherwise
   */
  static findParentEntityCaseInsensitive(parentName: string, entities: Entity[]): Entity | undefined {
    if (!parentName || !entities || entities.length === 0) {
      return undefined;
    }

    // Parse the hierarchical name to get the actual entity name
    const actualParentName = this.parseHierarchicalName(parentName);
    
    // Find entity by case-insensitive name match
    return entities.find(entity => 
      entity.name.toLowerCase() === actualParentName.toLowerCase()
    );
  }

  /**
   * Resolve parent URNs for an entity using hierarchical name parsing
   * 
   * @param entity - The entity to resolve parent URNs for
   * @param existingEntities - Array of existing entities to search in
   * @param generatedUrnMap - Map of generated URNs for new entities
   * @returns Array of parent URNs
   */
  static resolveParentUrns(
    entity: Entity,
    existingEntities: Entity[],
    generatedUrnMap: Map<string, string>
  ): string[] {
    const parentUrns: string[] = [];
    
    entity.parentNames.forEach(parentName => {
      // First check existing entities
      const existingParent = this.findParentEntity(parentName, existingEntities);
      if (existingParent && existingParent.urn) {
        parentUrns.push(existingParent.urn);
        return;
      }
      
      // Then check generated URNs for new entities
      const actualParentName = this.parseHierarchicalName(parentName);
      const parentEntity = existingEntities.find(e => 
        e.name === actualParentName && e.status === 'new'
      );
      
      if (parentEntity) {
        const parentUrn = generatedUrnMap.get(parentEntity.id);
        if (parentUrn) {
          parentUrns.push(parentUrn);
        }
      } else {
        console.warn(`Parent entity "${parentName}" (resolved to "${actualParentName}") not found for "${entity.name}"`);
      }
    });
    
    return parentUrns;
  }

  /**
   * Check if a hierarchical name has multiple levels
   * 
   * @param hierarchicalName - The name to check
   * @returns True if the name has multiple levels (contains dots)
   */
  static isHierarchicalName(hierarchicalName: string): boolean {
    return !!(hierarchicalName && hierarchicalName.includes('.'));
  }

  /**
   * Get all path parts from a hierarchical name
   * 
   * @param hierarchicalName - The hierarchical name
   * @returns Array of path parts
   */
  static getPathParts(hierarchicalName: string): string[] {
    if (!hierarchicalName) return [];
    
    return hierarchicalName
      .split('.')
      .map(part => part.trim())
      .filter(part => part.length > 0);
  }

  /**
   * Build hierarchical name from path parts
   * 
   * @param pathParts - Array of path parts
   * @returns Hierarchical name string
   */
  static buildHierarchicalName(pathParts: string[]): string {
    return pathParts.filter(part => part.trim().length > 0).join('.');
  }
}

/**
 * Legacy function for backward compatibility
 * @deprecated Use HierarchyNameResolver.findParentEntity instead
 */
export function findParentEntity(parentPath: string, entityMap: Map<string, Entity>): Entity | null {
  const pathParts = parentPath.split('.').map(part => part.trim()).filter(Boolean);
  
  if (pathParts.length === 1) {
    // Simple parent name
    return entityMap.get(pathParts[0]) || null;
  } else {
    // Hierarchical parent name - find the last part (the actual parent)
    const actualParentName = pathParts[pathParts.length - 1];
    return entityMap.get(actualParentName) || null;
  }
}
