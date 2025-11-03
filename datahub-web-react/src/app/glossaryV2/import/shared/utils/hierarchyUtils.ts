/**
 * Centralized hierarchy management utilities
 * Consolidates all hierarchical name parsing and parent lookup logic
 */

import { Entity } from '@app/glossaryV2/import/glossary.types';

// Fixes the bug where "Business Terms.Business Terms Nested" fails to find parent "Business Terms Nested"
export class HierarchyNameResolver {
  // Handles cases like "Business Terms.Business Terms Nested" -> "Business Terms Nested"
  static parseHierarchicalName(hierarchicalName: string): string {
    if (!hierarchicalName || hierarchicalName.trim() === '') {
      return '';
    }

    const pathParts = hierarchicalName
      .split('.')
      .map(part => part.trim())
      .filter(part => part.length > 0);
    
    return pathParts.length > 1 ? pathParts[pathParts.length - 1] : hierarchicalName;
  }

  static findParentEntity(parentName: string, entities: Entity[]): Entity | undefined {
    if (!parentName || !entities || entities.length === 0) {
      return undefined;
    }

    const actualParentName = this.parseHierarchicalName(parentName);
    return entities.find(entity => entity.name === actualParentName);
  }

  static findParentEntityCaseInsensitive(parentName: string, entities: Entity[]): Entity | undefined {
    if (!parentName || !entities || entities.length === 0) {
      return undefined;
    }

    const actualParentName = this.parseHierarchicalName(parentName);
    return entities.find(entity => 
      entity.name.toLowerCase() === actualParentName.toLowerCase(),
    );
  }

  static resolveParentUrns(
    entity: Entity,
    existingEntities: Entity[],
    generatedUrnMap: Map<string, string>,
  ): string[] {
    const parentUrns: string[] = [];
    
    entity.parentNames.forEach(parentName => {
      const existingParent = this.findParentEntity(parentName, existingEntities);
      if (existingParent && existingParent.urn) {
        parentUrns.push(existingParent.urn);
        return;
      }
      
      const actualParentName = this.parseHierarchicalName(parentName);
      const parentEntity = existingEntities.find(e => 
        e.name === actualParentName && e.status === 'new',
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

  static isHierarchicalName(hierarchicalName: string): boolean {
    return !!(hierarchicalName && hierarchicalName.includes('.'));
  }

  static getPathParts(hierarchicalName: string): string[] {
    if (!hierarchicalName) return [];
    
    return hierarchicalName
      .split('.')
      .map(part => part.trim())
      .filter(part => part.length > 0);
  }

  static buildHierarchicalName(pathParts: string[]): string {
    return pathParts.filter(part => part.trim().length > 0).join('.');
  }
}

/**
 * @deprecated Use HierarchyNameResolver.findParentEntity instead
 */
export function findParentEntity(parentPath: string, entityMap: Map<string, Entity>): Entity | null {
  const pathParts = parentPath.split('.').map(part => part.trim()).filter(Boolean);
  
  if (pathParts.length === 1) {
    return entityMap.get(pathParts[0]) || null;
  } 
    const actualParentName = pathParts[pathParts.length - 1];
    return entityMap.get(actualParentName) || null;
  
}
