/**
 * URN Generation Utilities
 * Matches backend logic from PatchResolverUtils.java
 */

import { Entity } from '../../glossary.types';

// Entity types that support auto-generated URNs (matches backend)
export const AUTO_GENERATE_ALLOWED_ENTITY_TYPES = new Set([
  'glossaryTerm',
  'glossaryNode',
  'container',
  'notebook',
  'domain',
  'dataProduct',
  'ownershipType'
]);

/**
 * Generate a GUID using crypto.randomUUID() (matches backend UUID.randomUUID())
 */
export function generateGuid(): string {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  
  // Fallback for environments without crypto.randomUUID
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

/**
 * Generate URN for entity type (matches backend logic)
 * Backend: String.format("urn:li:%s:%s", entityType, guid)
 */
export function generateEntityUrn(entityType: string): string {
  if (!AUTO_GENERATE_ALLOWED_ENTITY_TYPES.has(entityType)) {
    throw new Error(
      `Auto-generated URNs are only supported for entity types: ${Array.from(AUTO_GENERATE_ALLOWED_ENTITY_TYPES).join(', ')}. ` +
      `Entity type '${entityType}' requires a structured URN. ` +
      `Please provide a specific URN for this entity type.`
    );
  }
  
  const guid = generateGuid();
  return `urn:li:${entityType}:${guid}`;
}

/**
 * Generate URN for ownership type
 */
export function generateOwnershipTypeUrn(name: string): string {
  // Ownership types use a different pattern: urn:li:ownershipType:{name}
  const sanitizedName = name.toLowerCase().replace(/[^a-z0-9]/g, '-');
  return `urn:li:ownershipType:${sanitizedName}`;
}

/**
 * Pre-generate URNs for all new entities
 * Returns a map of entity ID -> generated URN
 */
export function preGenerateUrns(entities: Entity[]): Map<string, string> {
  const urnMap = new Map<string, string>();
  
  entities.forEach(entity => {
    // Only generate URNs for new entities that don't already have one
    if (entity.status === 'new' && !entity.urn) {
      try {
        const urn = generateEntityUrn(entity.type);
        urnMap.set(entity.id, urn);
        console.log(`Generated URN for ${entity.name} (${entity.type}): ${urn}`);
      } catch (error) {
        console.error(`Failed to generate URN for ${entity.name}:`, error);
        throw error;
      }
    }
  });
  
  return urnMap;
}

/**
 * Resolve URN for an entity (either existing or generated)
 */
export function resolveEntityUrn(entity: Entity, urnMap: Map<string, string>): string {
  // Use existing URN if available
  if (entity.urn) {
    return entity.urn;
  }
  
  // Use generated URN for new entities
  const generatedUrn = urnMap.get(entity.id);
  if (generatedUrn) {
    return generatedUrn;
  }
  
  throw new Error(`No URN available for entity ${entity.name} (${entity.id})`);
}

/**
 * Validate URN format
 */
export function isValidUrn(urn: string): boolean {
  if (!urn || urn.trim() === '') return false;
  return urn.startsWith('urn:li:') && urn.split(':').length >= 4;
}

/**
 * Extract entity type from URN
 */
export function extractEntityTypeFromUrn(urn: string): string | null {
  if (!isValidUrn(urn)) return null;
  const parts = urn.split(':');
  return parts.length >= 3 ? parts[2] : null;
}

/**
 * Create URN mapping for existing entities
 */
export function createExistingEntityUrnMap(existingEntities: Entity[]): Map<string, string> {
  const urnMap = new Map<string, string>();
  
  existingEntities.forEach(entity => {
    if (entity.urn) {
      urnMap.set(entity.id, entity.urn);
      urnMap.set(entity.name.toLowerCase(), entity.urn);
    }
  });
  
  return urnMap;
}

/**
 * Resolve parent URNs for an entity
 */
export function resolveParentUrns(
  entity: Entity, 
  existingUrnMap: Map<string, string>,
  generatedUrnMap: Map<string, string>
): string[] {
  const parentUrns: string[] = [];
  
  entity.parentNames.forEach(parentName => {
    // First check existing entities
    let parentUrn = existingUrnMap.get(parentName.toLowerCase());
    
    // Then check generated URNs for new entities
    if (!parentUrn) {
      // Find the parent entity in our import batch
      const parentEntity = Array.from(generatedUrnMap.keys()).find(id => {
        // This would need to be passed in as a parameter in real usage
        return false; // Placeholder - would need entity lookup
      });
      
      if (parentEntity) {
        parentUrn = generatedUrnMap.get(parentEntity);
      }
    }
    
    if (parentUrn) {
      parentUrns.push(parentUrn);
    } else {
      console.warn(`Parent entity "${parentName}" not found for "${entity.name}"`);
    }
  });
  
  return parentUrns;
}
