/**
 * Centralized URN Management
 * Consolidates all URN generation, validation, and resolution logic
 * Matches backend logic from PatchResolverUtils.java
 */

import { Entity } from '../../glossary.types';

/**
 * Entity types that support auto-generated URNs (matches backend)
 */
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
 * Centralized URN Manager class
 * Provides all URN-related operations in one place
 */
export class UrnManager {
  /**
   * Generate a GUID using crypto.randomUUID() (matches backend UUID.randomUUID())
   */
  static generateGuid(): string {
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
  static generateEntityUrn(entityType: string): string {
    if (!AUTO_GENERATE_ALLOWED_ENTITY_TYPES.has(entityType)) {
      throw new Error(
        `Auto-generated URNs are only supported for entity types: ${Array.from(AUTO_GENERATE_ALLOWED_ENTITY_TYPES).join(', ')}. ` +
        `Entity type '${entityType}' requires a structured URN. ` +
        `Please provide a specific URN for this entity type.`
      );
    }
    
    const guid = this.generateGuid();
    return `urn:li:${entityType}:${guid}`;
  }

  /**
   * Generate URN for ownership type
   * Ownership types use a different pattern: urn:li:ownershipType:{name}
   */
  static generateOwnershipTypeUrn(name: string): string {
    const sanitizedName = name.toLowerCase().replace(/[^a-z0-9]/g, '-');
    return `urn:li:ownershipType:${sanitizedName}`;
  }

  /**
   * Generate URN for glossary entity (name-based, not GUID-based)
   * Used for backward compatibility with older glossary import logic
   */
  static generateGlossaryUrn(entityType: 'glossaryTerm' | 'glossaryNode', name: string): string {
    const encodedName = encodeURIComponent(name);
    return `urn:li:${entityType}:${encodedName}`;
  }

  /**
   * Validate URN format
   * @param allowEmpty - If true, empty URNs are considered valid (will be generated)
   */
  static isValidUrn(urn: string, allowEmpty: boolean = false): boolean {
    if (!urn || urn.trim() === '') {
      return allowEmpty;
    }
    return urn.startsWith('urn:li:') && urn.split(':').length >= 4;
  }

  /**
   * Extract entity type from URN
   */
  static extractEntityTypeFromUrn(urn: string): string | null {
    if (!this.isValidUrn(urn)) return null;
    const parts = urn.split(':');
    return parts.length >= 3 ? parts[2] : null;
  }

  /**
   * Extract entity ID (GUID or name) from URN
   */
  static extractEntityIdFromUrn(urn: string): string | null {
    if (!this.isValidUrn(urn)) return null;
    const parts = urn.split(':');
    return parts.length >= 4 ? parts.slice(3).join(':') : null;
  }

  /**
   * Pre-generate URNs for all entities (new and existing)
   * Returns a map of entity ID -> URN (generated for new entities, existing for others)
   */
  static preGenerateUrns(entities: Entity[]): Map<string, string> {
    const urnMap = new Map<string, string>();
    
    entities.forEach(entity => {
      if (entity.urn) {
        // Entity already has a URN (existing/updated), use it
        urnMap.set(entity.id, entity.urn);
      } else if (entity.status === 'new') {
        // Generate new URN for new entities
        try {
          const urn = this.generateEntityUrn(entity.type);
          urnMap.set(entity.id, urn);
        } catch (error) {
          throw error;
        }
      }
    });
    
    return urnMap;
  }

  /**
   * Resolve URN for an entity (either existing or generated)
   */
  static resolveEntityUrn(entity: Entity, urnMap: Map<string, string>): string {
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
   * Create URN mapping for existing entities
   * Maps both entity ID and lowercase name to URN for flexible lookup
   */
  static createExistingEntityUrnMap(existingEntities: Entity[]): Map<string, string> {
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
   * Find entity URN by name (case-insensitive)
   */
  static findUrnByName(name: string, entities: Entity[]): string | undefined {
    const entity = entities.find(e => 
      e.name.toLowerCase() === name.toLowerCase() && e.urn
    );
    return entity?.urn;
  }

  /**
   * Check if entity type supports auto-generated URNs
   */
  static supportsAutoGeneratedUrn(entityType: string): boolean {
    return AUTO_GENERATE_ALLOWED_ENTITY_TYPES.has(entityType);
  }

  /**
   * Parse URN into components
   */
  static parseUrn(urn: string): { scheme: string; namespace: string; entityType: string; id: string } | null {
    if (!this.isValidUrn(urn)) return null;
    
    const parts = urn.split(':');
    if (parts.length < 4) return null;
    
    return {
      scheme: parts[0],
      namespace: parts[1],
      entityType: parts[2],
      id: parts.slice(3).join(':')
    };
  }

  /**
   * Build URN from components
   */
  static buildUrn(entityType: string, id: string): string {
    return `urn:li:${entityType}:${id}`;
  }

  /**
   * Validate domain URN specifically
   */
  static isDomainUrn(urn: string): boolean {
    const entityType = this.extractEntityTypeFromUrn(urn);
    return entityType === 'domain';
  }

  /**
   * Validate ownership type URN specifically
   */
  static isOwnershipTypeUrn(urn: string): boolean {
    const entityType = this.extractEntityTypeFromUrn(urn);
    return entityType === 'ownershipType';
  }
}

/**
 * Legacy function exports for backward compatibility
 * @deprecated Use UrnManager class methods instead
 */
export const generateGuid = () => UrnManager.generateGuid();
export const generateEntityUrn = (entityType: string) => UrnManager.generateEntityUrn(entityType);
export const generateOwnershipTypeUrn = (name: string) => UrnManager.generateOwnershipTypeUrn(name);
export const generateGlossaryUrn = (entityType: 'glossaryTerm' | 'glossaryNode', name: string) => 
  UrnManager.generateGlossaryUrn(entityType, name);
export const isValidUrn = (urn: string, allowEmpty?: boolean) => UrnManager.isValidUrn(urn, allowEmpty);
export const extractEntityTypeFromUrn = (urn: string) => UrnManager.extractEntityTypeFromUrn(urn);
export const preGenerateUrns = (entities: Entity[]) => UrnManager.preGenerateUrns(entities);
export const resolveEntityUrn = (entity: Entity, urnMap: Map<string, string>) => 
  UrnManager.resolveEntityUrn(entity, urnMap);
export const createExistingEntityUrnMap = (existingEntities: Entity[]) => 
  UrnManager.createExistingEntityUrnMap(existingEntities);

