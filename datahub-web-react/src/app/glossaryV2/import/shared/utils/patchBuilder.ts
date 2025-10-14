/**
 * Centralized Patch Builder
 * Consolidates all patch operation creation logic
 */

import { Entity, PatchOperation } from '../../glossary.types';
import { UrnManager } from './urnManager';
import { HierarchyNameResolver } from './hierarchyUtils';
import { createOwnershipPatchOperations } from './ownershipParsingUtils';

/**
 * Input type for ownership type patches
 */
export interface OwnershipTypeInput {
  name: string;
  description: string;
  urn: string;
}

/**
 * Array primary key input for patch operations
 */
export interface ArrayPrimaryKeyInput {
  arrayField: string;
  keys: string[];
}

/**
 * Input type for comprehensive patch operations
 */
export interface ComprehensivePatchInput {
  urn?: string;
  entityType: string;
  aspectName: string;
  patch: PatchOperation[];
  arrayPrimaryKeys?: ArrayPrimaryKeyInput[];
  forceGenericPatch?: boolean;
}

/**
 * Centralized Patch Builder class
 * Provides all patch creation operations in one place
 */
export class PatchBuilder {
  /**
   * Create patches for ownership types
   */
  static createOwnershipTypePatches(ownershipTypes: OwnershipTypeInput[]): ComprehensivePatchInput[] {
    return ownershipTypes.map(ownershipType => ({
      urn: ownershipType.urn,
      entityType: 'ownershipType',
      aspectName: 'ownershipTypeInfo',
      patch: [
        { op: 'ADD' as const, path: '/name', value: ownershipType.name },
        { op: 'ADD' as const, path: '/description', value: ownershipType.description },
        { op: 'ADD' as const, path: '/created', value: JSON.stringify({
          time: Date.now(),
          actor: 'urn:li:corpuser:datahub' // Will be replaced with actual user
        })},
        { op: 'ADD' as const, path: '/lastModified', value: JSON.stringify({
          time: Date.now(),
          actor: 'urn:li:corpuser:datahub' // Will be replaced with actual user
        })}
      ]
    }));
  }

  /**
   * Check if entity info has changed between existing and new entity
   */
  static hasEntityInfoChanged(newEntity: Entity, existingEntity: Entity): boolean {
    // Compare name
    if (newEntity.name !== existingEntity.name) return true;
    
    // Compare description
    const newDescription = newEntity.data.description || '';
    const existingDescription = existingEntity.data.description || '';
    if (newDescription !== existingDescription) return true;
    
    // Compare term_source (for glossary terms)
    const newTermSource = newEntity.data.term_source || '';
    const existingTermSource = existingEntity.data.term_source || '';
    if (newTermSource !== existingTermSource) return true;
    
    // Compare source_ref
    const newSourceRef = newEntity.data.source_ref || '';
    const existingSourceRef = existingEntity.data.source_ref || '';
    if (newSourceRef !== existingSourceRef) return true;
    
    // Compare source_url
    const newSourceUrl = newEntity.data.source_url || '';
    const existingSourceUrl = existingEntity.data.source_url || '';
    if (newSourceUrl !== existingSourceUrl) return true;
    
    // Compare parent names
    const newParentNames = newEntity.parentNames || [];
    const existingParentNames = existingEntity.parentNames || [];
    if (JSON.stringify(newParentNames) !== JSON.stringify(existingParentNames)) return true;
    
    return false;
  }

  /**
   * Create entity patches in hierarchical order
   */
  static createEntityPatches(
    entities: Entity[],
    urnMap: Map<string, string>,
    existingEntities: Entity[] = []
  ): ComprehensivePatchInput[] {
    return entities.map(entity => {
      const urn = UrnManager.resolveEntityUrn(entity, urnMap);
      const aspectName = entity.type === 'glossaryTerm' ? 'glossaryTermInfo' : 'glossaryNodeInfo';
      
      // Find existing entity to compare
      const existingEntity = existingEntities.find(e => e.urn === entity.urn);
      
      // Only create patch if entity is new or has changes
      const shouldPatch = entity.status === 'new' || 
                         entity.status === 'updated' ||
                         (existingEntity && this.hasEntityInfoChanged(entity, existingEntity));
      
      if (!shouldPatch && entity.status === 'existing') {
        // No changes needed for this entity
        return null;
      }

      const patch: PatchOperation[] = [];

      // Add basic entity info
      patch.push({ op: 'ADD' as const, path: '/name', value: entity.name });

      // Add description if present
      if (entity.data.description) {
        const fieldName = entity.type === 'glossaryTerm' ? 'definition' : 'definition';
        patch.push({ op: 'ADD' as const, path: `/${fieldName}`, value: entity.data.description });
      }

      // Add term_source for glossary terms
      if (entity.type === 'glossaryTerm' && entity.data.term_source) {
        patch.push({ op: 'ADD' as const, path: '/termSource', value: entity.data.term_source });
      }

      // Add source_ref if present
      if (entity.data.source_ref) {
        patch.push({ op: 'ADD' as const, path: '/sourceRef', value: entity.data.source_ref });
      }

      // Add source_url if present
      if (entity.data.source_url) {
        patch.push({ op: 'ADD' as const, path: '/sourceUrl', value: entity.data.source_url });
      }

      // Add custom properties if present
      if (entity.data.custom_properties) {
        try {
          const customProps = typeof entity.data.custom_properties === 'string'
            ? JSON.parse(entity.data.custom_properties)
            : entity.data.custom_properties;

          Object.entries(customProps).forEach(([key, value]) => {
            patch.push({
              op: 'ADD' as const,
              path: `/customProperties/${key}`,
              value: JSON.stringify(value)
            });
          });
        } catch (error) {
          console.warn(`Failed to parse custom properties for entity ${entity.name}:`, error);
        }
      }

      // Add parent relationships directly to the entity patch
      if (entity.parentNames && entity.parentNames.length > 0) {
        // For parent relationships, we only support one parent per entity
        // Take the first parent if multiple are specified
        const parentName = entity.parentNames[0];
        
        // Use HierarchyNameResolver to find parent entity (handles hierarchical names)
        const parentEntity = HierarchyNameResolver.findParentEntity(parentName, existingEntities);
        
        let parentUrn: string | undefined;
        
        if (parentEntity) {
          // Parent found in existing entities
          parentUrn = parentEntity.urn;
        } else {
          // Check entities in current batch
          const actualParentName = HierarchyNameResolver.parseHierarchicalName(parentName);
          const batchParent = entities.find(e => e.name === actualParentName && e.status === 'new');
          if (batchParent) {
            parentUrn = urnMap.get(batchParent.id);
          }
        }
        
        if (parentUrn) {
          patch.push({
            op: 'ADD' as const,
            path: '/parentNode',
            value: parentUrn
          });
        } else {
          const actualParentName = HierarchyNameResolver.parseHierarchicalName(parentName);
          console.warn(`Parent entity "${parentName}" (resolved to "${actualParentName}") not found for "${entity.name}"`);
        }
      }

      return {
        urn,
        entityType: entity.type,
        aspectName,
        patch
      };
    }).filter((patch): patch is ComprehensivePatchInput => patch !== null);
  }

  /**
   * Create ownership patches for entities
   */
  static createOwnershipPatches(
    entities: Entity[],
    urnMap: Map<string, string>,
    ownershipTypeMap: Map<string, string>
  ): ComprehensivePatchInput[] {
    const ownershipPatches: ComprehensivePatchInput[] = [];

    entities.forEach(entity => {
      try {
        const urn = UrnManager.resolveEntityUrn(entity, urnMap);
        const patches = createOwnershipPatchOperations(entity.data, ownershipTypeMap);

        if (patches.length > 0) {
          ownershipPatches.push({
            urn,
            entityType: entity.type,
            aspectName: 'ownership',
            patch: patches
          });
        }
      } catch (error) {
        console.error(`Failed to create ownership patches for ${entity.name}:`, error);
      }
    });

    return ownershipPatches;
  }

  /**
   * Create related term patches
   */
  static createRelatedTermPatches(
    entities: Entity[],
    urnMap: Map<string, string>
  ): ComprehensivePatchInput[] {
    const relatedTermPatches: ComprehensivePatchInput[] = [];

    entities.forEach(entity => {
      const relatedTerms: { [relationshipType: string]: string[] } = {};

      // Parse related_contains
      if (entity.data.related_contains) {
        const containsList = entity.data.related_contains.split(',').map(name => name.trim()).filter(Boolean);
        containsList.forEach(relatedName => {
          // Find related entity
          const relatedEntity = entities.find(e => e.name === relatedName);
          if (relatedEntity) {
            const relatedUrn = UrnManager.resolveEntityUrn(relatedEntity, urnMap);
            if (!relatedTerms['contains']) relatedTerms['contains'] = [];
            relatedTerms['contains'].push(relatedUrn);
          } else {
            console.warn(`🔗 Related entity not found for contains: "${relatedName}"`);
          }
        });
      }

      // Parse related_inherits
      if (entity.data.related_inherits) {
        const inheritsList = entity.data.related_inherits.split(',').map(name => name.trim()).filter(Boolean);
        inheritsList.forEach(relatedName => {
          // Find related entity
          const relatedEntity = entities.find(e => e.name === relatedName);
          if (relatedEntity) {
            const relatedUrn = UrnManager.resolveEntityUrn(relatedEntity, urnMap);
            if (!relatedTerms['inherits']) relatedTerms['inherits'] = [];
            relatedTerms['inherits'].push(relatedUrn);
          } else {
            console.warn(`🔗 Related entity not found for inherits: "${relatedName}"`);
          }
        });
      }

      // Create patches for related terms
      if (Object.keys(relatedTerms).length > 0) {
        const currentUrn = UrnManager.resolveEntityUrn(entity, urnMap);
        const patches: PatchOperation[] = [];

        Object.entries(relatedTerms).forEach(([relationshipType, relatedUrns]) => {
          relatedUrns.forEach(relatedUrn => {
            patches.push({
              op: 'ADD' as const,
              path: `/isRelatedTerms/${relatedUrn}/${relationshipType}`,
              value: '{}'
            });
          });
        });

        if (patches.length > 0) {
          relatedTermPatches.push({
            urn: currentUrn,
            entityType: entity.type,
            aspectName: 'glossaryRelatedTerms',
            patch: patches
          });
        }
      }
    });

    return relatedTermPatches;
  }

  /**
   * Create domain assignment patches
   */
  static createDomainAssignmentPatches(
    entities: Entity[],
    urnMap: Map<string, string>
  ): ComprehensivePatchInput[] {
    const domainPatches: ComprehensivePatchInput[] = [];

    entities.forEach(entity => {
      if (entity.data.domain_urn) {
        const urn = UrnManager.resolveEntityUrn(entity, urnMap);

        domainPatches.push({
          urn,
          entityType: entity.type,
          aspectName: 'domains',
          patch: [
            {
              op: 'ADD' as const,
              path: '/domains/0',
              value: entity.data.domain_urn
            }
          ]
        });
      }
    });

    return domainPatches;
  }
}

/**
 * Legacy function exports for backward compatibility
 * @deprecated Use PatchBuilder class methods instead
 */
export const createOwnershipTypePatches = (ownershipTypes: OwnershipTypeInput[]) =>
  PatchBuilder.createOwnershipTypePatches(ownershipTypes);

export const createEntityPatches = (
  entities: Entity[],
  urnMap: Map<string, string>,
  existingEntities: Entity[] = []
) => PatchBuilder.createEntityPatches(entities, urnMap, existingEntities);

export const createOwnershipPatches = (
  entities: Entity[],
  urnMap: Map<string, string>,
  ownershipTypeMap: Map<string, string>
) => PatchBuilder.createOwnershipPatches(entities, urnMap, ownershipTypeMap);

export const createRelatedTermPatches = (
  entities: Entity[],
  urnMap: Map<string, string>
) => PatchBuilder.createRelatedTermPatches(entities, urnMap);

export const createDomainAssignmentPatches = (
  entities: Entity[],
  urnMap: Map<string, string>
) => PatchBuilder.createDomainAssignmentPatches(entities, urnMap);

