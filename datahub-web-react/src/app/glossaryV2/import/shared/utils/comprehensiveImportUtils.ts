/**
 * Comprehensive Import Utilities
 * Handles single GraphQL call with hierarchical ordering and dependency resolution
 */

import { Entity, EntityData, PatchOperation } from '../../glossary.types';
import { UrnManager } from './urnManager';
import { sortEntitiesByHierarchy } from '../../glossary.utils';
import { parseOwnershipFromColumns, createOwnershipPatchOperations } from './ownershipParsingUtils';
import { HierarchyNameResolver } from './hierarchyUtils';

// Types for comprehensive import
export interface ComprehensivePatchInput {
  urn?: string;
  entityType: string;
  aspectName: string;
  patch: PatchOperation[];
  arrayPrimaryKeys?: ArrayPrimaryKeyInput[];
  forceGenericPatch?: boolean;
}

export interface ArrayPrimaryKeyInput {
  arrayField: string;
  keys: string[];
}

export interface OwnershipTypeInput {
  name: string;
  description: string;
  urn: string;
}

export interface ComprehensiveImportPlan {
  ownershipTypes: OwnershipTypeInput[];
  entities: Entity[];
  urnMap: Map<string, string>; // URN mapping for consistent URN usage
  ownershipPatches: ComprehensivePatchInput[];
  parentRelationshipPatches: ComprehensivePatchInput[];
  relatedTermPatches: ComprehensivePatchInput[];
  domainAssignmentPatches: ComprehensivePatchInput[];
}

/**
 * Create comprehensive import plan with proper ordering
 */
export function createComprehensiveImportPlan(
  entities: Entity[],
  existingEntities: Entity[],
  existingOwnershipTypes: Map<string, string>
): ComprehensiveImportPlan {
  // 1. Pre-generate URNs for all new entities
  const urnMap = UrnManager.preGenerateUrns(entities);
  
  // 2. Sort entities by hierarchy (parents before children)
  const sortedEntities = sortEntitiesByHierarchy(entities);
  
  // 3. Extract and create ownership types
  const ownershipTypes = extractOwnershipTypes(entities, existingOwnershipTypes);
  
  // 4. Create all patch inputs in proper order
  const plan: ComprehensiveImportPlan = {
    ownershipTypes,
    entities: sortedEntities,
    urnMap, // Include the URN map in the plan
    ownershipPatches: createOwnershipPatches(sortedEntities, urnMap, existingOwnershipTypes, existingEntities),
    parentRelationshipPatches: [], // Parent relationships are now handled directly in createEntityPatches
    relatedTermPatches: createRelatedTermPatches(sortedEntities, urnMap, existingEntities),
    domainAssignmentPatches: createDomainAssignmentPatches(sortedEntities, urnMap, existingEntities)
  };
  
  return plan;
}

/**
 * Extract ownership types that need to be created
 */
function extractOwnershipTypes(
  entities: Entity[],
  existingOwnershipTypes: Map<string, string>
): OwnershipTypeInput[] {
  const ownershipTypeNames = new Set<string>();
  
  entities.forEach(entity => {
    // Extract from ownership_users column
    if (entity.data.ownership_users) {
      const parsed = parseOwnershipFromColumns(entity.data.ownership_users, '');
      parsed.forEach(({ ownershipTypeName }) => {
        if (!existingOwnershipTypes.has(ownershipTypeName.toLowerCase())) {
          ownershipTypeNames.add(ownershipTypeName);
        }
      });
    }
    
    // Extract from ownership_groups column
    if (entity.data.ownership_groups) {
      const parsed = parseOwnershipFromColumns('', entity.data.ownership_groups);
      parsed.forEach(({ ownershipTypeName }) => {
        if (!existingOwnershipTypes.has(ownershipTypeName.toLowerCase())) {
          ownershipTypeNames.add(ownershipTypeName);
        }
      });
    }
  });
  
  return Array.from(ownershipTypeNames).map(name => ({
    name,
    description: `Custom ownership type: ${name}`,
    urn: UrnManager.generateOwnershipTypeUrn(name)
  }));
}

/**
 * Create ownership type patches
 */
function createOwnershipTypePatches(ownershipTypes: OwnershipTypeInput[]): ComprehensivePatchInput[] {
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
function hasEntityInfoChanged(newEntity: Entity, existingEntity: Entity): boolean {
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
function createEntityPatches(
  entities: Entity[],
  urnMap: Map<string, string>,
  existingEntities: Entity[] = []
): ComprehensivePatchInput[] {
  return entities.map(entity => {
    const urn = UrnManager.resolveEntityUrn(entity, urnMap);
    const aspectName = entity.type === 'glossaryTerm' ? 'glossaryTermInfo' : 'glossaryNodeInfo';
    const isNewEntity = entity.status === 'new';
    
    // For existing entities, check if entity info has actually changed
    if (!isNewEntity) {
      const existingEntity = existingEntities.find(existing => existing.name === entity.name);
      if (existingEntity) {
        const entityInfoChanged = hasEntityInfoChanged(entity, existingEntity);
        
        // Check if any children have changes (including grandchildren)
        const hasChildrenWithChanges = entities.some(child => 
          child.parentNames && 
          child.parentNames.includes(entity.name) &&
          (child.status === 'new' || hasEntityInfoChanged(child, existingEntities.find(e => e.name === child.name) || child))
        );
        
        if (!entityInfoChanged && !hasChildrenWithChanges) {
          // Return empty patch for unchanged entities
          return {
            urn,
            entityType: entity.type,
            aspectName,
            patch: []
          };
        }
      }
    }
    
    const patch: PatchOperation[] = [];
    
    // For new entities, always provide required fields (mimic createGlossaryTerm behavior)
    // For existing entities, only provide fields that have values
    if (isNewEntity) {
      // Required fields for new entities
      patch.push({ op: 'ADD' as const, path: '/name', value: entity.name || null });
      patch.push({ op: 'ADD' as const, path: '/definition', value: entity.data.description || "" });
      
      if (entity.type === 'glossaryTerm') {
        patch.push({ op: 'ADD' as const, path: '/termSource', value: entity.data.term_source || 'INTERNAL' });
      }
    } else {
      // For existing entities, only patch fields that have actual values
      if (entity.name) {
        patch.push({ op: 'ADD' as const, path: '/name', value: entity.name });
      }
      if (entity.data.description) {
        patch.push({ op: 'ADD' as const, path: '/definition', value: entity.data.description });
      }
      if (entity.type === 'glossaryTerm' && entity.data.term_source) {
        patch.push({ op: 'ADD' as const, path: '/termSource', value: entity.data.term_source });
      }
    }
    
    // Optional fields - only add if they have values
    if (entity.data.source_ref) {
      patch.push({ op: 'ADD' as const, path: '/sourceRef', value: entity.data.source_ref });
    }
    if (entity.data.source_url) {
      patch.push({ op: 'ADD' as const, path: '/sourceUrl', value: entity.data.source_url });
    }
    
    // Add custom properties
    if (entity.data.custom_properties) {
      try {
        const customProps = typeof entity.data.custom_properties === 'string' 
          ? JSON.parse(entity.data.custom_properties)
          : entity.data.custom_properties;
        
        Object.entries(customProps).forEach(([key, value]) => {
          patch.push({ 
            op: 'ADD' as const, 
            path: `/customProperties/${key}`,
            value: JSON.stringify(String(value))
          });
        });
      } catch (error) {
        console.warn(`Failed to parse custom properties for ${entity.name}:`, error);
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
      urn, // Always provide URN for batching
      entityType: entity.type,
      aspectName,
      patch
    };
  });
}

/**
 * Check if ownership data has changed between existing and new entity
 */
function hasOwnershipDataChanged(newEntity: Entity, existingEntity: Entity): boolean {
  // Compare ownership_users
  const newUsers = newEntity.data.ownership_users || '';
  const existingUsers = existingEntity.data.ownership_users || '';
  if (newUsers !== existingUsers) return true;
  
  // Compare ownership_groups
  const newGroups = newEntity.data.ownership_groups || '';
  const existingGroups = existingEntity.data.ownership_groups || '';
  if (newGroups !== existingGroups) return true;
  
  return false;
}

/**
 * Check if relationship data has changed between existing and new entity
 */
function hasRelationshipDataChanged(newEntity: Entity, existingEntity: Entity): boolean {
  // Compare related_contains
  const newContains = newEntity.data.related_contains || '';
  const existingContains = existingEntity.data.related_contains || '';
  if (newContains !== existingContains) return true;
  
  // Compare related_inherits
  const newInherits = newEntity.data.related_inherits || '';
  const existingInherits = existingEntity.data.related_inherits || '';
  if (newInherits !== existingInherits) return true;
  
  return false;
}

/**
 * Check if domain data has changed between existing and new entity
 */
function hasDomainDataChanged(newEntity: Entity, existingEntity: Entity): boolean {
  // Compare domain_urn
  const newDomainUrn = newEntity.data.domain_urn || '';
  const existingDomainUrn = existingEntity.data.domain_urn || '';
  if (newDomainUrn !== existingDomainUrn) return true;
  
  // Compare domain_name
  const newDomainName = newEntity.data.domain_name || '';
  const existingDomainName = existingEntity.data.domain_name || '';
  if (newDomainName !== existingDomainName) return true;
  
  return false;
}

/**
 * Create ownership patches
 */
function createOwnershipPatches(
  entities: Entity[],
  urnMap: Map<string, string>,
  existingOwnershipTypes: Map<string, string>,
  existingEntities: Entity[] = []
): ComprehensivePatchInput[] {
  const patches: ComprehensivePatchInput[] = [];
  
  entities.forEach(entity => {
    if (!entity.data.ownership_users && !entity.data.ownership_groups) {
      return; // Skip entities without ownership
    }
    
    try {
      const parsedOwnership = parseOwnershipFromColumns(
        entity.data.ownership_users || '',
        entity.data.ownership_groups || ''
      );
      
      if (parsedOwnership.length === 0) return;
      
      // Determine if this is an update (entity already exists) or new entity
      const existingEntity = existingEntities.find(existing => existing.name === entity.name);
      const isUpdate = !!existingEntity;
      
      // For updates, check if ownership has actually changed
      if (isUpdate && existingEntity) {
        const hasOwnershipChanged = hasOwnershipDataChanged(entity, existingEntity);
        if (!hasOwnershipChanged) {
          return; // Skip if ownership hasn't changed
        }
      }
      
      const ownershipPatchOps = createOwnershipPatchOperations(parsedOwnership, existingOwnershipTypes, isUpdate);
      
      if (ownershipPatchOps.length > 0) {
        const urn = UrnManager.resolveEntityUrn(entity, urnMap);
        patches.push({
          urn,
          entityType: entity.type,
          aspectName: 'ownership',
          patch: ownershipPatchOps
        });
      }
    } catch (error) {
      console.warn(`Failed to create ownership patches for ${entity.name}:`, error);
    }
  });
  
  return patches;
}

/**
 * Create parent relationship patches
 */
function createParentRelationshipPatches(
  entities: Entity[],
  urnMap: Map<string, string>,
  existingEntities: Entity[]
): ComprehensivePatchInput[] {
  const patches: ComprehensivePatchInput[] = [];
  
  // Create lookup map for existing entities
  const existingUrnMap = new Map<string, string>();
  existingEntities.forEach(entity => {
    if (entity.urn) {
      existingUrnMap.set(entity.name.toLowerCase(), entity.urn);
    }
  });
  
  entities.forEach(entity => {
    if (entity.parentNames.length === 0) return;
    
    const parentUrns: string[] = [];
    
    entity.parentNames.forEach(parentName => {
      // Use HierarchyNameResolver to find parent entity (handles hierarchical names)
      const parentEntity = HierarchyNameResolver.findParentEntity(parentName, existingEntities);
      
      let parentUrn: string | undefined;
      
      if (parentEntity) {
        // Parent found in existing entities
        parentUrn = parentEntity.urn;
      } else {
        // Check entities in current batch
        const actualParentName = HierarchyNameResolver.parseHierarchicalName(parentName);
        const batchParent = entities.find(e => 
          e.name.toLowerCase() === actualParentName.toLowerCase() && 
          e.status === 'new'
        );
        if (batchParent) {
          parentUrn = urnMap.get(batchParent.id);
        }
      }
      
      if (parentUrn) {
        parentUrns.push(parentUrn);
      } else {
        const actualParentName = HierarchyNameResolver.parseHierarchicalName(parentName);
        console.warn(`Parent entity "${parentName}" (resolved to "${actualParentName}") not found for "${entity.name}"`);
      }
    });
    
    if (parentUrns.length > 0) {
      const urn = UrnManager.resolveEntityUrn(entity, urnMap);
      // For parent relationships, we only support one parent per entity
      // Take the first parent if multiple are specified
      const parentUrn = parentUrns[0];
      
      const parentPatches: PatchOperation[] = [{
        op: 'ADD' as const,
        path: '/parentNode',
        value: parentUrn
      }];
      
      const aspectName = entity.type === 'glossaryTerm' ? 'glossaryTermInfo' : 'glossaryNodeInfo';
      
      patches.push({
        urn,
        entityType: entity.type,
        aspectName,
        patch: parentPatches
      });
    }
  });
  
  return patches;
}

/**
 * Create related term patches
 */
function createRelatedTermPatches(
  entities: Entity[],
  urnMap: Map<string, string>,
  existingEntities: Entity[] = []
): ComprehensivePatchInput[] {
  const patches: ComprehensivePatchInput[] = [];

  entities.forEach(entity => {
    const relatedContains = entity.data.related_contains;
    const relatedInherits = entity.data.related_inherits;

    if (!relatedContains && !relatedInherits) return;

    // For existing entities, check if relationships have actually changed
    const existingEntity = existingEntities.find(existing => existing.name === entity.name);
    if (existingEntity) {
      const hasRelationshipsChanged = hasRelationshipDataChanged(entity, existingEntity);
      if (!hasRelationshipsChanged) {
        return; // Skip if relationships haven't changed
      }
    }

    const currentUrn = UrnManager.resolveEntityUrn(entity, urnMap);

    // Handle related contains (HasA relationship)
    if (relatedContains) {
      const containsNames = relatedContains.split(',').map(name => name.trim()).filter(Boolean);
      const relatedUrns: string[] = [];
      
      containsNames.forEach(relatedName => {
        // Find the related entity URN - try both exact name and hierarchical name
        let relatedEntity = entities.find(e => e.name.toLowerCase() === relatedName.toLowerCase());
        if (!relatedEntity) {
          // Try to find by hierarchical name (e.g., "Business Terms.Customer Name" -> "Customer Name")
          const simpleName = relatedName.split('.').pop() || relatedName;
          relatedEntity = entities.find(e => e.name.toLowerCase() === simpleName.toLowerCase());
        }
        if (relatedEntity) {
          const relatedUrn = UrnManager.resolveEntityUrn(relatedEntity, urnMap);
          relatedUrns.push(relatedUrn);
        } else {
          console.warn(`🔗 Related entity not found for contains: "${relatedName}"`);
        }
      });

      if (relatedUrns.length > 0) {
        patches.push({
          urn: currentUrn,
          entityType: entity.type,
          aspectName: 'addRelatedTerms', // Special marker for addRelatedTerms mutation
          patch: [{
            op: 'ADD' as const,
            path: '/',
            value: {
              termUrns: relatedUrns,
              relationshipType: 'hasA'
            }
          }]
        });
      }
    }
    
    // Handle related inherits (IsA relationship)
    if (relatedInherits) {
      const inheritsNames = relatedInherits.split(',').map(name => name.trim()).filter(Boolean);
      const relatedUrns: string[] = [];
      
      inheritsNames.forEach(relatedName => {
        // Find the related entity URN - try both exact name and hierarchical name
        let relatedEntity = entities.find(e => e.name.toLowerCase() === relatedName.toLowerCase());
        if (!relatedEntity) {
          // Try to find by hierarchical name (e.g., "Business Terms.Customer Name" -> "Customer Name")
          const simpleName = relatedName.split('.').pop() || relatedName;
          relatedEntity = entities.find(e => e.name.toLowerCase() === simpleName.toLowerCase());
        }
        if (relatedEntity) {
          const relatedUrn = UrnManager.resolveEntityUrn(relatedEntity, urnMap);
          relatedUrns.push(relatedUrn);
        } else {
          console.warn(`🔗 Related entity not found for inherits: "${relatedName}"`);
        }
      });

      if (relatedUrns.length > 0) {
        patches.push({
          urn: currentUrn,
          entityType: entity.type,
          aspectName: 'addRelatedTerms', // Special marker for addRelatedTerms mutation
          patch: [{
            op: 'ADD' as const,
            path: '/',
            value: {
              termUrns: relatedUrns,
              relationshipType: 'isA'
            }
          }]
        });
      }
    }
  });
  
  return patches;
}

/**
 * Create domain assignment patches
 */
function createDomainAssignmentPatches(
  entities: Entity[],
  urnMap: Map<string, string>,
  existingEntities: Entity[] = []
): ComprehensivePatchInput[] {
  const patches: ComprehensivePatchInput[] = [];
  
  entities.forEach(entity => {
    if (!entity.data.domain_urn && !entity.data.domain_name) return;
    
    // For existing entities, check if domain has actually changed
    const existingEntity = existingEntities.find(existing => existing.name === entity.name);
    if (existingEntity) {
      const hasDomainChanged = hasDomainDataChanged(entity, existingEntity);
      if (!hasDomainChanged) {
        return; // Skip if domain hasn't changed
      }
    }
    
    const domainUrn = entity.data.domain_urn || `urn:li:domain:${entity.data.domain_name}`;
    
    const urn = UrnManager.resolveEntityUrn(entity, urnMap);
    patches.push({
      urn,
      entityType: entity.type,
      aspectName: 'domains',
      patch: [{
        op: 'ADD' as const,
        path: '/domains',
        value: JSON.stringify({
          domain: domainUrn
        })
      }]
    });
  });
  
  return patches;
}

/**
 * Convert comprehensive plan to single patch input array
 */
export function convertPlanToPatchInputs(plan: ComprehensiveImportPlan, entitiesToProcess: Entity[], existingEntities: Entity[] = []): ComprehensivePatchInput[] {
  const patchInputs: ComprehensivePatchInput[] = [];
  
  // Phase 1: Ownership Types (must exist first)
  patchInputs.push(...createOwnershipTypePatches(plan.ownershipTypes));
  
  // Phase 2: Entities in hierarchical order (use only filtered entities for entity patches)
  patchInputs.push(...createEntityPatches(entitiesToProcess, plan.urnMap, existingEntities));
  
  // Phase 3: All relationships and assignments (use all entities from plan)
  patchInputs.push(...plan.ownershipPatches);
  // Parent relationships are now handled directly in createEntityPatches
  patchInputs.push(...plan.relatedTermPatches);
  patchInputs.push(...plan.domainAssignmentPatches);
  return patchInputs;
}
