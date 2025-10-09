/**
 * Comprehensive Import Utilities
 * Handles single GraphQL call with hierarchical ordering and dependency resolution
 */

import { Entity, EntityData, PatchOperation } from '../../glossary.types';
import { preGenerateUrns, resolveEntityUrn, generateOwnershipTypeUrn } from './urnGenerationUtils';
import { sortEntitiesByHierarchy } from '../../glossary.utils';
import { parseOwnershipFromColumns, createOwnershipPatchOperations } from './ownershipParsingUtils';

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
  console.log('🎯 Creating comprehensive import plan...');
  
  // 1. Pre-generate URNs for all new entities
  const urnMap = preGenerateUrns(entities);
  console.log(`📋 Generated ${urnMap.size} URNs for new entities`);
  
  // 2. Sort entities by hierarchy (parents before children)
  const sortedEntities = sortEntitiesByHierarchy(entities);
  console.log(`📊 Sorted ${sortedEntities.length} entities by hierarchy`);
  
  // 3. Extract and create ownership types
  const ownershipTypes = extractOwnershipTypes(entities, existingOwnershipTypes);
  console.log(`🏷️ Found ${ownershipTypes.length} ownership types to create`);
  
  // 4. Create all patch inputs in proper order
  const plan: ComprehensiveImportPlan = {
    ownershipTypes,
    entities: sortedEntities,
    ownershipPatches: createOwnershipPatches(sortedEntities, urnMap, existingOwnershipTypes),
    parentRelationshipPatches: createParentRelationshipPatches(sortedEntities, urnMap, existingEntities),
    relatedTermPatches: createRelatedTermPatches(sortedEntities, urnMap),
    domainAssignmentPatches: createDomainAssignmentPatches(sortedEntities, urnMap)
  };
  
  console.log('✅ Comprehensive import plan created');
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
    urn: generateOwnershipTypeUrn(name)
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
 * Create entity patches in hierarchical order
 */
function createEntityPatches(
  entities: Entity[],
  urnMap: Map<string, string>
): ComprehensivePatchInput[] {
  return entities.map(entity => {
    const urn = resolveEntityUrn(entity, urnMap);
    const aspectName = entity.type === 'glossaryTerm' ? 'glossaryTermInfo' : 'glossaryNodeInfo';
    
    const patch: PatchOperation[] = [
      { op: 'ADD' as const, path: '/name', value: entity.name },
      { op: 'ADD' as const, path: '/description', value: entity.data.description || '' }
    ];
    
    // Add term-specific fields
    if (entity.type === 'glossaryTerm') {
      patch.push({ op: 'ADD' as const, path: '/termSource', value: entity.data.term_source || 'INTERNAL' });
      
      if (entity.data.source_ref) {
        patch.push({ op: 'ADD' as const, path: '/sourceRef', value: entity.data.source_ref });
      }
      if (entity.data.source_url) {
        patch.push({ op: 'ADD' as const, path: '/sourceUrl', value: entity.data.source_url });
      }
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
    
    return {
      urn: entity.status === 'new' ? undefined : urn, // Let backend generate for new entities
      entityType: entity.type,
      aspectName,
      patch
    };
  });
}

/**
 * Create ownership patches
 */
function createOwnershipPatches(
  entities: Entity[],
  urnMap: Map<string, string>,
  existingOwnershipTypes: Map<string, string>
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
      
      const ownershipPatchOps = createOwnershipPatchOperations(parsedOwnership, existingOwnershipTypes);
      
      if (ownershipPatchOps.length > 0) {
        const urn = resolveEntityUrn(entity, urnMap);
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
      // First check existing entities
      let parentUrn = existingUrnMap.get(parentName.toLowerCase());
      
      // Then check generated URNs for new entities
      if (!parentUrn) {
        // Find parent in our import batch
        const parentEntity = entities.find(e => 
          e.name.toLowerCase() === parentName.toLowerCase() && 
          e.status === 'new'
        );
        if (parentEntity) {
          parentUrn = urnMap.get(parentEntity.id);
        }
      }
      
      if (parentUrn) {
        parentUrns.push(parentUrn);
      } else {
        console.warn(`Parent entity "${parentName}" not found for "${entity.name}"`);
      }
    });
    
    if (parentUrns.length > 0) {
      const urn = resolveEntityUrn(entity, urnMap);
      const parentPatches: PatchOperation[] = parentUrns.map(parentUrn => ({
        op: 'ADD' as const,
        path: '/parentNode',
        value: parentUrn
      }));
      
      patches.push({
        urn,
        entityType: entity.type,
        aspectName: 'parentNodes',
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
  urnMap: Map<string, string>
): ComprehensivePatchInput[] {
  const patches: ComprehensivePatchInput[] = [];
  
  entities.forEach(entity => {
    const relatedContains = entity.data.related_contains;
    const relatedInherits = entity.data.related_inherits;
    
    if (!relatedContains && !relatedInherits) return;
    
    const relationshipPatches: PatchOperation[] = [];
    
    // Handle related contains
    if (relatedContains) {
      const containsNames = relatedContains.split(',').map(name => name.trim()).filter(Boolean);
      containsNames.forEach(relatedName => {
        // Find the related entity URN
        const relatedEntity = entities.find(e => e.name.toLowerCase() === relatedName.toLowerCase());
        if (relatedEntity) {
          const relatedUrn = resolveEntityUrn(relatedEntity, urnMap);
          relationshipPatches.push({
            op: 'ADD' as const,
            path: '/relationships',
            value: JSON.stringify({
              type: 'HasA',
              entity: relatedUrn,
              direction: 'OUTGOING'
            })
          });
        }
      });
    }
    
    // Handle related inherits
    if (relatedInherits) {
      const inheritsNames = relatedInherits.split(',').map(name => name.trim()).filter(Boolean);
      inheritsNames.forEach(relatedName => {
        const relatedEntity = entities.find(e => e.name.toLowerCase() === relatedName.toLowerCase());
        if (relatedEntity) {
          const relatedUrn = resolveEntityUrn(relatedEntity, urnMap);
          relationshipPatches.push({
            op: 'ADD' as const,
            path: '/relationships',
            value: JSON.stringify({
              type: 'InheritsFrom',
              entity: relatedUrn,
              direction: 'OUTGOING'
            })
          });
        }
      });
    }
    
    if (relationshipPatches.length > 0) {
      const urn = resolveEntityUrn(entity, urnMap);
      patches.push({
        urn,
        entityType: entity.type,
        aspectName: 'relationships',
        patch: relationshipPatches
      });
    }
  });
  
  return patches;
}

/**
 * Create domain assignment patches
 */
function createDomainAssignmentPatches(
  entities: Entity[],
  urnMap: Map<string, string>
): ComprehensivePatchInput[] {
  const patches: ComprehensivePatchInput[] = [];
  
  entities.forEach(entity => {
    if (!entity.data.domain_urn && !entity.data.domain_name) return;
    
    const domainUrn = entity.data.domain_urn || `urn:li:domain:${entity.data.domain_name}`;
    
    const urn = resolveEntityUrn(entity, urnMap);
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
export function convertPlanToPatchInputs(plan: ComprehensiveImportPlan): ComprehensivePatchInput[] {
  const patchInputs: ComprehensivePatchInput[] = [];
  
  // Phase 1: Ownership Types (must exist first)
  patchInputs.push(...createOwnershipTypePatches(plan.ownershipTypes));
  
  // Phase 2: Entities in hierarchical order
  const urnMap = preGenerateUrns(plan.entities);
  patchInputs.push(...createEntityPatches(plan.entities, urnMap));
  
  // Phase 3: All relationships and assignments
  patchInputs.push(...plan.ownershipPatches);
  patchInputs.push(...plan.parentRelationshipPatches);
  patchInputs.push(...plan.relatedTermPatches);
  patchInputs.push(...plan.domainAssignmentPatches);
  
  console.log(`📦 Created ${patchInputs.length} total patch operations`);
  return patchInputs;
}
