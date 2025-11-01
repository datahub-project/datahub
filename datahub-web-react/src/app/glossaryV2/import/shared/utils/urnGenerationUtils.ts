/**
 * URN Generation Utilities
 * @deprecated This file is deprecated. Import from urnManager.ts instead.
 * 
 * This file now re-exports from UrnManager for backward compatibility.
 * All new code should import directly from urnManager.ts
 */

import { HierarchyNameResolver } from './hierarchyUtils';
import { UrnManager } from './urnManager';
import { Entity } from '../../glossary.types';

// Re-export constants
export { AUTO_GENERATE_ALLOWED_ENTITY_TYPES } from './urnManager';

// Re-export all functions from UrnManager for backward compatibility
export const generateGuid = () => UrnManager.generateGuid();
export const generateEntityUrn = (entityType: string) => UrnManager.generateEntityUrn(entityType);
export const generateOwnershipTypeUrn = (name: string) => UrnManager.generateOwnershipTypeUrn(name);
export const isValidUrn = (urn: string, allowEmpty?: boolean) => UrnManager.isValidUrn(urn, allowEmpty ?? false);
export const extractEntityTypeFromUrn = (urn: string) => UrnManager.extractEntityTypeFromUrn(urn);
export const preGenerateUrns = (entities: Entity[]) => UrnManager.preGenerateUrns(entities);
export const resolveEntityUrn = (entity: Entity, urnMap: Map<string, string>) => 
  UrnManager.resolveEntityUrn(entity, urnMap);
export const createExistingEntityUrnMap = (existingEntities: Entity[]) => 
  UrnManager.createExistingEntityUrnMap(existingEntities);

/**
 * Resolve parent URNs for an entity
 * @deprecated Use HierarchyNameResolver.resolveParentUrns instead
 */
export function resolveParentUrns(
  entity: Entity, 
  existingUrnMap: Map<string, string>,
  generatedUrnMap: Map<string, string>
): string[] {
  // Convert existingUrnMap to Entity array for HierarchyNameResolver
  const existingEntities: Entity[] = [];
  existingUrnMap.forEach((urn, name) => {
    existingEntities.push({
      id: name,
      name: name,
      type: 'glossaryNode' as const,
      status: 'existing' as const,
      parentNames: [],
      parentUrns: [],
      level: 0,
      data: {} as any,
      urn
    });
  });

  // Use HierarchyNameResolver to resolve parent URNs
  return HierarchyNameResolver.resolveParentUrns(entity, existingEntities, generatedUrnMap);
}
