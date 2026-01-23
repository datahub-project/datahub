/**
 * Comprehensive Import Utilities
 * Handles single GraphQL call with hierarchical ordering and dependency resolution
 */
import { Entity, PatchOperation } from '@app/glossaryV2/import/glossary.types';
import { sortEntitiesByHierarchy } from '@app/glossaryV2/import/glossary.utils';
import { HierarchyNameResolver } from '@app/glossaryV2/import/shared/utils/hierarchyUtils';
import {
    createOwnershipPatchOperations,
    parseOwnershipFromColumns,
} from '@app/glossaryV2/import/shared/utils/ownershipParsingUtils';
import {
    ArrayPrimaryKeyInput,
    ComprehensivePatchInput,
    OwnershipTypeInput,
    PatchBuilder,
} from '@app/glossaryV2/import/shared/utils/patchBuilder';
import { UrnManager } from '@app/glossaryV2/import/shared/utils/urnManager';
import { hasEntityInfoChanged } from '@app/glossaryV2/import/shared/utils/entityComparisonUtils';

// Re-export types for backward compatibility
export type { ComprehensivePatchInput, ArrayPrimaryKeyInput, OwnershipTypeInput };

export interface ComprehensiveImportPlan {
    ownershipTypes: OwnershipTypeInput[];
    entities: Entity[];
    urnMap: Map<string, string>; // URN mapping for consistent URN usage
    ownershipPatches: ComprehensivePatchInput[];
    parentRelationshipPatches: ComprehensivePatchInput[];
    relatedTermPatches: ComprehensivePatchInput[];
    domainAssignmentPatches: ComprehensivePatchInput[];
}

export type WarningCallback = (entityId: string, entityName: string, operation: string, message: string) => void;

/**
 * Create comprehensive import plan with proper ordering
 */
export function createComprehensiveImportPlan(
    entities: Entity[],
    existingEntities: Entity[],
    existingOwnershipTypes: Map<string, string>,
    onWarning?: WarningCallback,
): ComprehensiveImportPlan {
    const urnMap = UrnManager.preGenerateUrns(entities);
    const sortedEntities = sortEntitiesByHierarchy(entities);
    const ownershipTypes = extractOwnershipTypes(entities, existingOwnershipTypes);

    // Merge existing and new ownership types into complete map
    // This ensures ownership patches can resolve URNs for both existing and newly created ownership types
    const completeOwnershipTypeMap = new Map<string, string>(existingOwnershipTypes);
    ownershipTypes.forEach((ot) => {
        completeOwnershipTypeMap.set(ot.name.toLowerCase(), ot.urn);
    });

    const plan: ComprehensiveImportPlan = {
        ownershipTypes,
        entities: sortedEntities,
        urnMap,
        ownershipPatches: createOwnershipPatches(sortedEntities, urnMap, completeOwnershipTypeMap, existingEntities, onWarning),
        parentRelationshipPatches: [], // Parent relationships are now handled directly in createEntityPatches
        relatedTermPatches: createRelatedTermPatches(sortedEntities, urnMap, existingEntities, onWarning),
        domainAssignmentPatches: createDomainAssignmentPatches(sortedEntities, urnMap, existingEntities),
    };

    return plan;
}

/**
 * Extract ownership types that need to be created
 */
function extractOwnershipTypes(entities: Entity[], existingOwnershipTypes: Map<string, string>): OwnershipTypeInput[] {
    const ownershipTypeNames = new Set<string>();

    entities.forEach((entity) => {
        if (entity.data.ownership_users) {
            const parsed = parseOwnershipFromColumns(entity.data.ownership_users, '');
            parsed.forEach(({ ownershipTypeName }) => {
                if (!existingOwnershipTypes.has(ownershipTypeName.toLowerCase())) {
                    ownershipTypeNames.add(ownershipTypeName);
                }
            });
        }

        if (entity.data.ownership_groups) {
            const parsed = parseOwnershipFromColumns('', entity.data.ownership_groups);
            parsed.forEach(({ ownershipTypeName }) => {
                if (!existingOwnershipTypes.has(ownershipTypeName.toLowerCase())) {
                    ownershipTypeNames.add(ownershipTypeName);
                }
            });
        }
    });

    return Array.from(ownershipTypeNames).map((name) => ({
        name,
        description: `Custom ownership type: ${name}`,
        urn: UrnManager.generateOwnershipTypeUrn(name),
    }));
}



function createEntityPatches(
    entities: Entity[],
    urnMap: Map<string, string>,
    existingEntities: Entity[] = [],
    onWarning?: WarningCallback,
): ComprehensivePatchInput[] {
    return entities
        .filter((entity) => {
            if (entity.status === 'existing' || entity.status === 'updated') {
                const existingEntity = existingEntities.find((existing) => existing.name === entity.name);
                if (existingEntity) {
                    const entityInfoChanged = hasEntityInfoChanged(entity, existingEntity);

                    // Include if entity info changed or any children have changes
                    const hasChildrenWithChanges = entities.some(
                        (child) =>
                            child.parentNames &&
                            child.parentNames.includes(entity.name) &&
                            (child.status === 'new' ||
                                hasEntityInfoChanged(
                                    child,
                                    existingEntities.find((e) => e.name === child.name) || child,
                                )),
                    );

                    return entityInfoChanged || hasChildrenWithChanges;
                }
            }
            return true;
        })
        .map((entity) => {
            const urn = UrnManager.resolveEntityUrn(entity, urnMap);
            const aspectName = entity.type === 'glossaryTerm' ? 'glossaryTermInfo' : 'glossaryNodeInfo';
            const isNewEntity = entity.status === 'new';

            const patch: PatchOperation[] = [];

            // For new entities, always provide required fields (mimic createGlossaryTerm behavior)
            if (isNewEntity) {
                patch.push({ op: 'ADD' as const, path: '/name', value: entity.name || null });
                patch.push({ op: 'ADD' as const, path: '/definition', value: entity.data.description || '' });

                if (entity.type === 'glossaryTerm') {
                    const termSource = entity.data.term_source || 'INTERNAL';
                    const normalizedTermSource = termSource.trim().toUpperCase();
                    if (normalizedTermSource !== 'INTERNAL' && normalizedTermSource !== 'EXTERNAL') {
                        const message = `Invalid term_source "${termSource}" for entity "${entity.name}", defaulting to INTERNAL`;
                        if (onWarning) {
                            onWarning(entity.id || entity.urn || '', entity.name, 'term_source_validation', message);
                        } else {
                            console.warn(message);
                        }
                        patch.push({ op: 'ADD' as const, path: '/termSource', value: 'INTERNAL' });
                    } else {
                        patch.push({ op: 'ADD' as const, path: '/termSource', value: normalizedTermSource });
                    }
                }
            } else {
                if (entity.name) {
                    patch.push({ op: 'ADD' as const, path: '/name', value: entity.name });
                }
                if (entity.data.description) {
                    patch.push({ op: 'ADD' as const, path: '/definition', value: entity.data.description });
                }
                if (entity.type === 'glossaryTerm' && entity.data.term_source) {
                    const termSource = entity.data.term_source;
                    const normalizedTermSource = termSource.trim().toUpperCase();
                    if (normalizedTermSource !== 'INTERNAL' && normalizedTermSource !== 'EXTERNAL') {
                        const message = `Invalid term_source "${termSource}" for entity "${entity.name}", skipping`;
                        if (onWarning) {
                            onWarning(entity.id || entity.urn || '', entity.name, 'term_source_validation', message);
                        } else {
                            console.warn(message);
                        }
                    } else {
                        patch.push({ op: 'ADD' as const, path: '/termSource', value: normalizedTermSource });
                    }
                }
            }

            if (entity.data.source_ref) {
                patch.push({ op: 'ADD' as const, path: '/sourceRef', value: entity.data.source_ref });
            }
            if (entity.data.source_url) {
                patch.push({ op: 'ADD' as const, path: '/sourceUrl', value: entity.data.source_url });
            }

            if (entity.data.custom_properties) {
                try {
                    const customProps =
                        typeof entity.data.custom_properties === 'string'
                            ? JSON.parse(entity.data.custom_properties)
                            : entity.data.custom_properties;

                    Object.entries(customProps).forEach(([key, value]) => {
                        patch.push({
                            op: 'ADD' as const,
                            path: `/customProperties/${key}`,
                            value: JSON.stringify(String(value)),
                        });
                    });
                } catch (error) {
                    const message = `Failed to parse custom properties for ${entity.name}: ${error instanceof Error ? error.message : 'Unknown error'}`;
                    if (onWarning) {
                        onWarning(entity.id || entity.urn || '', entity.name, 'custom_properties_parsing', message);
                    } else {
                        console.warn(message, error);
                    }
                }
            }

            if (entity.parentNames && entity.parentNames.length > 0) {
                // For parent relationships, we only support one parent per entity
                const parentName = entity.parentNames[0];

                const parentEntity = HierarchyNameResolver.findParentEntity(parentName, existingEntities);

                let parentUrn: string | undefined;

                if (parentEntity) {
                    parentUrn = parentEntity.urn;
                } else {
                    const actualParentName = HierarchyNameResolver.parseHierarchicalName(parentName);
                    const batchParent = entities.find((e) => e.name === actualParentName && e.status === 'new');
                    if (batchParent) {
                        parentUrn = urnMap.get(batchParent.id);
                    }
                }

                if (parentUrn) {
                    patch.push({
                        op: 'ADD' as const,
                        path: '/parentNode',
                        value: parentUrn,
                    });
                } else {
                    const actualParentName = HierarchyNameResolver.parseHierarchicalName(parentName);
                    const message = `Parent entity "${parentName}" (resolved to "${actualParentName}") not found for "${entity.name}"`;
                    if (onWarning) {
                        onWarning(entity.id || entity.urn || '', entity.name, 'parent_resolution', message);
                    } else {
                        console.warn(message);
                    }
                }
            }

            return {
                urn,
                entityType: entity.type,
                aspectName,
                patch,
                forceGenericPatch: true,
            };
        })
        .filter((patchInput) => patchInput.patch.length > 0);
}

function hasOwnershipDataChanged(newEntity: Entity, existingEntity: Entity): boolean {
    const newUsers = newEntity.data.ownership_users || '';
    const existingUsers = existingEntity.data.ownership_users || '';
    if (newUsers !== existingUsers) return true;

    const newGroups = newEntity.data.ownership_groups || '';
    const existingGroups = existingEntity.data.ownership_groups || '';
    if (newGroups !== existingGroups) return true;

    return false;
}

function hasRelationshipDataChanged(newEntity: Entity, existingEntity: Entity): boolean {
    const newContains = newEntity.data.related_contains || '';
    const existingContains = existingEntity.data.related_contains || '';
    if (newContains !== existingContains) return true;

    const newInherits = newEntity.data.related_inherits || '';
    const existingInherits = existingEntity.data.related_inherits || '';
    if (newInherits !== existingInherits) return true;

    return false;
}

function hasDomainDataChanged(newEntity: Entity, existingEntity: Entity): boolean {
    const newDomainUrn = newEntity.data.domain_urn || '';
    const existingDomainUrn = existingEntity.data.domain_urn || '';
    if (newDomainUrn !== existingDomainUrn) return true;

    const newDomainName = newEntity.data.domain_name || '';
    const existingDomainName = existingEntity.data.domain_name || '';
    if (newDomainName !== existingDomainName) return true;

    return false;
}

function createOwnershipPatches(
    entities: Entity[],
    urnMap: Map<string, string>,
    ownershipTypeMap: Map<string, string>,
    existingEntities: Entity[] = [],
    onWarning?: WarningCallback,
): ComprehensivePatchInput[] {
    const patches: ComprehensivePatchInput[] = [];

    entities.forEach((entity) => {
        if (!entity.data.ownership_users && !entity.data.ownership_groups) {
            return;
        }

        try {
            const parsedOwnership = parseOwnershipFromColumns(
                entity.data.ownership_users || '',
                entity.data.ownership_groups || '',
            );

            if (parsedOwnership.length === 0) return;

            const existingEntity = existingEntities.find((existing) => existing.name === entity.name);
            const isUpdate = !!existingEntity;

            if (isUpdate && existingEntity) {
                const hasOwnershipChanged = hasOwnershipDataChanged(entity, existingEntity);
                if (!hasOwnershipChanged) {
                    return; // Skip if ownership hasn't changed
                }
            }

            const ownershipPatchOps = createOwnershipPatchOperations(parsedOwnership, ownershipTypeMap);

            if (ownershipPatchOps.length > 0) {
                const urn = UrnManager.resolveEntityUrn(entity, urnMap);
                patches.push({
                    urn,
                    entityType: entity.type,
                    aspectName: 'ownership',
                    patch: ownershipPatchOps,
                    arrayPrimaryKeys: [
                        {
                            arrayField: 'owners',
                            keys: ['owner', 'typeUrn'],
                        },
                    ],
                    forceGenericPatch: true,
                });
            }
        } catch (error) {
            const message = `Failed to create ownership patches for ${entity.name}: ${error instanceof Error ? error.message : 'Unknown error'}`;
            if (onWarning) {
                onWarning(entity.id || entity.urn || '', entity.name, 'ownership_patch_creation', message);
            } else {
                console.warn(message, error);
            }
        }
    });

    return patches;
}

// NOTE: createParentRelationshipPatches() has been removed as dead code.
// Parent relationships are now handled directly in createEntityPatches() above.

function createRelatedTermPatches(
    entities: Entity[],
    urnMap: Map<string, string>,
    existingEntities: Entity[] = [],
    onWarning?: WarningCallback,
): ComprehensivePatchInput[] {
    const patches: ComprehensivePatchInput[] = [];

    entities.forEach((entity) => {
        const relatedContains = entity.data.related_contains;
        const relatedInherits = entity.data.related_inherits;

        if (!relatedContains && !relatedInherits) return;

        const existingEntity = existingEntities.find((existing) => existing.name === entity.name);
        if (existingEntity) {
            const hasRelationshipsChanged = hasRelationshipDataChanged(entity, existingEntity);
            if (!hasRelationshipsChanged) {
                return;
            }
        }

        const currentUrn = UrnManager.resolveEntityUrn(entity, urnMap);

        if (relatedContains) {
            const containsNames = relatedContains
                .split(',')
                .map((name) => name.trim())
                .filter(Boolean);
            const relatedUrns: string[] = [];

            containsNames.forEach((relatedName) => {
                let relatedEntity = entities.find((e) => e.name.toLowerCase() === relatedName.toLowerCase());
                if (!relatedEntity) {
                    const simpleName = relatedName.split('.').pop() || relatedName;
                    relatedEntity = entities.find((e) => e.name.toLowerCase() === simpleName.toLowerCase());
                }
                if (relatedEntity) {
                    const relatedUrn = UrnManager.resolveEntityUrn(relatedEntity, urnMap);
                    relatedUrns.push(relatedUrn);
                } else {
                    const message = `Related entity not found for contains: "${relatedName}"`;
                    if (onWarning) {
                        onWarning(entity.id || entity.urn || '', entity.name, 'related_term_resolution', message);
                    } else {
                        console.warn(`🔗 ${message}`);
                    }
                }
            });

            if (relatedUrns.length > 0) {
                patches.push({
                    urn: currentUrn,
                    entityType: entity.type,
                    aspectName: 'addRelatedTerms',
                    patch: [
                        {
                            op: 'ADD' as const,
                            path: '/',
                            value: {
                                termUrns: relatedUrns,
                                relationshipType: 'hasA',
                            },
                        },
                    ],
                    forceGenericPatch: true,
                });
            }
        }

        if (relatedInherits) {
            const inheritsNames = relatedInherits
                .split(',')
                .map((name) => name.trim())
                .filter(Boolean);
            const relatedUrns: string[] = [];

            inheritsNames.forEach((relatedName) => {
                let relatedEntity = entities.find((e) => e.name.toLowerCase() === relatedName.toLowerCase());
                if (!relatedEntity) {
                    const simpleName = relatedName.split('.').pop() || relatedName;
                    relatedEntity = entities.find((e) => e.name.toLowerCase() === simpleName.toLowerCase());
                }
                if (relatedEntity) {
                    const relatedUrn = UrnManager.resolveEntityUrn(relatedEntity, urnMap);
                    relatedUrns.push(relatedUrn);
                } else {
                    const message = `Related entity not found for inherits: "${relatedName}"`;
                    if (onWarning) {
                        onWarning(entity.id || entity.urn || '', entity.name, 'related_term_resolution', message);
                    } else {
                        console.warn(`🔗 ${message}`);
                    }
                }
            });

            if (relatedUrns.length > 0) {
                patches.push({
                    urn: currentUrn,
                    entityType: entity.type,
                    aspectName: 'addRelatedTerms',
                    patch: [
                        {
                            op: 'ADD' as const,
                            path: '/',
                            value: {
                                termUrns: relatedUrns,
                                relationshipType: 'isA',
                            },
                        },
                    ],
                    forceGenericPatch: true,
                });
            }
        }
    });

    return patches;
}

function createDomainAssignmentPatches(
    entities: Entity[],
    urnMap: Map<string, string>,
    existingEntities: Entity[] = [],
): ComprehensivePatchInput[] {
    const patches: ComprehensivePatchInput[] = [];

    entities.forEach((entity) => {
        if (!entity.data.domain_urn && !entity.data.domain_name) return;

        const existingEntity = existingEntities.find((existing) => existing.name === entity.name);
        if (existingEntity) {
            const hasDomainChanged = hasDomainDataChanged(entity, existingEntity);
            if (!hasDomainChanged) {
                return;
            }
        }

        const domainUrn = entity.data.domain_urn || `urn:li:domain:${entity.data.domain_name}`;

        const urn = UrnManager.resolveEntityUrn(entity, urnMap);
        patches.push({
            urn,
            entityType: entity.type,
            aspectName: 'domains',
            patch: [
                {
                    op: 'ADD' as const,
                    path: '/domains',
                    value: JSON.stringify({
                        domain: domainUrn,
                    }),
                },
            ],
            forceGenericPatch: true,
        });
    });

    return patches;
}

export function convertPlanToPatchInputs(
    plan: ComprehensiveImportPlan,
    entitiesToProcess: Entity[],
    existingEntities: Entity[] = [],
    onWarning?: WarningCallback,
): ComprehensivePatchInput[] {
    const patchInputs: ComprehensivePatchInput[] = [];

    // Ownership types must exist before entities can reference them
    patchInputs.push(...PatchBuilder.createOwnershipTypePatches(plan.ownershipTypes));
    patchInputs.push(...createEntityPatches(entitiesToProcess, plan.urnMap, existingEntities, onWarning));
    patchInputs.push(...plan.ownershipPatches);
    // Parent relationships are now handled directly in createEntityPatches
    patchInputs.push(...plan.relatedTermPatches);
    patchInputs.push(...plan.domainAssignmentPatches);
    return patchInputs;
}
