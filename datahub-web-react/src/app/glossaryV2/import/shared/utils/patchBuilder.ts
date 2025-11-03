/**
 * Centralized Patch Builder
 * Consolidates all patch operation creation logic
 */
import { Entity, PatchOperation } from '@app/glossaryV2/import/glossary.types';
import { HierarchyNameResolver } from '@app/glossaryV2/import/shared/utils/hierarchyUtils';
import {
    createOwnershipPatchOperations,
    parseOwnershipFromColumns,
} from '@app/glossaryV2/import/shared/utils/ownershipParsingUtils';
import { UrnManager } from '@app/glossaryV2/import/shared/utils/urnManager';

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
    urn: string;
    entityType: string;
    aspectName: string;
    patch: PatchOperation[];
    arrayPrimaryKeys?: ArrayPrimaryKeyInput[];
    forceGenericPatch?: boolean;
}

export class PatchBuilder {
    static createOwnershipTypePatches(ownershipTypes: OwnershipTypeInput[]): ComprehensivePatchInput[] {
        return ownershipTypes.map((ownershipType) => ({
            urn: ownershipType.urn,
            entityType: 'ownershipType',
            aspectName: 'ownershipTypeInfo',
            patch: [
                { op: 'ADD' as const, path: '/name', value: ownershipType.name },
                { op: 'ADD' as const, path: '/description', value: ownershipType.description },
                {
                    op: 'ADD' as const,
                    path: '/created',
                    value: JSON.stringify({
                        time: Date.now(),
                        actor: 'urn:li:corpuser:datahub',
                    }),
                },
                {
                    op: 'ADD' as const,
                    path: '/lastModified',
                    value: JSON.stringify({
                        time: Date.now(),
                        actor: 'urn:li:corpuser:datahub',
                    }),
                },
            ],
            forceGenericPatch: true,
        }));
    }

    static hasEntityInfoChanged(newEntity: Entity, existingEntity: Entity): boolean {
        if (newEntity.name !== existingEntity.name) return true;

        const newDescription = newEntity.data.description || '';
        const existingDescription = existingEntity.data.description || '';
        if (newDescription !== existingDescription) return true;

        const newTermSource = newEntity.data.term_source || '';
        const existingTermSource = existingEntity.data.term_source || '';
        if (newTermSource !== existingTermSource) return true;

        const newSourceRef = newEntity.data.source_ref || '';
        const existingSourceRef = existingEntity.data.source_ref || '';
        if (newSourceRef !== existingSourceRef) return true;

        const newSourceUrl = newEntity.data.source_url || '';
        const existingSourceUrl = existingEntity.data.source_url || '';
        if (newSourceUrl !== existingSourceUrl) return true;

        const newParentNames = newEntity.parentNames || [];
        const existingParentNames = existingEntity.parentNames || [];
        if (JSON.stringify(newParentNames) !== JSON.stringify(existingParentNames)) return true;

        return false;
    }

    static createEntityPatches(
        entities: Entity[],
        urnMap: Map<string, string>,
        existingEntities: Entity[] = [],
    ): ComprehensivePatchInput[] {
        return entities
            .filter((entity) => {
                if (entity.status === 'new' || entity.status === 'updated') return true;

                const existingEntity = existingEntities.find((e) => e.urn === entity.urn);
                if (existingEntity && this.hasEntityInfoChanged(entity, existingEntity)) return true;

                return false;
            })
            .map((entity) => {
                const urn = UrnManager.resolveEntityUrn(entity, urnMap);
                const aspectName = entity.type === 'glossaryTerm' ? 'glossaryTermInfo' : 'glossaryNodeInfo';

                const patch: PatchOperation[] = [];

                patch.push({ op: 'ADD' as const, path: '/name', value: entity.name });

                if (entity.data.description) {
                    const fieldName = entity.type === 'glossaryTerm' ? 'definition' : 'definition';
                    patch.push({ op: 'ADD' as const, path: `/${fieldName}`, value: entity.data.description });
                }

                if (entity.type === 'glossaryTerm' && entity.data.term_source) {
                    patch.push({ op: 'ADD' as const, path: '/termSource', value: entity.data.term_source });
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
                                value: JSON.stringify(value),
                            });
                        });
                    } catch (error) {
                        console.warn(`Failed to parse custom properties for entity ${entity.name}:`, error);
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
                        console.warn(
                            `Parent entity "${parentName}" (resolved to "${actualParentName}") not found for "${entity.name}"`,
                        );
                    }
                }

                return {
                    urn,
                    entityType: entity.type,
                    aspectName,
                    patch,
                    forceGenericPatch: true,
                };
            });
    }

    static createOwnershipPatches(
        entities: Entity[],
        urnMap: Map<string, string>,
        ownershipTypeMap: Map<string, string>,
    ): ComprehensivePatchInput[] {
        const ownershipPatches: ComprehensivePatchInput[] = [];

        entities.forEach((entity) => {
            try {
                if (!entity.data.ownership_users && !entity.data.ownership_groups) {
                    return;
                }

                const parsedOwnership = parseOwnershipFromColumns(
                    entity.data.ownership_users || '',
                    entity.data.ownership_groups || '',
                );

                if (parsedOwnership.length === 0) return;

                const urn = UrnManager.resolveEntityUrn(entity, urnMap);
                const patches = createOwnershipPatchOperations(parsedOwnership, ownershipTypeMap);

                if (patches.length > 0) {
                    ownershipPatches.push({
                        urn,
                        entityType: entity.type,
                        aspectName: 'ownership',
                        patch: patches,
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
                console.error(`Failed to create ownership patches for ${entity.name}:`, error);
            }
        });

        return ownershipPatches;
    }

    /**
     * Create related term patches
     */
    static createRelatedTermPatches(entities: Entity[], urnMap: Map<string, string>): ComprehensivePatchInput[] {
        const relatedTermPatches: ComprehensivePatchInput[] = [];

        entities.forEach((entity) => {
            const relatedTerms: { [relationshipType: string]: string[] } = {};

            if (entity.data.related_contains) {
                const containsList = entity.data.related_contains
                    .split(',')
                    .map((name) => name.trim())
                    .filter(Boolean);
                containsList.forEach((relatedName) => {
                    const relatedEntity = entities.find((e) => e.name === relatedName);
                    if (relatedEntity) {
                        const relatedUrn = UrnManager.resolveEntityUrn(relatedEntity, urnMap);
                        if (!relatedTerms.contains) relatedTerms.contains = [];
                        relatedTerms.contains.push(relatedUrn);
                    } else {
                        console.warn(`🔗 Related entity not found for contains: "${relatedName}"`);
                    }
                });
            }

            if (entity.data.related_inherits) {
                const inheritsList = entity.data.related_inherits
                    .split(',')
                    .map((name) => name.trim())
                    .filter(Boolean);
                inheritsList.forEach((relatedName) => {
                    const relatedEntity = entities.find((e) => e.name === relatedName);
                    if (relatedEntity) {
                        const relatedUrn = UrnManager.resolveEntityUrn(relatedEntity, urnMap);
                        if (!relatedTerms.inherits) relatedTerms.inherits = [];
                        relatedTerms.inherits.push(relatedUrn);
                    } else {
                        console.warn(`🔗 Related entity not found for inherits: "${relatedName}"`);
                    }
                });
            }

            if (Object.keys(relatedTerms).length > 0) {
                const currentUrn = UrnManager.resolveEntityUrn(entity, urnMap);
                const patches: PatchOperation[] = [];

                Object.entries(relatedTerms).forEach(([relationshipType, relatedUrns]) => {
                    relatedUrns.forEach((relatedUrn) => {
                        patches.push({
                            op: 'ADD' as const,
                            path: `/isRelatedTerms/${relatedUrn}/${relationshipType}`,
                            value: '{}',
                        });
                    });
                });

                if (patches.length > 0) {
                    relatedTermPatches.push({
                        urn: currentUrn,
                        entityType: entity.type,
                        aspectName: 'glossaryRelatedTerms',
                        patch: patches,
                        forceGenericPatch: true,
                    });
                }
            }
        });

        return relatedTermPatches;
    }

    /**
     * Create domain assignment patches
     */
    static createDomainAssignmentPatches(entities: Entity[], urnMap: Map<string, string>): ComprehensivePatchInput[] {
        const domainPatches: ComprehensivePatchInput[] = [];

        entities.forEach((entity) => {
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
                            value: entity.data.domain_urn,
                        },
                    ],
                    forceGenericPatch: true,
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

export const createEntityPatches = (entities: Entity[], urnMap: Map<string, string>, existingEntities: Entity[] = []) =>
    PatchBuilder.createEntityPatches(entities, urnMap, existingEntities);

export const createOwnershipPatches = (
    entities: Entity[],
    urnMap: Map<string, string>,
    ownershipTypeMap: Map<string, string>,
) => PatchBuilder.createOwnershipPatches(entities, urnMap, ownershipTypeMap);

export const createRelatedTermPatches = (entities: Entity[], urnMap: Map<string, string>) =>
    PatchBuilder.createRelatedTermPatches(entities, urnMap);

export const createDomainAssignmentPatches = (entities: Entity[], urnMap: Map<string, string>) =>
    PatchBuilder.createDomainAssignmentPatches(entities, urnMap);
