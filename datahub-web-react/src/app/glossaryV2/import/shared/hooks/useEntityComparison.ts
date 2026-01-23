/**
 * Hook for entity comparison and change detection
 */
import { useCallback } from 'react';

import { Entity, EntityData, UseEntityComparisonReturn } from '@app/glossaryV2/import/glossary.types';
import { compareCustomProperties } from '@app/glossaryV2/import/shared/utils/customPropertiesUtils';

export function useEntityComparison(): UseEntityComparisonReturn {
    const compareEntityData = useCallback((entity1: EntityData, entity2: EntityData): boolean => {
        const fieldsToCompare: (keyof EntityData)[] = [
            'entity_type',
            'name',
            'description',
            'term_source',
            'source_ref',
            'source_url',
            'ownership_users',
            'ownership_groups',
            'parent_nodes',
            'related_contains',
            'related_inherits',
            'domain_urn',
            'domain_name',
            'custom_properties',
        ];

        return fieldsToCompare.every((field) => {
            if (field === 'custom_properties') {
                return compareCustomProperties(entity1[field], entity2[field]);
            }

            const value1 = normalizeValue(entity1[field]);
            const value2 = normalizeValue(entity2[field]);
            return value1 === value2;
        });
    }, []);

    const identifyChanges = useCallback((entity1: EntityData, entity2: EntityData): string[] => {
        const changedFields: string[] = [];
        const fieldsToCompare: (keyof EntityData)[] = [
            'entity_type',
            'name',
            'description',
            'term_source',
            'source_ref',
            'source_url',
            'ownership_users',
            'ownership_groups',
            'parent_nodes',
            'related_contains',
            'related_inherits',
            'domain_urn',
            'domain_name',
            'custom_properties',
        ];

        fieldsToCompare.forEach((field) => {
            if (field === 'custom_properties') {
                if (!compareCustomProperties(entity1[field], entity2[field])) {
                    changedFields.push(field);
                }
            } else {
                const value1 = normalizeValue(entity1[field]);
                const value2 = normalizeValue(entity2[field]);
                if (value1 !== value2) {
                    changedFields.push(field);
                }
            }
        });

        return changedFields;
    }, []);

    const detectConflicts = useCallback((entity1: Entity, entity2: Entity): boolean => {
        return entity1.name.toLowerCase() === entity2.name.toLowerCase() && entity1.type !== entity2.type;
    }, []);

    const categorizeEntities = useCallback(
        (importedEntities: Entity[], existingEntities: Entity[]) => {
            const existingByName = new Map<string, Entity>();
            existingEntities.forEach((entity) => {
                existingByName.set(entity.name.toLowerCase(), entity);
            });

            const newEntities: Entity[] = [];
            const updatedEntities: Entity[] = [];
            const unchangedEntities: Entity[] = [];
            const conflictedEntities: Entity[] = [];

            importedEntities.forEach((importedEntity) => {
                const existingEntity = existingByName.get(importedEntity.name.toLowerCase());

                if (!existingEntity) {
                    newEntities.push(importedEntity);
                } else if (detectConflicts(importedEntity, existingEntity)) {
                    conflictedEntities.push({
                        ...importedEntity,
                        status: 'conflict',
                        existingEntity,
                    });
                } else {
                    const hasChanges = !compareEntityData(importedEntity.data, existingEntity.data);
                    if (hasChanges) {
                        updatedEntities.push({
                            ...importedEntity,
                            status: 'updated',
                            existingEntity,
                        });
                    } else {
                        unchangedEntities.push({
                            ...importedEntity,
                            status: 'existing',
                            existingEntity,
                        });
                    }
                }
            });

            return {
                newEntities,
                updatedEntities,
                unchangedEntities,
                conflictedEntities,
            };
        },
        [compareEntityData, detectConflicts],
    );

    const getChangeDetails = useCallback(
        (
            importedEntity: Entity,
            existingEntity: Entity,
        ): {
            hasChanges: boolean;
            changedFields: string[];
            changeSummary: string;
        } => {
            const changedFields = identifyChanges(importedEntity.data, existingEntity.data);
            const hasChanges = changedFields.length > 0;

            let changeSummary = '';
            if (hasChanges) {
                changeSummary = `Changed fields: ${changedFields.join(', ')}`;
            } else {
                changeSummary = 'No changes detected';
            }

            return {
                hasChanges,
                changedFields,
                changeSummary,
            };
        },
        [identifyChanges],
    );

    const validateEntityCompatibility = useCallback(
        (
            importedEntity: Entity,
            existingEntity: Entity,
        ): {
            isCompatible: boolean;
            issues: string[];
        } => {
            const issues: string[] = [];

            if (importedEntity.type !== existingEntity.type) {
                issues.push(`Type mismatch: imported is ${importedEntity.type}, existing is ${existingEntity.type}`);
            }

            if (importedEntity.urn && existingEntity.urn && importedEntity.urn !== existingEntity.urn) {
                issues.push(`URN mismatch: imported URN differs from existing URN`);
            }

            const importedParents = importedEntity.parentNames.sort();
            const existingParents = existingEntity.parentNames.sort();
            if (JSON.stringify(importedParents) !== JSON.stringify(existingParents)) {
                issues.push(`Parent hierarchy differs from existing entity`);
            }

            return {
                isCompatible: issues.length === 0,
                issues,
            };
        },
        [],
    );

    return {
        compareEntityData,
        identifyChanges,
        detectConflicts,
        categorizeEntities,
        getChangeDetails,
        validateEntityCompatibility,
    };
}

/**
 * Normalize value for comparison (trim whitespace, handle null/undefined)
 */
function normalizeValue(value: any): string {
    if (value === null || value === undefined) {
        return '';
    }
    return String(value).trim();
}
