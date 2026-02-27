import { ChangeCategoryType, EntityType } from '@types';

export type CategoryOption = { value: ChangeCategoryType; label: string };

export const ALL_CATEGORY_OPTIONS: CategoryOption[] = [
    { value: ChangeCategoryType.TechnicalSchema, label: 'Schema' },
    { value: ChangeCategoryType.Documentation, label: 'Documentation' },
    { value: ChangeCategoryType.Tag, label: 'Tags' },
    { value: ChangeCategoryType.GlossaryTerm, label: 'Terms' },
    { value: ChangeCategoryType.Ownership, label: 'Owners' },
    { value: ChangeCategoryType.Domain, label: 'Domains' },
    { value: ChangeCategoryType.StructuredProperty, label: 'Properties' },
    { value: ChangeCategoryType.Application, label: 'Applications' },
];

// Supported categories per entity type, matching the backend registry in TimelineServiceImpl.java
export const ENTITY_SUPPORTED_CATEGORIES: Partial<Record<EntityType, Set<ChangeCategoryType>>> = {
    [EntityType.Dataset]: new Set([
        ChangeCategoryType.TechnicalSchema,
        ChangeCategoryType.Documentation,
        ChangeCategoryType.Tag,
        ChangeCategoryType.GlossaryTerm,
        ChangeCategoryType.Ownership,
        ChangeCategoryType.Domain,
        ChangeCategoryType.StructuredProperty,
    ]),
    [EntityType.GlossaryTerm]: new Set([
        ChangeCategoryType.Documentation,
        ChangeCategoryType.GlossaryTerm,
        ChangeCategoryType.Ownership,
        ChangeCategoryType.Domain,
        ChangeCategoryType.StructuredProperty,
        ChangeCategoryType.Application,
    ]),
    [EntityType.Domain]: new Set([
        ChangeCategoryType.Documentation,
        ChangeCategoryType.Ownership,
        ChangeCategoryType.StructuredProperty,
    ]),
};

export function getCategoryOptions(entityType?: EntityType): CategoryOption[] {
    const supported = entityType ? ENTITY_SUPPORTED_CATEGORIES[entityType] : undefined;
    if (!supported) return ALL_CATEGORY_OPTIONS;
    return ALL_CATEGORY_OPTIONS.filter((o) => supported.has(o.value));
}
