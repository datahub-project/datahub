import { ChangeCategoryType, ChangeOperationType, EntityType } from '@types';

// ──────────────────────────────────────────────────────────────────────────────
// Change event parameter keys — must match the backend change event generators.
// See metadata-io/.../timeline/eventgenerator/*ChangeEventGenerator.java
// ──────────────────────────────────────────────────────────────────────────────
export const PARAM_FIELD_PATH = 'fieldPath';
export const PARAM_DESCRIPTION = 'description';
export const PARAM_TAG_URN = 'tagUrn';
export const PARAM_TERM_URN = 'termUrn';
export const PARAM_RELATIONSHIP_TYPE = 'relationshipType';
export const PARAM_OWNER_URN = 'ownerUrn';
export const PARAM_OWNER_TYPE = 'ownerType';
export const PARAM_OWNER_TYPE_URN = 'ownerTypeUrn';
export const PARAM_DOMAIN_URN = 'domainUrn';
export const PARAM_PROPERTY_URN = 'propertyUrn';
export const PARAM_PROPERTY_VALUES = 'propertyValues';

// ──────────────────────────────────────────────────────────────────────────────
// Glossary relationship type labels — maps backend relationship type strings
// (emitted by GlossaryRelatedTermsChangeEventGenerator) to short display labels.
// These are the formal type names the backend uses ("Is A", "Has A", etc.),
// distinct from the UI tab labels ("Contains", "Inherits").
// ──────────────────────────────────────────────────────────────────────────────
export const GLOSSARY_RELATIONSHIP_TYPE_LABELS: Record<string, string> = {
    'Is A': 'inherited',
    'Has A': 'contained',
    'Has Value': 'value',
    'Is Related To': 'related',
};

// Owner types that don't add useful context in change event display
export const UNINFORMATIVE_OWNER_TYPES = new Set(['NONE', 'CUSTOM']);

// ──────────────────────────────────────────────────────────────────────────────
// Change categories not yet in the generated ChangeCategoryType enum
// (pending GraphQL schema update). Use these string constants until then.
// ──────────────────────────────────────────────────────────────────────────────
export const CATEGORY_DOMAIN = 'DOMAIN';
export const CATEGORY_STRUCTURED_PROPERTY = 'STRUCTURED_PROPERTY';
export const CATEGORY_APPLICATION = 'APPLICATION';
export const CATEGORY_ASSET_MEMBERSHIP = 'ASSET_MEMBERSHIP';

// Re-export generated enums for convenience within the history feature
export { ChangeCategoryType, ChangeOperationType };

// Category value type — includes generated enum values plus string constants
// for categories not yet in the GraphQL schema.
export type ChangeCategoryValue = ChangeCategoryType | string;

export type CategoryOption = { value: ChangeCategoryValue; label: string };

export const ALL_CATEGORY_OPTIONS: CategoryOption[] = [
    { value: ChangeCategoryType.TechnicalSchema, label: 'Schema' },
    { value: ChangeCategoryType.Documentation, label: 'Documentation' },
    { value: ChangeCategoryType.Tag, label: 'Tags' },
    { value: ChangeCategoryType.GlossaryTerm, label: 'Terms' },
    { value: ChangeCategoryType.Ownership, label: 'Owners' },
    { value: CATEGORY_DOMAIN, label: 'Domains' },
    { value: CATEGORY_STRUCTURED_PROPERTY, label: 'Properties' },
    { value: CATEGORY_APPLICATION, label: 'Applications' },
    { value: CATEGORY_ASSET_MEMBERSHIP, label: 'Assets' },
];

// Supported categories per entity type, matching the backend registry in TimelineServiceImpl.java
export const ENTITY_SUPPORTED_CATEGORIES: Partial<Record<EntityType, Set<ChangeCategoryValue>>> = {
    [EntityType.Dataset]: new Set([
        ChangeCategoryType.TechnicalSchema,
        ChangeCategoryType.Documentation,
        ChangeCategoryType.Tag,
        ChangeCategoryType.GlossaryTerm,
        ChangeCategoryType.Ownership,
        CATEGORY_DOMAIN,
        CATEGORY_STRUCTURED_PROPERTY,
        CATEGORY_APPLICATION,
    ]),
    [EntityType.GlossaryTerm]: new Set([
        ChangeCategoryType.Documentation,
        ChangeCategoryType.GlossaryTerm,
        ChangeCategoryType.Ownership,
        CATEGORY_DOMAIN,
        CATEGORY_STRUCTURED_PROPERTY,
        CATEGORY_APPLICATION,
    ]),
    [EntityType.Domain]: new Set([
        ChangeCategoryType.Documentation,
        ChangeCategoryType.Ownership,
        CATEGORY_STRUCTURED_PROPERTY,
    ]),
    [EntityType.DataProduct]: new Set([
        ChangeCategoryType.Documentation,
        ChangeCategoryType.Tag,
        ChangeCategoryType.GlossaryTerm,
        ChangeCategoryType.Ownership,
        CATEGORY_DOMAIN,
        CATEGORY_STRUCTURED_PROPERTY,
        CATEGORY_APPLICATION,
        CATEGORY_ASSET_MEMBERSHIP,
    ]),
};

export function getCategoryOptions(entityType?: EntityType): CategoryOption[] {
    const supported = entityType ? ENTITY_SUPPORTED_CATEGORIES[entityType] : undefined;
    if (!supported) return ALL_CATEGORY_OPTIONS;
    return ALL_CATEGORY_OPTIONS.filter((o) => supported.has(o.value));
}
