import i18next from 'i18next';

// Enum keys map to GraphQL relationship fields and the values are used as comparison keys, so both stay
// stable (English) and must not be translated. User-facing labels are resolved via getRelatedTermTypeLabel.
export enum RelatedTermTypes {
    hasRelatedTerms = 'Contains',
    isRelatedTerms = 'Inherits',
    containedBy = 'Contained by',
    isAChildren = 'Inherited by',
}

const RELATED_TERM_TYPE_LABELS: Record<RelatedTermTypes, () => string> = {
    [RelatedTermTypes.hasRelatedTerms]: () => i18next.t('entity.types:glossaryTerm.relatedTermType.hasRelatedTerms'),
    [RelatedTermTypes.isRelatedTerms]: () => i18next.t('entity.types:glossaryTerm.relatedTermType.isRelatedTerms'),
    [RelatedTermTypes.containedBy]: () => i18next.t('entity.types:glossaryTerm.relatedTermType.containedBy'),
    [RelatedTermTypes.isAChildren]: () => i18next.t('entity.types:glossaryTerm.relatedTermType.isAChildren'),
};

export function getRelatedTermTypeLabel(type: string): string {
    return RELATED_TERM_TYPE_LABELS[type as RelatedTermTypes]?.() ?? type;
}
