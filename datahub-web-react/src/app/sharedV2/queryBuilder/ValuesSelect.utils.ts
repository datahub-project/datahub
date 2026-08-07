import { TFunction } from 'i18next';

import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';

/**
 * Resolves the multi-select labeled chip text for a ValuesSelect property.
 * Falls back to capitalizing the raw property id when no override exists.
 */
export function getValuesSelectLabel(property: string | undefined, t: TFunction): string | undefined {
    switch (property) {
        case 'urn':
            return t('value.assetsLabel');
        case 'glossaryTerms':
            return t('value.termsLabel');
        case '_entityType':
            return t('value.typesLabel');
        case 'typeNames':
            return t('value.subTypesLabel');
        case 'fieldPaths':
            return t('value.columnsLabel');
        case 'platformInstance':
            return t('value.instancesLabel');
        case 'owners':
            return t('value.ownersLabel');
        case 'parentDocument':
            return t('value.documentsLabel');
        default:
            return property ? capitalizeFirstLetterOnly(property) : undefined;
    }
}
