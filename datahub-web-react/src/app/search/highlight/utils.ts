import { EntityType } from '../../../types.generated';
import { MATCHED_FIELD_MAPPING, NormalizedMatchedFieldName } from './constants';

export const getMatchedFieldNames = (
    entityType: EntityType | undefined,
    normalizedFieldName: NormalizedMatchedFieldName | undefined,
): Array<string> => {
    if (
        normalizedFieldName &&
        entityType &&
        entityType in MATCHED_FIELD_MAPPING &&
        normalizedFieldName in MATCHED_FIELD_MAPPING[entityType]
    )
        return MATCHED_FIELD_MAPPING[entityType][normalizedFieldName] ?? [];
    return [];
};
