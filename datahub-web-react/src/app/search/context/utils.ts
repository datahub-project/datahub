import { MatchedField } from '../../../types.generated';
import { MATCHED_FIELD_MAPPING, MatchedFieldName, NormalizedMatchedFieldName } from './constants';

export const getMatchedFieldNames = (
    normalizedFieldName: NormalizedMatchedFieldName | undefined,
): Array<MatchedFieldName> => {
    if (normalizedFieldName && normalizedFieldName in MATCHED_FIELD_MAPPING)
        return MATCHED_FIELD_MAPPING[normalizedFieldName] ?? [];
    return [];
};

export const getMatchedFieldsByNames = (
    fields: Array<MatchedField> | undefined,
    names: Array<string>,
): Array<MatchedField> => {
    return fields?.filter((field) => names.includes(field.name)) ?? [];
};

export const getMatchedFieldByUrn = (fields: Array<MatchedField>, urn: string) => {
    return fields.some((field) => field.value === urn);
};
