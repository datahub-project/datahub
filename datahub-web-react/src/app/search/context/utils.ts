import { EntityType, MatchedField } from '../../../types.generated';
import { MATCHED_FIELD_MAPPING, MatchFieldMapping, MatchedFieldName, NormalizedMatchedFieldName } from './constants';

const getFieldMappingByEntityType = (entityType: EntityType | undefined): MatchFieldMapping => {
    return entityType && entityType in MATCHED_FIELD_MAPPING
        ? MATCHED_FIELD_MAPPING[entityType]
        : MATCHED_FIELD_MAPPING.DEFAULT;
};

export const getMatchedFieldNames = (
    entityType: EntityType | undefined,
    normalizedFieldName: NormalizedMatchedFieldName | undefined,
): Array<MatchedFieldName> => {
    const fieldMapping = getFieldMappingByEntityType(entityType);
    return normalizedFieldName && normalizedFieldName in fieldMapping ? fieldMapping[normalizedFieldName] : [];
};

export const getMatchedFieldsByNames = (fields: Array<MatchedField>, names: Array<string>): Array<MatchedField> => {
    return fields?.filter((field) => names.includes(field.name)) ?? [];
};

export const getMatchedFieldByUrn = (fields: Array<MatchedField>, urn: string) => {
    return fields.some((field) => field.value === urn);
};
