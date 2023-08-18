import * as QueryString from 'query-string';
import { EntityType, MatchedField } from '../../../types.generated';
import { MATCHED_FIELD_CONFIG, MatchedFieldConfig, MatchedFieldName, MatchesGroupedByFieldName } from './constants';

const getFieldConfigByEntityType = (
    entityType: EntityType | undefined,
): Record<MatchedFieldName, MatchedFieldConfig> => {
    return entityType && entityType in MATCHED_FIELD_CONFIG
        ? MATCHED_FIELD_CONFIG[entityType]
        : MATCHED_FIELD_CONFIG.DEFAULT;
};

export const shouldShowInMatchedFieldList = (entityType: EntityType | undefined, field: MatchedField): boolean => {
    const config = getFieldConfigByEntityType(entityType);
    return field.name in config && !!config[field.name].showInMatchedFieldList;
};

export const getMatchedFieldLabel = (entityType: EntityType | undefined, fieldName: string): string => {
    const config = getFieldConfigByEntityType(entityType);
    return fieldName in config ? config[fieldName].label : '';
};

export const getMatchedFieldNames = (
    entityType: EntityType | undefined,
    fieldName: MatchedFieldName,
): Array<MatchedFieldName> => {
    return Object.values(getFieldConfigByEntityType(entityType))
        .filter((config) => fieldName === config.normalizedName || fieldName === config.name)
        .map((field) => field.name);
};

export const getMatchedFieldsByNames = (fields: Array<MatchedField>, names: Array<string>): Array<MatchedField> => {
    return fields.filter((field) => names.includes(field.name));
};

export const getMatchedFieldsByUrn = (fields: Array<MatchedField>, urn: string): Array<MatchedField> => {
    return fields.filter((field) => field.value === urn);
};

function normalize(value: string) {
    return value.trim().toLowerCase();
}

function fromQueryGetBestMatch(
    selectedMatchedFields: MatchedField[],
    rawQuery: string,
    primaryField: string,
): Array<MatchedField> {
    const query = normalize(rawQuery);
    const primaryMatches: Array<MatchedField> = selectedMatchedFields.filter((field) => field.name === primaryField);
    const nonPrimaryMatches: Array<MatchedField> = selectedMatchedFields.filter((field) => field.name !== primaryField);
    const exactMatches: Array<MatchedField> = [];
    const containedMatches: Array<MatchedField> = [];
    const rest: Array<MatchedField> = [];

    [...primaryMatches, ...nonPrimaryMatches].forEach((field) => {
        const normalizedValue = normalize(field.value);
        if (normalizedValue === query) exactMatches.push(field);
        else if (normalizedValue.includes(query)) containedMatches.push(field);
        else rest.push(field);
    });

    return [...exactMatches, ...containedMatches, ...rest];
}

const getMatchesGroupedByFieldName = (matchedFields: Array<MatchedField>): Array<MatchesGroupedByFieldName> => {
    const fieldNameToMatches = new Map<string, Array<MatchedField>>();
    const fieldNames: Array<string> = [];
    matchedFields.forEach((field) => {
        const matchesInMap = fieldNameToMatches.get(field.name);
        if (matchesInMap) {
            matchesInMap.push(field);
        } else {
            fieldNameToMatches.set(field.name, [field]);
            fieldNames.push(field.name);
        }
    });
    return fieldNames.map((fieldName) => ({
        fieldName,
        matchedFields: fieldNameToMatches.get(fieldName) ?? [],
    }));
};

export const getMatchesPrioritizingPrimary = (
    matchedFields: MatchedField[],
    primaryField: string,
): Array<MatchesGroupedByFieldName> => {
    const { location } = window;
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const query: string = decodeURIComponent(params.query ? (params.query as string) : '');
    const matches = fromQueryGetBestMatch(matchedFields, query, primaryField);
    return getMatchesGroupedByFieldName(matches);
};
