import * as QueryString from 'query-string';
import { EntityType, MatchedField } from '../../../types.generated';
import {
    FIELDS_TO_HIGHLIGHT,
    MATCHED_FIELD_MAPPING,
    MatchFieldMapping,
    MatchedFieldName,
    MatchesGroupedByFieldName,
    NormalizedMatchedFieldName,
} from './constants';

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
    const highlightedMatches = matches.filter((field) => FIELDS_TO_HIGHLIGHT.has(field.name));
    return getMatchesGroupedByFieldName(highlightedMatches);
};
