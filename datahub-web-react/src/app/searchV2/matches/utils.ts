import * as QueryString from 'query-string';
import { EntityType, MatchedField } from '../../../types.generated';
import {
    HIGHLIGHTABLE_ENTITY_TYPES,
    MATCHED_FIELD_CONFIG,
    MatchedFieldConfig,
    MatchedFieldName,
    MatchesGroupedByFieldName,
} from './constants';

const getFieldConfigsByEntityType = (entityType: EntityType | undefined): Array<MatchedFieldConfig> => {
    return entityType && entityType in MATCHED_FIELD_CONFIG
        ? MATCHED_FIELD_CONFIG[entityType]
        : MATCHED_FIELD_CONFIG.DEFAULT;
};

export const shouldShowInMatchedFieldList = (entityType: EntityType | undefined, field: MatchedField): boolean => {
    const configs = getFieldConfigsByEntityType(entityType);
    return configs.some((config) => config.name === field.name && config.showInMatchedFieldList);
};

export const getMatchedFieldLabel = (entityType: EntityType | undefined, fieldName: string): string => {
    const configs = getFieldConfigsByEntityType(entityType);
    return configs.find((config) => config.name === fieldName)?.label ?? '';
};

export const getGroupedFieldName = (
    entityType: EntityType | undefined,
    fieldName: string,
): MatchedFieldName | undefined => {
    const configs = getFieldConfigsByEntityType(entityType);
    const fieldConfig = configs.find((config) => config.name === fieldName);
    return fieldConfig?.groupInto;
};

export const getMatchedFieldNames = (
    entityType: EntityType | undefined,
    fieldName: MatchedFieldName,
): Array<MatchedFieldName> => {
    return getFieldConfigsByEntityType(entityType)
        .filter((config) => fieldName === config.groupInto || fieldName === config.name)
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
    prioritizedField: string,
): Array<MatchedField> {
    const query = normalize(rawQuery);
    const priorityMatches: Array<MatchedField> = selectedMatchedFields.filter(
        (field) => field.name === prioritizedField,
    );
    const nonPriorityMatches: Array<MatchedField> = selectedMatchedFields.filter(
        (field) => field.name !== prioritizedField,
    );
    const exactMatches: Array<MatchedField> = [];
    const containedMatches: Array<MatchedField> = [];
    const rest: Array<MatchedField> = [];

    [...priorityMatches, ...nonPriorityMatches].forEach((field) => {
        const normalizedValue = normalize(field.value);
        if (normalizedValue === query) exactMatches.push(field);
        else if (normalizedValue.includes(query)) containedMatches.push(field);
        else rest.push(field);
    });

    return [...exactMatches, ...containedMatches, ...rest];
}

const getMatchesGroupedByFieldName = (
    entityType: EntityType,
    matchedFields: Array<MatchedField>,
): Array<MatchesGroupedByFieldName> => {
    const fieldNameToMatches = new Map<string, Array<MatchedField>>();
    const fieldNames: Array<string> = [];
    matchedFields.forEach((field) => {
        const groupedFieldName = getGroupedFieldName(entityType, field.name) || field.name;
        const matchesInMap = fieldNameToMatches.get(groupedFieldName);
        if (matchesInMap) {
            matchesInMap.push(field);
        } else {
            fieldNameToMatches.set(groupedFieldName, [field]);
            fieldNames.push(groupedFieldName);
        }
    });
    return fieldNames.map((fieldName) => ({
        fieldName,
        matchedFields: fieldNameToMatches.get(fieldName) ?? [],
    }));
};

export const getMatchesPrioritized = (
    entityType: EntityType,
    matchedFields: MatchedField[],
    prioritizedField: string,
): Array<MatchesGroupedByFieldName> => {
    const { location } = window;
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const query: string = decodeURIComponent(params.query ? (params.query as string) : '');
    const matches = fromQueryGetBestMatch(matchedFields, query, prioritizedField);
    return getMatchesGroupedByFieldName(entityType, matches);
};

export const isHighlightableEntityField = (field: MatchedField) =>
    !!field.entity && HIGHLIGHTABLE_ENTITY_TYPES.includes(field.entity.type);

export const isDescriptionField = (field: MatchedField) => field.name.toLowerCase().includes('description');

const SURROUNDING_DESCRIPTION_CHARS = 10;
const MAX_DESCRIPTION_CHARS = 50;

export const getDescriptionSlice = (text: string, target: string) => {
    const queryIndex = text.indexOf(target);
    const start = Math.max(0, queryIndex - SURROUNDING_DESCRIPTION_CHARS);
    const end = Math.min(
        start + MAX_DESCRIPTION_CHARS,
        text.length,
        queryIndex + target.length + SURROUNDING_DESCRIPTION_CHARS,
    );
    const startEllipsis = start > 0 ? '...' : '';
    const endEllipsis = end < text.length ? '...' : '';
    return `${startEllipsis}${text.slice(start, end)}${endEllipsis}`;
};

export const getColumnsTabUrlPath = (entityType: EntityType) => {
    if (entityType === EntityType.Chart) {
        return 'Fields';
    }
    return 'Columns';
};
