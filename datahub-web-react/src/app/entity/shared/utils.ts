import { MatchedField } from '../../../types.generated';
import { FIELDS_TO_HIGHLIGHT } from '../dataset/search/highlights';
import { GenericEntityProperties } from './types';

export function dictToQueryStringParams(params: Record<string, string | boolean>) {
    return Object.keys(params)
        .map((key) => `${key}=${params[key]}`)
        .join('&');
}

export function urlEncodeUrn(urn: string) {
    return (
        urn &&
        urn
            // Hack - React Router v5 does not like pre-url-encoded paths. Since URNs can contain free form IDs, there's nothing preventing them from having percentages.
            // If we use double encoded paths, React ends up decoding them fully, which breaks our ability to read urns properly.
            .replace(/%/g, '{{encoded_percent}}')
            .replace(/\//g, '%2F')
            .replace(/\?/g, '%3F')
            .replace(/#/g, '%23')
            .replace(/\[/g, '%5B')
            .replace(/\]/g, '%5D')
    );
}

export function decodeUrn(encodedUrn: string) {
    // Hack-This is not ideal because it means that if you had the percent
    // sequence in your urn things may not work as expected.
    return decodeURIComponent(encodedUrn).replace(/{{encoded_percent}}/g, '%');
}

export function getNumberWithOrdinal(n) {
    const suffixes = ['th', 'st', 'nd', 'rd'];
    const v = n % 100;
    return n + (suffixes[(v - 20) % 10] || suffixes[v] || suffixes[0]);
}

export const encodeComma = (str: string) => {
    return str.replace(/,/g, '%2C');
};

export const decodeComma = (str: string) => {
    return str.replace(/%2C/g, ',');
};

export function notEmpty<TValue>(value: TValue | null | undefined): value is TValue {
    return value !== null && value !== undefined;
}

export const truncate = (length: number, input?: string | null) => {
    if (!input) return '';
    if (input.length > length) {
        return `${input.substring(0, length)}...`;
    }
    return input;
};

export const singularizeCollectionName = (collectionName: string): string => {
    if (!collectionName) {
        return collectionName;
    }

    const lastChar = collectionName[collectionName.length - 1];
    if (lastChar === 's') {
        return collectionName.slice(0, -1);
    }

    return collectionName;
};

export function getPlatformName(entityData: GenericEntityProperties | null) {
    return entityData?.platform?.properties?.displayName || entityData?.platform?.name;
}

export const EDITED_DESCRIPTIONS_CACHE_NAME = 'editedDescriptions';

export const FORBIDDEN_URN_CHARS_REGEX = /.*[(),\\].*/;

/**
 * Utility function for checking whether a list is a subset of another.
 */
export const isListSubset = (l1, l2): boolean => {
    return l1.every((result) => l2.indexOf(result) >= 0);
};

export const getMatchPrioritizingPrimary = (
    matchedFields: MatchedField[],
    primaryField: string,
): MatchedField | undefined => {
    const primaryMatch = matchedFields.find((field) => field.name === primaryField);
    if (primaryMatch) {
        return primaryMatch;
    }

    return matchedFields.find((field) => FIELDS_TO_HIGHLIGHT.has(field.name));
};
