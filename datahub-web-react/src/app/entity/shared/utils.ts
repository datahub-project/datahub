export function urlEncodeUrn(urn: string) {
    return (
        urn &&
        urn
            .replace(/%/g, '%25')
            .replace(/\//g, '%2F')
            .replace(/\?/g, '%3F')
            .replace(/#/g, '%23')
            .replace(/\[/g, '%5B')
            .replace(/\]/g, '%5D')
    );
}

export function getNumberWithOrdinal(n) {
    const suffixes = ['th', 'st', 'nd', 'rd'];
    const v = n % 100;
    return n + (suffixes[(v - 20) % 10] || suffixes[v] || suffixes[0]);
}

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

export const EDITED_DESCRIPTIONS_CACHE_NAME = 'editedDescriptions';
