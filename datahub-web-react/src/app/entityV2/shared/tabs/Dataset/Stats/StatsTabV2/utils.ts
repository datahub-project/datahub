import { EntityType } from '@src/types.generated';

export const isPresent = (val?: string | number | null): val is string | number => {
    return val !== undefined && val !== null;
};

/**
 * Computes a set of the object keys across all items in a given array.
 */
export const getItemKeySet = (items: Array<any>) => {
    const keySet = new Set<string>();
    items.forEach((item) => {
        Object.keys(item).forEach((key) => {
            keySet.add(key);
        });
    });
    return keySet;
};

export const getMockTopUsersData = (noOfUsers: number) => {
    const user = {
        type: EntityType.CorpUser,
        urn: 'urn:li:corpuser:user_name',
        username: 'user_name',
    };
    const count = 0;
    return Array.from({ length: noOfUsers }, (_, index) => ({
        user,
        count,
        id: index,
    }));
};

export enum SectionKeys {
    ROWS_AND_USERS = 'rowsAndUsers',
    QUERIES = 'queries',
    STORAGE = 'storage',
    CHANGES = 'changes',
    COLUMN_STATS = 'columnStats',
}
