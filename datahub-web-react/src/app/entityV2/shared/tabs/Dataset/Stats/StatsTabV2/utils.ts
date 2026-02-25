import { GetDatasetQuery } from '@src/graphql/dataset.generated';
import { CorpGroup, CorpUser, EntityType } from '@src/types.generated';

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
    ROWS = 'rows',
    USERS = 'users',
}

export type SectionsToDisplay = Exclude<SectionKeys, SectionKeys.ROWS | SectionKeys.USERS>;

export const getUserOrGroupAvatarUrl = (entity: CorpUser | CorpGroup) => {
    return entity.editableProperties?.pictureLink;
};

const hasStats = (entity) => {
    return (
        (entity?.latestFullTableProfile?.length || 0) > 0 ||
        (entity?.latestPartitionProfile?.length || 0) > 0 ||
        (entity?.usageStats?.buckets?.length || 0) > 0 ||
        (entity?.operations?.length || 0) > 0
    );
};

export const getSiblingEntityWithStats = (baseEntity: GetDatasetQuery) => {
    const areStatsPresentInBaseEntity = hasStats(baseEntity.dataset);
    const siblingEntity = baseEntity.dataset?.siblingsSearch?.searchResults[0]?.entity;
    if (!areStatsPresentInBaseEntity && hasStats(siblingEntity)) return siblingEntity?.urn;
    return baseEntity.dataset?.urn;
};

export const getIsSiblingsMode = (baseEntity: GetDatasetQuery, isSeparateSiblingsMode: boolean) => {
    if ((baseEntity.dataset?.siblingsSearch?.searchResults?.length || 0) > 0) {
        if (isSeparateSiblingsMode) return false;
        return true;
    }
    return false;
};
