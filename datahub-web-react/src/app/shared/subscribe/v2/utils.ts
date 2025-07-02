import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { getPlatformName } from '@src/app/entityV2/shared/utils';
import { DataPlatform, EntityType } from '@src/types.generated';

export type SubscriptionSiblingOptions = {
    title: string;
    urn: string;
    platform?: DataPlatform;
    entityType?: EntityType;
};

/**
 * Gets sibling options that a user can manage subscriptions on
 * @param entityData
 * @param urn
 * @param entityType
 * @returns {SubscriptionSiblingOptions[]}
 */
export const useSiblingOptionsForSubscriptions = (
    entityData: GenericEntityProperties | null,
    urn: string,
    entityType: EntityType,
    isSiblingMode: boolean,
): SubscriptionSiblingOptions[] => {
    const optionsToAuthorOn: SubscriptionSiblingOptions[] = [];
    // push main entity data
    optionsToAuthorOn.push({
        title:
            entityData?.platform?.properties?.displayName ??
            entityData?.platform?.name ??
            entityData?.dataPlatformInstance?.platform.name ??
            entityData?.platform?.urn ??
            entityData?.properties?.name ??
            entityData?.name ??
            urn,
        urn,
        platform: entityData?.platform ?? entityData?.dataPlatformInstance?.platform,
        entityType,
    });
    if (!isSiblingMode) {
        return optionsToAuthorOn;
    }
    // push siblings data
    const siblings: GenericEntityProperties[] = entityData?.siblingsSearch?.searchResults?.map((r) => r.entity) || [];
    // Count the number of entities on each platform
    const platformCountMap: Record<string, number> = siblings
        .map((sib) => sib.platform || sib.dataPlatformInstance?.platform)
        .concat(entityData?.platform || entityData?.dataPlatformInstance?.platform)
        .reduce(
            (map, platform) =>
                platform?.urn
                    ? {
                          ...map,
                          [platform.urn]: (map[platform.urn] || 0) + 1,
                      }
                    : map,
            {} as Record<string, number>,
        );
    // Push sibling options
    siblings.forEach((sibling) => {
        if (sibling.urn === urn || !sibling.urn) {
            return;
        }
        const newOption: SubscriptionSiblingOptions = {
            urn: sibling.urn,
            title:
                getPlatformName(sibling) ??
                sibling?.dataPlatformInstance?.platform?.name ??
                sibling?.platform?.urn ??
                sibling.urn,
            platform: sibling?.platform ?? sibling?.dataPlatformInstance?.platform,
            entityType: sibling.type,
        };
        // If there are multiple entities on the same platform, add the name to the title
        if ((platformCountMap[newOption.platform?.urn || ''] || 0) > 1) {
            newOption.title = `${newOption.title} (${sibling.name || sibling.editableProperties?.name || sibling.urn})`;
        }
        optionsToAuthorOn.push(newOption);
    });
    return optionsToAuthorOn;
};
