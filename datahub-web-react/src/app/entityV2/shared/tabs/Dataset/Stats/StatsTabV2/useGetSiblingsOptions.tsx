import { GenericEntityProperties } from '@src/app/entity/shared/types';
import PlatformIcon from '@src/app/sharedV2/icons/PlatformIcon';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { DataPlatform, Dataset, EntityType } from '@src/types.generated';
import React from 'react';

type SiblingOption = {
    title: string;
    urn: string;
    platform?: DataPlatform;
};

interface Props {
    primaryEntityData: Dataset;
}

export const useGetSiblingsOptions = ({ primaryEntityData }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    const options: SiblingOption[] = [];
    options.push({
        urn: primaryEntityData.urn,
        title: entityRegistry.getDisplayName(EntityType.DataPlatform, primaryEntityData.platform),
        platform: primaryEntityData?.platform ?? primaryEntityData?.dataPlatformInstance?.platform,
    });

    const siblings: GenericEntityProperties[] =
        primaryEntityData?.siblingsSearch?.searchResults?.map((res) => res.entity) || [];

    siblings.forEach((sibling) => {
        if (!sibling.urn) {
            return;
        }
        options.push({
            urn: sibling?.urn,
            title: entityRegistry.getDisplayName(EntityType.DataPlatform, sibling.platform),
            platform: sibling?.platform ?? sibling?.dataPlatformInstance?.platform,
        });
    });

    const siblingsOptions = options.map((option) => ({
        value: option.urn,
        label: option.title,
        icon: <PlatformIcon platform={option.platform} size={16} />,
    }));

    return siblingsOptions;
};
