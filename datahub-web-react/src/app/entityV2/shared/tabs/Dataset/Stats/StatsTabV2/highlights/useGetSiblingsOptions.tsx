/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { GenericEntityProperties } from '@src/app/entity/shared/types';
import PlatformIcon from '@src/app/sharedV2/icons/PlatformIcon';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { DataPlatform, Dataset, EntityType } from '@src/types.generated';

type SiblingOption = {
    title: string;
    urn: string;
    platform?: DataPlatform;
};

interface Props {
    baseEntityData: Dataset;
}

export const useGetSiblingsOptions = ({ baseEntityData }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    const options: SiblingOption[] = [];
    options.push({
        urn: baseEntityData.urn,
        title: entityRegistry.getDisplayName(EntityType.DataPlatform, baseEntityData.platform),
        platform: baseEntityData?.platform ?? baseEntityData?.dataPlatformInstance?.platform,
    });

    const siblings: GenericEntityProperties[] =
        baseEntityData?.siblingsSearch?.searchResults?.map((res) => res.entity) || [];

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
