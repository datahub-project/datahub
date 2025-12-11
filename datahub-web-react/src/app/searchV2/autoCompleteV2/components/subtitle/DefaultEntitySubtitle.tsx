/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { getParentEntities } from '@app/entityV2/shared/containers/profile/header/getParentEntities';
import { EntitySubtitleProps } from '@app/searchV2/autoCompleteV2/components/subtitle/types';
import ContextPath from '@src/app/previewV2/ContextPath';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

export default function DefaultEntitySubtitle({ entity }: EntitySubtitleProps) {
    const entityRegistry = useEntityRegistryV2();
    const genericEntityProperties = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const parentEntities = getParentEntities(genericEntityProperties);

    return (
        <ContextPath
            showPlatformText={false}
            entityType={entity.type}
            browsePaths={genericEntityProperties?.browsePathV2}
            parentEntities={parentEntities}
        />
    );
}
