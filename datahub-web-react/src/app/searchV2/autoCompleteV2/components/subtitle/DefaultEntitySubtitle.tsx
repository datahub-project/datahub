import React from 'react';

import { EntitySubtitleProps } from '@app/searchV2/autoCompleteV2/components/subtitle/types';
import { getContextPath } from '@src/app/entityV2/shared/containers/profile/header/getContextPath';
import ContextPath from '@src/app/previewV2/ContextPath';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

export default function DefaultEntitySubtitle({ entity, color, colorLevel }: EntitySubtitleProps) {
    const entityRegistry = useEntityRegistryV2();
    const genericEntityProperties = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const parentEntities = getContextPath(genericEntityProperties);

    return (
        <ContextPath
            showPlatformText={false}
            entityType={entity.type}
            browsePaths={genericEntityProperties?.browsePathV2}
            parentEntities={parentEntities}
        />
    );
}
