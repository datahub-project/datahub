import React from 'react';

import { getParentEntities } from '@app/entityV2/shared/containers/profile/header/getParentEntities';
import { EntitySubtitleProps } from '@app/searchV2/autoCompleteV2/components/subtitle/types';
import ContextPath from '@src/app/previewV2/ContextPath';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

export default function DefaultEntitySubtitle({ entity }: EntitySubtitleProps) {
    const entityRegistry = useEntityRegistryV2();
    const genericEntityProperties = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const parentEntities = getParentEntities(genericEntityProperties, entity.type);

    return (
        <ContextPath
            showPlatformText={false}
            entityType={entity.type}
            browsePaths={genericEntityProperties?.browsePathV2}
            parentEntities={parentEntities}
        />
    );
}
