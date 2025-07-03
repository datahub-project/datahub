import React from 'react';
import { useTheme } from 'styled-components';

import { getColor } from '@components/theme/utils';

import { EntitySubtitleProps } from '@app/searchV2/autoCompleteV2/components/subtitle/types';
import { getContextPath } from '@src/app/entityV2/shared/containers/profile/header/getContextPath';
import ContextPath from '@src/app/previewV2/ContextPath';
import useContentTruncation from '@src/app/shared/useContentTruncation';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

export default function DefaultEntitySubtitle({ entity, color, colorLevel }: EntitySubtitleProps) {
    const theme = useTheme();
    const entityRegistry = useEntityRegistryV2();
    const genericEntityProperties = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const parentEntities = getContextPath(genericEntityProperties);

    const { contentRef, isContentTruncated } = useContentTruncation(genericEntityProperties);

    return (
        <ContextPath
            instanceId={genericEntityProperties?.dataPlatformInstance?.instanceId}
            showPlatformText={false}
            entityType={entity.type}
            browsePaths={genericEntityProperties?.browsePathV2}
            parentEntities={parentEntities}
            contentRef={contentRef}
            isContentTruncated={isContentTruncated}
            color={getColor(color, colorLevel, theme)}
        />
    );
}
