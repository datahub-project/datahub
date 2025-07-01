import { colors } from '@components';
import React from 'react';

import { EntitySubtitleProps } from '@app/searchV2/autoCompleteV2/components/subtitle/types';
import { SUBTITLE_COLOR, SUBTITLE_COLOR_LEVEL } from '@app/searchV2/autoCompleteV2/constants';
import { getContextPath } from '@src/app/entityV2/shared/containers/profile/header/getContextPath';
import ContextPath from '@src/app/previewV2/ContextPath';
import useContentTruncation from '@src/app/shared/useContentTruncation';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

export default function DefaultEntitySubtitle({ entity }: EntitySubtitleProps) {
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
            color={colors[SUBTITLE_COLOR][SUBTITLE_COLOR_LEVEL]}
        />
    );
}
