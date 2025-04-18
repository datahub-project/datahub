import React from 'react';
<<<<<<< HEAD

import { EntitySubtitleProps } from '@app/searchV2/autoCompleteV2/components/subtitle/types';
=======
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105
import { getContextPath } from '@src/app/entityV2/shared/containers/profile/header/getContextPath';
import ContextPath from '@src/app/previewV2/ContextPath';
import useContentTruncation from '@src/app/shared/useContentTruncation';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
<<<<<<< HEAD
=======
import { EntitySubtitleProps } from './types';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

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
        />
    );
}
