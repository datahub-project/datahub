import React from 'react';

import ViewInPlatform from '@app/entityV2/shared/externalUrl/ViewInPlatform';
import { useEntityData } from '@src/app/entity/shared/EntityContext';

export default function ExternalUrlMenuAction() {
    const { urn: entityUrn, entityData } = useEntityData();

    return <ViewInPlatform urn={entityUrn} data={entityData} />;
}
