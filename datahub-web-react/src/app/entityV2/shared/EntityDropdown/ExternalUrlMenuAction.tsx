import ViewInPlatform from '@app/entityV2/shared/externalUrl/ViewInPlatform';
import React from 'react';
import { useEntityData } from '@src/app/entity/shared/EntityContext';

export default function ExternalUrlMenuAction() {
    const { urn: entityUrn } = useEntityData();

    return <ViewInPlatform urn={entityUrn} />;
}
