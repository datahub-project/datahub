import ViewInPlatform from '@app/entityV2/shared/externalUrl/ViewInPlatform';
import React from 'react';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { getPlatformName } from '../utils';

export default function ExternalUrlMenuAction() {
    const { urn: entityUrn, entityData, entityType } = useEntityData();

    return (
        <ViewInPlatform
            urn={entityUrn}
            entityType={entityType}
            platform={getPlatformName(entityData)}
            externalUrl={entityData?.externalUrl}
        />
    );
}
