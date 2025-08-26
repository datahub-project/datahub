import React from 'react';

import ViewInPlatform from '@app/entityV2/shared/externalUrl/ViewInPlatform';
import { useEntityData } from '@src/app/entity/shared/EntityContext';

interface Props {
    shouldFillAllAvailableSpace?: boolean;
}

export default function ExternalUrlMenuAction({ shouldFillAllAvailableSpace }: Props) {
    const { urn: entityUrn, entityData } = useEntityData();

    return (
        <ViewInPlatform urn={entityUrn} data={entityData} shouldFillAllAvailableSpace={shouldFillAllAvailableSpace} />
    );
}
