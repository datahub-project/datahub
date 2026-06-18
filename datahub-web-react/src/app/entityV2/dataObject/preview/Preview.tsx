import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { IconStyleType, PreviewType } from '@app/entityV2/Entity';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataObject, EntityType, Owner } from '@types';

export const Preview = ({
    dataObject: _dataObject,
    urn,
    data,
    name,
    description,
    platformName,
    platformLogo,
    platformInstanceId,
    owners,
    logoComponent,
    previewType,
}: {
    dataObject: DataObject;
    urn: string;
    data: GenericEntityProperties | null;
    name: string;
    description?: string | null;
    platformName?: string;
    platformLogo?: string | null;
    platformInstanceId?: string;
    owners?: Array<Owner> | null;
    logoComponent?: JSX.Element;
    previewType: PreviewType;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const truncatedDescription =
        description && description.length > 200 ? `${description.substring(0, 200)}...` : description;

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.DataObject, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            description={truncatedDescription || ''}
            entityType={EntityType.DataObject}
            platform={platformName}
            logoUrl={platformLogo || undefined}
            platformInstanceId={platformInstanceId}
            typeIcon={entityRegistry.getIcon(EntityType.DataObject, 14, IconStyleType.ACCENT)}
            owners={owners}
            logoComponent={logoComponent}
            previewType={previewType}
        />
    );
};
