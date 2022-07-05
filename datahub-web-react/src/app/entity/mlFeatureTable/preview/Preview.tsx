import React from 'react';
import { EntityType, Owner } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../Entity';

export const Preview = ({
    urn,
    name,
    description,
    owners,
    logoUrl,
    platformName,
    platformInstanceId,
}: {
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    logoUrl?: string | null;
    platformName?: string | null;
    platformInstanceId?: string;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.MlfeatureTable, urn)}
            name={name}
            description={description || ''}
            type={entityRegistry.getEntityName(EntityType.MlfeatureTable)}
            typeIcon={entityRegistry.getIcon(EntityType.MlfeatureTable, 14, IconStyleType.ACCENT)}
            owners={owners}
            logoUrl={logoUrl || undefined}
            platform={platformName || ''}
            platformInstanceId={platformInstanceId}
            logoComponent={entityRegistry.getIcon(EntityType.MlfeatureTable, 20, IconStyleType.HIGHLIGHT)}
        />
    );
};
