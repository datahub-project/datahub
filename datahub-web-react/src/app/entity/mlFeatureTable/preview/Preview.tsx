import React from 'react';
import { DataProduct, EntityType, Owner } from '../../../../types.generated';
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
    dataProduct,
    platformInstanceId,
}: {
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    logoUrl?: string | null;
    platformName?: string | null;
    dataProduct?: DataProduct | null;
    platformInstanceId?: string;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.MlfeatureTable, urn)}
            name={name}
            urn={urn}
            description={description || ''}
            type={entityRegistry.getEntityName(EntityType.MlfeatureTable)}
            typeIcon={entityRegistry.getIcon(EntityType.MlfeatureTable, 14, IconStyleType.ACCENT)}
            owners={owners}
            logoUrl={logoUrl || undefined}
            platform={platformName || ''}
            platformInstanceId={platformInstanceId}
            dataProduct={dataProduct}
            logoComponent={entityRegistry.getIcon(EntityType.MlfeatureTable, 20, IconStyleType.HIGHLIGHT)}
        />
    );
};
