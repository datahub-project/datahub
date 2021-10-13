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
}: {
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.MlfeatureTable, urn)}
            name={name}
            description={description || ''}
            type={entityRegistry.getEntityName(EntityType.MlfeatureTable)}
            owners={owners}
            logoComponent={entityRegistry.getIcon(EntityType.MlfeatureTable, 20, IconStyleType.HIGHLIGHT)}
        />
    );
};
