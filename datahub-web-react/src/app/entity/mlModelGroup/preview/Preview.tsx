import React from 'react';
import { EntityType, MlModelGroup } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { capitalizeFirstLetter } from '../../../shared/textUtil';
import { useEntityRegistry } from '../../../useEntityRegistry';

export const Preview = ({ group }: { group: MlModelGroup }): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const capitalPlatformName = capitalizeFirstLetter(group?.platform?.name || '');

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.MlmodelGroup, group.urn)}
            name={group?.name || ''}
            platformInstanceId={group.dataPlatformInstance?.instanceId}
            description={group?.description || ''}
            type="MLModel Group"
            logoUrl={group?.platform?.properties?.logoUrl || ''}
            platform={capitalPlatformName}
            qualifier={group?.origin}
            owners={group?.ownership?.owners}
        />
    );
};
