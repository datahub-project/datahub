import React from 'react';
import { EntityType, MlModel } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';
import { useEntityRegistry } from '../../../useEntityRegistry';

export const Preview = ({ model }: { model: MlModel }): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const capitalPlatformName = capitalizeFirstLetter(model?.platform?.name || '');

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Mlmodel, model.urn)}
            name={model.name || ''}
            description={model.description || ''}
            type="MLModel"
            logoUrl={model?.platform?.info?.logoUrl || ''}
            platform={capitalPlatformName}
            qualifier={model.origin}
            tags={model.globalTags || undefined}
            owners={model?.ownership?.owners}
        />
    );
};
