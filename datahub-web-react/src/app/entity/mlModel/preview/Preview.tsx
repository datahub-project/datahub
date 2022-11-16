import React from 'react';
import { EntityType, MlModel } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { capitalizeFirstLetter } from '../../../shared/textUtil';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../Entity';

export const Preview = ({ model }: { model: MlModel }): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const capitalPlatformName = capitalizeFirstLetter(model?.platform?.name || '');

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Mlmodel, model.urn)}
            name={model.name || ''}
            urn={model.urn}
            description={model.description || ''}
            platformInstanceId={model.dataPlatformInstance?.instanceId}
            type={entityRegistry.getEntityName(EntityType.Mlmodel)}
            typeIcon={entityRegistry.getIcon(EntityType.Mlmodel, 14, IconStyleType.ACCENT)}
            platform={capitalPlatformName}
            qualifier={model.origin}
            tags={model.globalTags || undefined}
            owners={model?.ownership?.owners}
        />
    );
};
