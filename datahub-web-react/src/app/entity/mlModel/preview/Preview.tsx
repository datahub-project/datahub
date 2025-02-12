import React from 'react';
import { EntityPath, EntityType, MlModel } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../Entity';
import { getDataProduct } from '../../shared/utils';

export const Preview = ({
    model,
    degree,
    paths,
}: {
    model: MlModel;
    degree?: number;
    paths?: EntityPath[];
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const genericProperties = entityRegistry.getGenericEntityProperties(EntityType.Mlmodel, model);

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Mlmodel, model.urn)}
            // eslint-disable-next-line @typescript-eslint/dot-notation
            name={model.properties?.['propertiesName'] || model.name || ''}
            urn={model.urn}
            description={model.description || ''}
            platformInstanceId={model.dataPlatformInstance?.instanceId}
            type={entityRegistry.getEntityName(EntityType.Mlmodel)}
            typeIcon={entityRegistry.getIcon(EntityType.Mlmodel, 14, IconStyleType.ACCENT)}
            platform={model?.platform?.properties?.displayName || capitalizeFirstLetterOnly(model?.platform?.name)}
            qualifier={model.origin}
            tags={model.globalTags || undefined}
            owners={model?.ownership?.owners}
            dataProduct={getDataProduct(genericProperties?.dataProduct)}
            degree={degree}
            paths={paths}
        />
    );
};
