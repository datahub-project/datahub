import React from 'react';
import { EntityPath, EntityType, MlModelGroup } from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { getDataProduct } from '../../shared/utils';

export const Preview = ({
    group,
    degree,
    paths,
}: {
    group: MlModelGroup;
    degree?: number;
    paths?: EntityPath[];
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const genericProperties = entityRegistry.getGenericEntityProperties(EntityType.MlmodelGroup, group);
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.MlmodelGroup, group.urn)}
            // eslint-disable-next-line @typescript-eslint/dot-notation
            name={group?.properties?.['propertiesName'] || group?.name || ''}
            urn={group.urn}
            platformInstanceId={group.dataPlatformInstance?.instanceId}
            description={group?.description || ''}
            type="MLModel Group"
            logoUrl={group?.platform?.properties?.logoUrl || ''}
            platform={group?.platform?.properties?.displayName || capitalizeFirstLetterOnly(group?.platform?.name)}
            qualifier={group?.origin}
            owners={group?.ownership?.owners}
            dataProduct={getDataProduct(genericProperties?.dataProduct)}
            degree={degree}
            paths={paths}
        />
    );
};
