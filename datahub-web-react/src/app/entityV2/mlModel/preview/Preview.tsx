import { GenericEntityProperties } from '@app/entity/shared/types';
import React from 'react';
import { EntityPath, EntityType, MlModel } from '../../../../types.generated';
import DefaultPreviewCard from '../../../previewV2/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType, PreviewType } from '../../Entity';
import { getDataProduct } from '../../shared/utils';
import { EntityMenuItems } from '../../shared/EntityDropdown/EntityMenuActions';

export const Preview = ({
    data,
    model,
    degree,
    paths,
    isOutputPort,
    headerDropdownItems,
    previewType,
}: {
    data: GenericEntityProperties | null;
    model: MlModel;
    degree?: number;
    paths?: EntityPath[];
    isOutputPort?: boolean;
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType?: PreviewType;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const genericProperties = entityRegistry.getGenericEntityProperties(EntityType.Mlmodel, model);

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Mlmodel, model.urn)}
            name={data?.name || ''}
            urn={model.urn}
            data={data}
            description={model.description || ''}
            platformInstanceId={model.dataPlatformInstance?.instanceId}
            entityType={EntityType.Mlmodel}
            typeIcon={entityRegistry.getIcon(EntityType.Mlmodel, 14, IconStyleType.ACCENT)}
            platform={model?.platform?.properties?.displayName || capitalizeFirstLetterOnly(model?.platform?.name)}
            qualifier={model.origin}
            tags={model.globalTags || undefined}
            owners={model?.ownership?.owners}
            dataProduct={getDataProduct(genericProperties?.dataProduct)}
            degree={degree}
            paths={paths}
            isOutputPort={isOutputPort}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
        />
    );
};
