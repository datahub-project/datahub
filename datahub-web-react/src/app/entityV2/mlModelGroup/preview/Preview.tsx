import { GenericEntityProperties } from '@app/entity/shared/types';
import React from 'react';
import { EntityPath, EntityType, MlModelGroup } from '../../../../types.generated';
import DefaultPreviewCard from '../../../previewV2/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { getDataProduct } from '../../shared/utils';
import { EntityMenuItems } from '../../shared/EntityDropdown/EntityMenuActions';
import { PreviewType } from '../../Entity';

export const Preview = ({
    data,
    group,
    degree,
    paths,
    isOutputPort,
    headerDropdownItems,
    previewType,
}: {
    data: GenericEntityProperties | null;
    group: MlModelGroup;
    degree?: number;
    paths?: EntityPath[];
    isOutputPort?: boolean;
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType?: PreviewType;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const genericProperties = entityRegistry.getGenericEntityProperties(EntityType.MlmodelGroup, group);
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.MlmodelGroup, group.urn)}
            name={data?.name || ''}
            urn={group.urn}
            data={data}
            platformInstanceId={group.dataPlatformInstance?.instanceId}
            description={group?.description || ''}
            entityType={EntityType.MlmodelGroup}
            logoUrl={group?.platform?.properties?.logoUrl || ''}
            platform={group?.platform?.properties?.displayName || capitalizeFirstLetterOnly(group?.platform?.name)}
            qualifier={group?.origin}
            owners={group?.ownership?.owners}
            dataProduct={getDataProduct(genericProperties?.dataProduct)}
            degree={degree}
            paths={paths}
            isOutputPort={isOutputPort}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
        />
    );
};
