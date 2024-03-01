import React from 'react';
import { EntityPath, EntityType, MlModelGroup } from '../../../../types.generated';
import DefaultPreviewCard from '../../../previewV2/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { getDataProduct } from '../../shared/utils';
import { EntityMenuItems } from '../../shared/EntityDropdown/EntityMenuActions';

export const Preview = ({
    group,
    degree,
    paths,
    isOutputPort,
    headerDropdownItems,
}: {
    group: MlModelGroup;
    degree?: number;
    paths?: EntityPath[];
    isOutputPort?: boolean;
    headerDropdownItems?: Set<EntityMenuItems>;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const genericProperties = entityRegistry.getGenericEntityProperties(EntityType.MlmodelGroup, group);
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.MlmodelGroup, group.urn)}
            name={group?.name || ''}
            urn={group.urn}
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
        />
    );
};
