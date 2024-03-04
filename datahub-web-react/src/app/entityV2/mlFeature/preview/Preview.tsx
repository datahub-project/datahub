import React from 'react';
import { DataPlatform, DataProduct, EntityPath, EntityType, Owner } from '../../../../types.generated';
import DefaultPreviewCard from '../../../previewV2/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType } from '../../Entity';
import { EntityMenuItems } from '../../shared/EntityDropdown/EntityMenuActions';

export const Preview = ({
    urn,
    name,
    platformInstanceId,
    featureNamespace,
    description,
    dataProduct,
    owners,
    platform,
    degree,
    paths,
    isOutputPort,
    headerDropdownItems,
}: {
    urn: string;
    name: string;
    featureNamespace: string;
    platformInstanceId?: string;
    description?: string | null;
    dataProduct?: DataProduct | null;
    owners?: Array<Owner> | null;
    platform?: DataPlatform | null | undefined;
    degree?: number;
    paths?: EntityPath[];
    isOutputPort?: boolean;
    headerDropdownItems?: Set<EntityMenuItems>;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Mlfeature, urn)}
            name={name}
            urn={urn}
            platformInstanceId={platformInstanceId}
            description={description || ''}
            platform={
                platform?.properties?.displayName || capitalizeFirstLetterOnly(platform?.name) || featureNamespace
            }
            logoUrl={platform?.properties?.logoUrl || ''}
            entityType={EntityType.Mlfeature}
            typeIcon={entityRegistry.getIcon(EntityType.Mlfeature, 14, IconStyleType.ACCENT)}
            owners={owners}
            dataProduct={dataProduct}
            degree={degree}
            paths={paths}
            isOutputPort={isOutputPort}
            headerDropdownItems={headerDropdownItems}
        />
    );
};
