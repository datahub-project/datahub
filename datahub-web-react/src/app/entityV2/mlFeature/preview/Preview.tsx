import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { BrowsePathV2, DataPlatform, DataProduct, EntityPath, EntityType, Owner } from '@types';

export const Preview = ({
    urn,
    data,
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
    previewType,
    browsePaths,
}: {
    urn: string;
    data: GenericEntityProperties | null;
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
    previewType?: PreviewType;
    browsePaths?: BrowsePathV2 | undefined;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const platformName = platform?.properties?.displayName || capitalizeFirstLetterOnly(platform?.name);
    const platformTitle =
        platformName && featureNamespace
            ? `${platformName} > ${featureNamespace}`
            : platformName || featureNamespace || '';

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Mlfeature, urn)}
            name={name}
            urn={urn}
            data={data}
            platformInstanceId={platformInstanceId}
            description={description || ''}
            platform={platformTitle}
            logoUrl={platform?.properties?.logoUrl || ''}
            entityType={EntityType.Mlfeature}
            typeIcon={entityRegistry.getIcon(EntityType.Mlfeature, 14, IconStyleType.ACCENT)}
            owners={owners}
            dataProduct={dataProduct}
            degree={degree}
            paths={paths}
            isOutputPort={isOutputPort}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
            browsePaths={browsePaths}
        />
    );
};
