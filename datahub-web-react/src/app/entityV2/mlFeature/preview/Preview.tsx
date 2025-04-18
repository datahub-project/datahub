import { GenericEntityProperties } from '@app/entity/shared/types';
import React from 'react';
import { BrowsePathV2, DataPlatform, DataProduct, EntityPath, EntityType, Owner } from '../../../../types.generated';
import DefaultPreviewCard from '../../../previewV2/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType, PreviewType } from '../../Entity';
import { EntityMenuItems } from '../../shared/EntityDropdown/EntityMenuActions';

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
