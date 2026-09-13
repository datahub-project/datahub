import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataProduct, EntityPath, EntityType, Health, Owner } from '@types';

export const Preview = ({
    urn,
    data,
    name,
    description,
    owners,
    logoUrl,
    platformName,
    dataProduct,
    platformInstanceId,
    health,
    degree,
    paths,
    isOutputPort,
    headerDropdownItems,
    previewType,
}: {
    urn: string;
    data: GenericEntityProperties | null;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    logoUrl?: string | null;
    platformName?: string | null;
    dataProduct?: DataProduct | null;
    platformInstanceId?: string;
    health?: Health[] | null;
    degree?: number;
    paths?: EntityPath[];
    isOutputPort?: boolean;
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType: PreviewType;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.MlfeatureTable, urn)}
            name={name}
            urn={urn}
            data={data}
            description={description || ''}
            entityType={EntityType.MlfeatureTable}
            typeIcon={entityRegistry.getIcon(EntityType.MlfeatureTable, 14, IconStyleType.ACCENT)}
            owners={owners}
            logoUrl={logoUrl || undefined}
            platform={platformName || ''}
            platformInstanceId={platformInstanceId}
            dataProduct={dataProduct}
            logoComponent={entityRegistry.getIcon(EntityType.MlfeatureTable, 20, IconStyleType.HIGHLIGHT)}
            health={health || undefined}
            degree={degree}
            paths={paths}
            isOutputPort={isOutputPort}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
        />
    );
};
