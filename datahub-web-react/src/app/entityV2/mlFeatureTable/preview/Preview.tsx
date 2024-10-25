import { GenericEntityProperties } from '@app/entity/shared/types';
import React from 'react';
import { DataProduct, EntityPath, EntityType, Owner } from '../../../../types.generated';
import DefaultPreviewCard from '../../../previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { IconStyleType, PreviewType } from '../../Entity';
import { EntityMenuItems } from '../../shared/EntityDropdown/EntityMenuActions';

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
    degree?: number;
    paths?: EntityPath[];
    isOutputPort?: boolean;
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType?: PreviewType;
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
            degree={degree}
            paths={paths}
            isOutputPort={isOutputPort}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
        />
    );
};
