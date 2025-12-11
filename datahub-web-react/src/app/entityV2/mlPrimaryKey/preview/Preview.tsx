/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { IconStyleType, PreviewType } from '@app/entityV2/Entity';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataPlatform, DataProduct, EntityPath, EntityType, Owner } from '@types';

export const Preview = ({
    urn,
    data,
    name,
    featureNamespace,
    description,
    owners,
    platform,
    dataProduct,
    platformInstanceId,
    degree,
    paths,
    isOutputPort,
    previewType,
}: {
    urn: string;
    data: GenericEntityProperties | null;
    name: string;
    featureNamespace: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    platform?: DataPlatform | null | undefined;
    dataProduct?: DataProduct | null;
    platformInstanceId?: string;
    degree?: number;
    paths?: EntityPath[];
    isOutputPort?: boolean;
    previewType: PreviewType;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.MlprimaryKey, urn)}
            name={name}
            urn={urn}
            data={data}
            description={description || ''}
            platform={
                platform?.properties?.displayName || capitalizeFirstLetterOnly(platform?.name) || featureNamespace
            }
            logoUrl={platform?.properties?.logoUrl || ''}
            entityType={EntityType.MlprimaryKey}
            typeIcon={entityRegistry.getIcon(EntityType.MlprimaryKey, 14, IconStyleType.ACCENT)}
            owners={owners}
            dataProduct={dataProduct}
            platformInstanceId={platformInstanceId}
            degree={degree}
            paths={paths}
            isOutputPort={isOutputPort}
            previewType={previewType}
        />
    );
};
