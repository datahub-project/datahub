/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { IconStyleType } from '@app/entity/Entity';
import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DataProduct, EntityPath, EntityType, Owner } from '@types';

export const Preview = ({
    urn,
    name,
    description,
    owners,
    logoUrl,
    platformName,
    dataProduct,
    platformInstanceId,
    degree,
    paths,
}: {
    urn: string;
    name: string;
    description?: string | null;
    owners?: Array<Owner> | null;
    logoUrl?: string | null;
    platformName?: string | null;
    dataProduct?: DataProduct | null;
    platformInstanceId?: string;
    degree?: number;
    paths?: EntityPath[];
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.MlfeatureTable, urn)}
            name={name}
            urn={urn}
            description={description || ''}
            type={entityRegistry.getEntityName(EntityType.MlfeatureTable)}
            typeIcon={entityRegistry.getIcon(EntityType.MlfeatureTable, 14, IconStyleType.ACCENT)}
            owners={owners}
            logoUrl={logoUrl || undefined}
            platform={platformName || ''}
            platformInstanceId={platformInstanceId}
            dataProduct={dataProduct}
            logoComponent={entityRegistry.getIcon(EntityType.MlfeatureTable, 20, IconStyleType.HIGHLIGHT)}
            degree={degree}
            paths={paths}
        />
    );
};
