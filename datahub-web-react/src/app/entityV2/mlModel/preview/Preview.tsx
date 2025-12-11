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
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { getDataProduct } from '@app/entityV2/shared/utils';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityPath, EntityType, MlModel } from '@types';

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
    previewType: PreviewType;
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
