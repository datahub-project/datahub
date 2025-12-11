/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { EntityList } from '@app/entity/shared/tabs/Entity/components/EntityList';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

export const DataJobFlowTab = () => {
    const entity = useBaseEntity() as any;
    const dataJob = entity && entity.dataJob;
    const dataFlow = dataJob?.dataFlow;
    const entityRegistry = useEntityRegistry();
    const title = `Part of ${entityRegistry.getEntityName(EntityType.DataFlow)}`;
    return <EntityList title={title} type={EntityType.DataFlow} entities={dataFlow ? [dataFlow] : []} />;
};
