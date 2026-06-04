import React from 'react';
import { useTranslation } from 'react-i18next';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { EntityList } from '@app/entityV2/shared/tabs/Entity/components/EntityList';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

export const DataJobFlowTab = () => {
    const { t } = useTranslation('entity.profile.tabs');
    const entity = useBaseEntity() as any;
    const dataJob = entity && entity.dataJob;
    const dataFlow = dataJob?.dataFlow;
    const entityRegistry = useEntityRegistry();
    const title = t('entity.partOf', { entityName: entityRegistry.getEntityName(EntityType.DataFlow) });
    return <EntityList title={title} type={EntityType.DataFlow} entities={dataFlow ? [dataFlow] : []} />;
};
