import React from 'react';
import { useTranslation } from 'react-i18next';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { EntityList } from '@app/entityV2/shared/tabs/Entity/components/EntityList';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

export const DashboardDatasetsTab = () => {
    const { t } = useTranslation('entity.profile.tabs');
    const entity = useBaseEntity() as any;
    const dashboard = entity && entity.dashboard;
    const datasets = dashboard?.datasets?.relationships?.map((relationship) => relationship.entity);
    const entityRegistry = useEntityRegistry();
    const totalDatasets = dashboard?.datasets?.total || 0;
    const entityName =
        totalDatasets === 1
            ? entityRegistry.getEntityName(EntityType.Dataset)
            : entityRegistry.getCollectionName(EntityType.Dataset);
    const title = t('entity.consumesCount', { count: totalDatasets, entityName });
    return <EntityList title={title} type={EntityType.Dataset} entities={datasets || []} />;
};
