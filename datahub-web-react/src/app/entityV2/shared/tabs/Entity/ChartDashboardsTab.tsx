import React from 'react';
import { useTranslation } from 'react-i18next';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { EntityList } from '@app/entityV2/shared/tabs/Entity/components/EntityList';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

export const ChartDashboardsTab = () => {
    const { t } = useTranslation('entity.profile.tabs');
    const entity = useBaseEntity() as any;
    const chart = entity && entity.chart;
    const dashboards = chart?.dashboards?.relationships?.map((relationship) => relationship.entity);
    const entityRegistry = useEntityRegistry();
    const totalDashboards = chart?.dashboards?.total || 0;
    const entityName =
        totalDashboards === 1
            ? entityRegistry.getEntityName(EntityType.Dashboard)
            : entityRegistry.getCollectionName(EntityType.Dashboard);
    const title = t('entity.foundInCount', { count: totalDashboards, entityName });
    return <EntityList title={title} type={EntityType.Dashboard} entities={dashboards || []} />;
};
