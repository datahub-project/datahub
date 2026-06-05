import React from 'react';
import { useTranslation } from 'react-i18next';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { EntityList } from '@app/entityV2/shared/tabs/Entity/components/EntityList';

import { EntityType } from '@types';

export const DashboardChartsTab = () => {
    const { t } = useTranslation('entity.profile.tabs');
    const entity = useBaseEntity() as any;
    const dashboard = entity && entity.dashboard;
    const charts = dashboard?.charts?.relationships?.map((relationship) => relationship.entity);
    const totalCharts = dashboard?.charts?.total || 0;
    const title = t('entity.containsAssets', { count: totalCharts });
    return <EntityList title={title} type={EntityType.Chart} entities={charts || []} />;
};
