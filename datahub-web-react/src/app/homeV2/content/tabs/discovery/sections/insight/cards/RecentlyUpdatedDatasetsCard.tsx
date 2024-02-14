import React from 'react';
import { ClockCircleOutlined } from '@ant-design/icons';
import { SearchListInsightCard } from './SearchListInsightCard';
import { EntityType } from '../../../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../../../../entity/shared/constants';
import { buildRecentlyUpdatedDatasetsFilters, buildRecentlyUpdatedDatasetsSort } from './useRecentlyUpdatedDatasets';

const MAX_AGE = 1000 * 60 * 60 * 24 * 14; // 14 days

export const RecentlyUpdatedDatasetsCard = () => {
    return (
        <SearchListInsightCard
            icon={<ClockCircleOutlined style={{ color: ANTD_GRAY[7] }} />}
            types={[EntityType.Dataset]}
            tip="Tables updated in the last 2 weeks"
            title="Recently Updated Tables"
            filters={buildRecentlyUpdatedDatasetsFilters(MAX_AGE)}
            sort={buildRecentlyUpdatedDatasetsSort()}
        />
    );
};
