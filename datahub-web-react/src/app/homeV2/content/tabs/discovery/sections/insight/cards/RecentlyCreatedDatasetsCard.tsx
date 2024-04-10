import React from 'react';
import { ClockCircleOutlined } from '@ant-design/icons';
import { SearchListInsightCard } from './SearchListInsightCard';
import { EntityType } from '../../../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../../../../entity/shared/constants';
import { buildRecentlyCreatedDatasetsFilters, buildRecentlyCreatedDatasetsSort } from './useRecentlyCreatedDatasets';

const MAX_AGE_DAYS = 14;

export const RecentlyCreatedDatasetsCard = () => {
    return (
        <SearchListInsightCard
            icon={<ClockCircleOutlined style={{ color: ANTD_GRAY[7] }} />}
            types={[EntityType.Dataset]}
            tip="Tables created in the last 2 weeks"
            title="Recently Created Tables"
            filters={buildRecentlyCreatedDatasetsFilters(MAX_AGE_DAYS)}
            sort={buildRecentlyCreatedDatasetsSort()}
        />
    );
};
