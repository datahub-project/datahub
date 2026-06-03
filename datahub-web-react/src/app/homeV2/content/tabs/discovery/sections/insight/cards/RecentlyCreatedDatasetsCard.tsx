import React from 'react';
import { useTranslation } from 'react-i18next';

import { SearchListInsightCard } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/SearchListInsightCard';
import {
    buildRecentlyCreatedDatasetsFilters,
    buildRecentlyCreatedDatasetsSort,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useRecentlyCreatedDatasets';

import { EntityType } from '@types';

const MAX_AGE_DAYS = 14;

export const RECENTLY_CREATED_DATASETS_ID = 'RecentlyCreatedDatasets';

export const RecentlyCreatedDatasetsCard = () => {
    const { t } = useTranslation('home.v2');
    return (
        <SearchListInsightCard
            id={RECENTLY_CREATED_DATASETS_ID}
            types={[EntityType.Dataset]}
            tip={t('insights.recentlyCreatedTip')}
            title={t('insights.recentlyCreatedTitle')}
            filters={buildRecentlyCreatedDatasetsFilters(MAX_AGE_DAYS)}
            sort={buildRecentlyCreatedDatasetsSort()}
        />
    );
};
