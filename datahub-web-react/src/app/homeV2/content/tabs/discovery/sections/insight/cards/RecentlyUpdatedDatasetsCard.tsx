import React from 'react';
import { useTranslation } from 'react-i18next';

import { SearchListInsightCard } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/SearchListInsightCard';
import {
    buildRecentlyUpdatedDatasetsFilters,
    buildRecentlyUpdatedDatasetsSort,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useRecentlyUpdatedDatasets';

import { EntityType } from '@types';

const MAX_AGE_DAYS = 14;

export const RECENTLY_UPDATED_ID = 'RecentlyUpdatedDatasets';

export const RecentlyUpdatedDatasetsCard = () => {
    const { t } = useTranslation('home.v2');
    return (
        <SearchListInsightCard
            id={RECENTLY_UPDATED_ID}
            types={[EntityType.Dataset]}
            tip={t('insights.recentlyUpdatedTip')}
            title={t('insights.recentlyUpdatedTitle')}
            filters={buildRecentlyUpdatedDatasetsFilters(MAX_AGE_DAYS)}
            sort={buildRecentlyUpdatedDatasetsSort()}
        />
    );
};
