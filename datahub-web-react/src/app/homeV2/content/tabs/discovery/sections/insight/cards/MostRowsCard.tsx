import React from 'react';
import { useTranslation } from 'react-i18next';

import { SearchListInsightCard } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/SearchListInsightCard';
import {
    buildMostRowsFilters,
    buildMostRowsSort,
} from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useGetMostRows';

import { EntityType } from '@types';

export const MOST_ROWS_ID = 'MostRows';

export const MostRowsCard = () => {
    const { t } = useTranslation('home.v2');
    return (
        <SearchListInsightCard
            id={MOST_ROWS_ID}
            title={t('insights.mostRowsTitle')}
            types={[EntityType.Dataset]}
            filters={buildMostRowsFilters()}
            sort={buildMostRowsSort()}
        />
    );
};
