import { FireTwoTone } from '@ant-design/icons';
import React from 'react';
import { EntityType } from '../../../../../../../../types.generated';
import { V2_HOME_PAGE_MOST_POPULAR_ID } from '../../../../../../../onboarding/configV2/HomePageOnboardingConfig';
import { SearchListInsightCard } from './SearchListInsightCard';
import { buildMostUsersFilters, buildMostUsersSort } from './useGetMostUsers';

export const MostUsersCard = () => {
    return (
        <SearchListInsightCard
            id={V2_HOME_PAGE_MOST_POPULAR_ID}
            tip="Tables with the most unique users in the past month"
            icon={<FireTwoTone twoToneColor="orange" />}
            types={[EntityType.Dataset]}
            title="Most Popular Tables"
            filters={buildMostUsersFilters()}
            sort={buildMostUsersSort()}
        />
    );
};
