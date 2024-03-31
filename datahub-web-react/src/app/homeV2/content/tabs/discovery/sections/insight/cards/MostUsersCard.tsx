import React from 'react';
import { FireTwoTone } from '@ant-design/icons';
import { buildMostUsersFilters, buildMostUsersSort } from './useGetMostUsers';
import { SearchListInsightCard } from './SearchListInsightCard';
import { EntityType } from '../../../../../../../../types.generated';
import { V2_HOME_PAGE_MOST_POPULAR_ID } from '../../../../../../../onboarding/configV2/HomePageOnboardingConfig';

export const MostUsersCard = () => {
    return (
        <SearchListInsightCard
            id={V2_HOME_PAGE_MOST_POPULAR_ID}
            icon={<FireTwoTone twoToneColor="orange" />}
            types={[EntityType.Dataset]}
            title="Most Popular Tables"
            filters={buildMostUsersFilters()}
            sort={buildMostUsersSort()}
        />
    );
};
