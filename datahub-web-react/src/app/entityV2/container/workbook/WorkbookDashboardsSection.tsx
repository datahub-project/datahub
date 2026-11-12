import React from 'react';

import { useEntityData } from '@app/entityV2/shared/EntityContext';
import { HeaderTitle } from '@app/entityV2/shared/summary/HeaderComponents';
import { HorizontalList } from '@app/entityV2/shared/summary/ListComponents';
import { CONTAINER_FILTER_NAME } from '@app/searchV2/utils/constants';
import SummaryEntityCard from '@app/sharedV2/cards/SummaryEntityCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

/** The dashboards in a workbook container, such as a Qlik app's sheets. */
export default function WorkbookDashboardsSection() {
    const { urn } = useEntityData();
    const entityRegistry = useEntityRegistry();

    const { data } = useGetSearchResultsForMultipleQuery({
        skip: !urn,
        variables: {
            input: {
                types: [EntityType.Dashboard],
                query: '',
                orFilters: [{ and: [{ field: CONTAINER_FILTER_NAME, values: [urn] }] }],
                count: 1000,
            },
        },
        fetchPolicy: 'cache-first',
    });

    const dashboards = data?.searchAcrossEntities?.searchResults.map((r) => r.entity);
    if (!dashboards?.length) {
        return null;
    }
    return (
        <>
            <HeaderTitle>
                {entityRegistry.getCollectionName(EntityType.Dashboard)} ({dashboards.length})
            </HeaderTitle>
            <HorizontalList>
                {dashboards.map((dashboard) => (
                    <SummaryEntityCard key={dashboard.urn} entity={dashboard} />
                ))}
            </HorizontalList>
        </>
    );
}
