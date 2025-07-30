import Icon from '@ant-design/icons';
import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { HeaderTitle } from '@app/entityV2/shared/summary/HeaderComponents';
import { HorizontalList } from '@app/entityV2/shared/summary/ListComponents';
import { CONTAINER_FILTER_NAME } from '@app/searchV2/utils/constants';
import SummaryEntityCard from '@app/sharedV2/cards/SummaryEntityCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

import TableauViewIcon from '@images/tableau-view.svg?react';

const viewPattern = /.*tableau.com.*\/#(\/site\/[^/]*)?\/views\/(.*)/;

export default function TableauViewsSection() {
    const { urn } = useEntityData();

    const { data } = useGetSearchResultsForMultipleQuery({
        skip: !urn,
        variables: {
            input: {
                types: [EntityType.Chart, EntityType.Dashboard],
                query: '',
                orFilters: [{ and: [{ field: CONTAINER_FILTER_NAME, values: [urn] }] }],
                // TODO: Store this information in a filterable property
                count: 1000,
            },
        },
        fetchPolicy: 'cache-first',
    });

    const entityRegistry = useEntityRegistry();
    const views = data?.searchAcrossEntities?.searchResults
        .filter((r) =>
            entityRegistry.getGenericEntityProperties(r.entity.type, r.entity)?.externalUrl?.match(viewPattern),
        )
        .map((r) => r.entity);

    if (!views?.length) {
        return null;
    }
    return (
        <>
            <HeaderTitle>
                <Icon component={TableauViewIcon} />
                Views ({views.length})
            </HeaderTitle>
            <HorizontalList>
                {views.map((view) => (
                    <SummaryEntityCard key={view.urn} entity={view} />
                ))}
            </HorizontalList>
        </>
    );
}
