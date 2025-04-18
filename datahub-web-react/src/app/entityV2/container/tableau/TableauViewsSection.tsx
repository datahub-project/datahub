import React from 'react';
import Icon from '@ant-design/icons';
import { useEntityData } from '../../../entity/shared/EntityContext';
import { useGetSearchResultsForMultipleQuery } from '../../../../graphql/search.generated';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { HorizontalList } from '../../shared/summary/ListComponents';
import { HeaderTitle } from '../../shared/summary/HeaderComponents';
import TableauViewIcon from '../../../../images/tableau-view.svg?react';
import { CONTAINER_FILTER_NAME } from '../../../searchV2/utils/constants';
import SummaryEntityCard from '../../../sharedV2/cards/SummaryEntityCard';

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
