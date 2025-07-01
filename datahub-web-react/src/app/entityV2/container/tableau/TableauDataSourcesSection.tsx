import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { SubType, getSubTypeIcon } from '@app/entityV2/shared/components/subtypes';
import { HeaderTitle } from '@app/entityV2/shared/summary/HeaderComponents';
import { HorizontalList } from '@app/entityV2/shared/summary/ListComponents';
import { CONTAINER_FILTER_NAME, TYPE_NAMES_FILTER_NAME } from '@app/searchV2/utils/constants';
import SummaryEntityCard from '@app/sharedV2/cards/SummaryEntityCard';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType, FilterOperator } from '@types';

export default function TableauDataSourcesSection() {
    const { urn } = useEntityData();

    const { data: searchData } = useGetSearchResultsForMultipleQuery({
        skip: !urn,
        variables: {
            input: {
                types: [EntityType.Dataset],
                query: '',
                count: 10,
                orFilters: [
                    {
                        and: [
                            { field: CONTAINER_FILTER_NAME, values: [urn] },
                            {
                                field: TYPE_NAMES_FILTER_NAME,
                                values: [SubType.TableauEmbeddedDataSource, SubType.TableauPublishedDataSource],
                                condition: FilterOperator.Contain,
                            },
                        ],
                    },
                ],
            },
        },
        fetchPolicy: 'cache-first',
    });

    const dataSources = searchData?.searchAcrossEntities?.searchResults?.map((r) => r.entity);

    if (!dataSources?.length) {
        return null;
    }
    return (
        <>
            <HeaderTitle>
                {getSubTypeIcon(SubType.TableauPublishedDataSource)}
                Data Sources ({searchData?.searchAcrossEntities?.total})
            </HeaderTitle>
            <HorizontalList>
                {dataSources.map((dataSource) => (
                    <SummaryEntityCard key={dataSource.urn} entity={dataSource} />
                ))}
            </HorizontalList>
        </>
    );
}
