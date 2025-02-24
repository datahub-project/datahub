import React from 'react';
import { useEntityData } from '../../../entity/shared/EntityContext';
import { useGetSearchResultsForMultipleQuery } from '../../../../graphql/search.generated';
import { EntityType, FilterOperator } from '../../../../types.generated';
import { HorizontalList } from '../../shared/summary/ListComponents';
import { HeaderTitle } from '../../shared/summary/HeaderComponents';
import { getSubTypeIcon, SubType } from '../../shared/components/subtypes';
import { CONTAINER_FILTER_NAME, TYPE_NAMES_FILTER_NAME } from '../../../searchV2/utils/constants';
import SummaryEntityCard from '../../../sharedV2/cards/SummaryEntityCard';

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
