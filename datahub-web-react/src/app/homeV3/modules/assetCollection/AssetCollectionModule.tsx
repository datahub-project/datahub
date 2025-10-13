import { InfiniteScrollList } from '@components';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import { sortByUrnOrder } from '@app/homeV3/modules/assetCollection/utils';
import { excludeEmptyAndFilters } from '@app/searchV2/utils/filterUtils';
import { LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { convertLogicalPredicateToOrFilters } from '@app/sharedV2/queryBuilder/builder/utils';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { DataHubPageModuleType, Entity } from '@types';

const ContentWrapper = styled.div`
    height: 100%;
`;

const DEFAULT_PAGE_SIZE = 10;

const AssetCollectionModule = (props: ModuleProps) => {
    const [isFirstFetch, setIsFirstFetch] = useState(true);
    const assetUrns = useMemo(
        () =>
            props.module.properties.params.assetCollectionParams?.assetUrns.filter(
                (urn): urn is string => typeof urn === 'string',
            ) || [],
        [props.module.properties.params.assetCollectionParams?.assetUrns],
    );

    const dynamicFilterLogicalPredicate: LogicalPredicate | undefined = useMemo(
        () =>
            props.module.properties.params.assetCollectionParams?.dynamicFilterJson
                ? JSON.parse(props.module.properties.params.assetCollectionParams?.dynamicFilterJson)
                : undefined,
        [props.module.properties.params.assetCollectionParams?.dynamicFilterJson],
    );

    const shouldFetchByDynamicFilter = useMemo(
        () => assetUrns.length === 0 && !!dynamicFilterLogicalPredicate,
        [assetUrns, dynamicFilterLogicalPredicate],
    );

    const dynamicOrFilters = useMemo(() => {
        if (dynamicFilterLogicalPredicate) {
            const orFilters = excludeEmptyAndFilters(convertLogicalPredicateToOrFilters(dynamicFilterLogicalPredicate));
            return orFilters;
        }
        return undefined;
    }, [dynamicFilterLogicalPredicate]);

    const totalForInfiniteScroll = useMemo(
        () => (shouldFetchByDynamicFilter ? undefined : assetUrns.length),
        [shouldFetchByDynamicFilter, assetUrns],
    );

    const { data, loading, refetch } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                start: 0,
                count: DEFAULT_PAGE_SIZE,
                query: '*',
                ...(shouldFetchByDynamicFilter
                    ? { orFilters: dynamicOrFilters }
                    : {
                          filters: [{ field: 'urn', values: assetUrns.slice(0, DEFAULT_PAGE_SIZE) }],
                      }),
            },
        },
        skip: assetUrns.length === 0 && !dynamicOrFilters?.length,
        onCompleted: () => {
            setIsFirstFetch(false);
        },
        fetchPolicy: 'cache-first',
    });

    const initialEntities = useMemo(
        () =>
            data?.searchAcrossEntities?.searchResults
                ?.map((res) => res.entity)
                .filter((entity): entity is Entity => !!entity) || [],
        [data?.searchAcrossEntities?.searchResults],
    );

    const fetchEntitiesByDynamicFilter = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            if (!dynamicOrFilters?.length) return [];

            const result = await refetch({
                input: {
                    start,
                    count,
                    query: '*',
                    orFilters: dynamicOrFilters,
                },
            });

            const results =
                result.data?.searchAcrossEntities?.searchResults
                    ?.map((res) => res.entity)
                    ?.filter((entity): entity is Entity => !!entity) || [];

            return results;
        },
        [dynamicOrFilters, refetch],
    );

    const fetchEntitiesByAssetUrns = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            if (assetUrns.length === 0) return [];
            // urn slicing is done at the front-end to maintain the order of assets to show with pagination
            const urnSlice = assetUrns.slice(start, start + count);
            const result = await refetch({
                input: {
                    start: 0, // Using start as 0 every time because sliced urns are sent
                    count: urnSlice.length,
                    query: '*',
                    filters: [{ field: 'urn', values: urnSlice }],
                },
            });

            const results =
                result.data?.searchAcrossEntities?.searchResults
                    ?.map((res) => res.entity)
                    .filter((entity): entity is Entity => !!entity) || [];

            const urnToEntity = new Map(results.map((e) => [e.urn, e]));
            return sortByUrnOrder(
                urnSlice.map((urn) => urnToEntity.get(urn)).filter((entity): entity is Entity => !!entity),
                assetUrns,
            );
        },
        [assetUrns, refetch],
    );

    const fetchEntities = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            if (isFirstFetch) {
                return sortByUrnOrder(initialEntities, assetUrns);
            }
            if (shouldFetchByDynamicFilter) {
                return fetchEntitiesByDynamicFilter(start, count);
            }
            return fetchEntitiesByAssetUrns(start, count);
        },
        [
            isFirstFetch,
            shouldFetchByDynamicFilter,
            fetchEntitiesByAssetUrns,
            initialEntities,
            fetchEntitiesByDynamicFilter,
            assetUrns,
        ],
    );

    return (
        <LargeModule {...props} loading={loading} dataTestId="asset-collection-module">
            <ContentWrapper data-testid="asset-collection-entities">
                <InfiniteScrollList<Entity>
                    key={assetUrns.join(',')}
                    fetchData={fetchEntities}
                    renderItem={(entity) => (
                        <EntityItem
                            entity={entity}
                            key={entity?.urn}
                            moduleType={DataHubPageModuleType.AssetCollection}
                        />
                    )}
                    pageSize={DEFAULT_PAGE_SIZE}
                    emptyState={
                        <EmptyContent
                            icon="Stack"
                            title="No Assets"
                            description="Edit the module and add assets to see them in this list"
                        />
                    }
                    totalItemCount={totalForInfiniteScroll}
                />
            </ContentWrapper>
        </LargeModule>
    );
};

export default AssetCollectionModule;
