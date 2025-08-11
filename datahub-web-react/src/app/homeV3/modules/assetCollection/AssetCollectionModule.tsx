import { InfiniteScrollList } from '@components';
import React, { useCallback, useMemo } from 'react';

import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { DataHubPageModuleType, Entity } from '@types';

const DEFAULT_PAGE_SIZE = 6;

const AssetCollectionModule = (props: ModuleProps) => {
    const assetUrns = useMemo(
        () =>
            props.module.properties.params.assetCollectionParams?.assetUrns.filter(
                (urn): urn is string => typeof urn === 'string',
            ) || [],
        [props.module.properties.params.assetCollectionParams?.assetUrns],
    );

    const { loading, refetch } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                start: 0,
                count: DEFAULT_PAGE_SIZE,
                query: '*',
                filters: [{ field: 'urn', values: assetUrns }],
            },
        },
        skip: assetUrns.length === 0,
    });

    const fetchEntities = useCallback(
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
            return urnSlice.map((urn) => urnToEntity.get(urn)).filter((entity): entity is Entity => !!entity);
        },
        [assetUrns, refetch],
    );

    return (
        <LargeModule {...props} loading={loading} dataTestId="asset-collection-module">
            <div data-testid="asset-collection-entities">
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
                    totalItemCount={assetUrns.length}
                />
            </div>
        </LargeModule>
    );
};

export default AssetCollectionModule;
