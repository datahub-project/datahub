import { InfiniteScrollList } from '@components';
import React, { useCallback, useMemo } from 'react';

import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

import { useGetSearchResultsForMultipleLazyQuery } from '@graphql/search.generated';
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

    const [searchQuery, { loading }] = useGetSearchResultsForMultipleLazyQuery();

    const fetchEntities = useCallback(
        async (start: number, count: number): Promise<Entity[]> => {
            const urnSlice = assetUrns.slice(start, start + count);
            if (urnSlice.length === 0) return [];

            const { data } = await searchQuery({
                variables: {
                    input: {
                        start: 0,
                        count: urnSlice.length,
                        query: '*',
                        filters: [{ field: 'urn', values: urnSlice }],
                    },
                },
            });

            const results =
                data?.searchAcrossEntities?.searchResults
                    ?.map((res) => res.entity)
                    .filter((entity): entity is Entity => !!entity) || [];

            const urnToEntity = new Map(results.map((e) => [e.urn, e]));
            return urnSlice.map((urn) => urnToEntity.get(urn)).filter((entity): entity is Entity => !!entity);
        },
        [assetUrns, searchQuery],
    );

    return (
        <LargeModule {...props} loading={loading}>
            <InfiniteScrollList<Entity>
                key={assetUrns.join(',')}
                fetchData={fetchEntities}
                renderItem={(entity) => (
                    <EntityItem entity={entity} key={entity?.urn} moduleType={DataHubPageModuleType.AssetCollection} />
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
        </LargeModule>
    );
};

export default AssetCollectionModule;
