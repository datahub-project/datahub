import { useMemo } from 'react';

import {
    ChildrenLoaderInputType,
    ChildrenLoaderResultType,
} from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
import { convertEntityToTreeNode } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/utils';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { AndFilterInput } from '@types';

interface Input extends ChildrenLoaderInputType {
    orFilters: AndFilterInput[];
}

export default function useRelatedEntitiesLoader({
    parentValue,
    metadata,
    maxNumberToLoad,
    orFilters,
    dependenciesIsLoading,
}: Input): ChildrenLoaderResultType {
    const numberOfAlreadyLoadedRelatedEntities = metadata?.numberOfLoadedRelatedEntities ?? 0;

    const numberOfRelatedEntitiesToLoad = useMemo(() => {
        if (maxNumberToLoad <= 0) return 0;
        // Try to load rest of children When we don't know total number of related entities
        if (metadata?.totalNumberOfRelatedEntities === undefined) return maxNumberToLoad;

        const numberToLoad = Math.max(
            metadata.totalNumberOfRelatedEntities - (metadata.numberOfLoadedRelatedEntities ?? 0),
            0,
        );
        return Math.min(numberToLoad, maxNumberToLoad);
    }, [metadata, maxNumberToLoad]);

    const shouldLoadRelatedEntities =
        numberOfRelatedEntitiesToLoad > 0 || metadata?.totalNumberOfRelatedEntities === undefined;

    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                orFilters,
                start: numberOfAlreadyLoadedRelatedEntities,
                count: numberOfRelatedEntitiesToLoad,
            },
        },
        skip: !shouldLoadRelatedEntities || dependenciesIsLoading,
    });

    const treeNodes: TreeNode[] | undefined = useMemo(() => {
        return (
            data?.searchAcrossEntities?.searchResults
                .map((result) => result.entity)
                .map((entity) => convertEntityToTreeNode(entity, parentValue)) ?? []
        );
    }, [data, parentValue]);

    const total = useMemo(() => data?.searchAcrossEntities?.total, [data?.searchAcrossEntities?.total]);

    const isLoading = useMemo(() => {
        if (dependenciesIsLoading) return true;
        if (!shouldLoadRelatedEntities) return false;

        return loading || treeNodes === undefined;
    }, [shouldLoadRelatedEntities, loading, treeNodes, dependenciesIsLoading]);

    return {
        nodes: treeNodes,
        total,
        loading: isLoading,
    };
}
