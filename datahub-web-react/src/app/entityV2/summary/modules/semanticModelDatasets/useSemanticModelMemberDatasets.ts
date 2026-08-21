import { useMemo } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';

import { useGetSemanticModelMemberDatasetsQuery } from '@graphql/semanticModel.generated';
import { Dataset, EntityType } from '@types';

const DEFAULT_PAGE_SIZE = 20;

/**
 * Fetches member datasets for a SemanticModel profile summary page.
 * Shared by Datasets, Relationships, and Dimensions modules; Apollo cache-first
 * dedupes when multiple modules mount with the same variables.
 */
export function useSemanticModelMemberDatasets(): {
    datasets: Dataset[];
    total: number;
    loading: boolean;
} {
    const { urn } = useEntityData();
    const { data, loading } = useGetSemanticModelMemberDatasetsQuery({
        skip: !urn,
        variables: { urn: urn || '', start: 0, count: DEFAULT_PAGE_SIZE },
        fetchPolicy: 'cache-first',
    });

    const datasets = useMemo(
        () =>
            (data?.semanticModel?.entities?.searchResults ?? [])
                .map((result) => result?.entity)
                .filter((entity): entity is Dataset => entity?.type === EntityType.Dataset),
        [data?.semanticModel?.entities?.searchResults],
    );

    return {
        datasets,
        total: data?.semanticModel?.entities?.total ?? 0,
        loading,
    };
}
