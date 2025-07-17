import { useMemo } from 'react';

import { FieldToAppliedFieldFiltersMap } from '@app/searchV2/filtersV2/types';
import { convertFiltersMapToFilters } from '@app/searchV2/filtersV2/utils';
import { UnionType } from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';

interface Props {
    searchQuery: string | undefined;
    appliedFilters?: FieldToAppliedFieldFiltersMap;
}

export default function useGetAssetResults({ searchQuery, appliedFilters }: Props) {
    const filters = useMemo(() => convertFiltersMapToFilters(appliedFilters), [appliedFilters]);
    const orFilters = generateOrFilters(UnionType.AND, filters);

    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: searchQuery || '*',
                start: 0,
                count: 10,
                orFilters,
                searchFlags: {
                    skipCache: true,
                },
            },
        },
    });

    const entities = data?.searchAcrossEntities?.searchResults?.map((res) => res.entity) || [];

    return {
        entities,
        loading,
    };
}
