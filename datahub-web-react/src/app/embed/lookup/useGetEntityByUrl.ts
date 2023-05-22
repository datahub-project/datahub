import { useMemo } from 'react';
import { useGetSearchResultsForMultipleQuery } from '../../../graphql/search.generated';
import { FilterOperator } from '../../../types.generated';
import { UnionType } from '../../search/utils/constants';
import { generateOrFilters } from '../../search/utils/generateOrFilters';

const URL_FIELDS = ['externalUrl', 'chartUrl', 'dashboardUrl'] as const;

const useGetEntityByUrl = (externalUrl: string) => {
    const { data, loading, error } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                start: 0,
                count: 2,
                orFilters: generateOrFilters(
                    UnionType.OR,
                    URL_FIELDS.map((field) => ({
                        field,
                        values: [externalUrl],
                        condition: FilterOperator.Equal,
                    })),
                ),
            },
        },
    });

    const entities = data?.searchAcrossEntities?.searchResults.map((result) => result.entity) ?? [];
    const count = entities.length;
    const entity = count === 1 ? entities[0] : null;

    return useMemo(
        () =>
            ({
                count,
                entity,
                loading,
                error,
            } as const),
        [count, entity, error, loading],
    );
};

export default useGetEntityByUrl;
