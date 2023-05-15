import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import { FilterOperator } from '../../types.generated';
import { UnionType } from '../search/utils/constants';
import { generateOrFilters } from '../search/utils/generateOrFilters';

const useLookupByUrl = (url: string) => {
    const { data, loading, error } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                start: 0,
                count: 2,
                orFilters: generateOrFilters(
                    UnionType.OR,
                    ['externalUrl', 'chartUrl', 'dashboardUrl'].map((field) => ({
                        field,
                        values: [url],
                        condition: FilterOperator.Equal,
                    })),
                ),
            },
        },
    });

    return { data, loading, error } as const;
};

export default useLookupByUrl;
