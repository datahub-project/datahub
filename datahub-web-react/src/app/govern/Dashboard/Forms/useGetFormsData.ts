import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';

export const useGetFormsData = () => {
    const inputs = {
        types: [EntityType.Form],
        query: '*',
        start: 0,
        count: 200,
        searchFlags: { skipCache: true },
    };

    // Execute search
    const {
        data: searchData,
        loading,
        refetch,
        networkStatus,
    } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: inputs,
        },
        fetchPolicy: 'cache-first',
        notifyOnNetworkStatusChange: true,
    });

    return { inputs, searchData, loading, refetch, networkStatus };
};
