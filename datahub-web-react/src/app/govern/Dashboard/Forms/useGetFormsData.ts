import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';

export const LIST_FORMS_INPUTS = {
    types: [EntityType.Form],
    query: '*',
    start: 0,
    count: 500,
    searchFlags: { skipCache: true },
};

export const useGetFormsData = () => {
    // Execute search
    const {
        data: searchData,
        loading,
        refetch,
        networkStatus,
    } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: LIST_FORMS_INPUTS,
        },
        fetchPolicy: 'cache-first',
        notifyOnNetworkStatusChange: true,
    });

    return { inputs: LIST_FORMS_INPUTS, searchData, loading, refetch, networkStatus };
};
