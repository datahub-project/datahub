import { useEffect, useState } from 'react';

import {
    GetAutoCompleteMultipleResultsQuery,
    useGetAutoCompleteMultipleResultsLazyQuery,
} from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';

const LIMIT_OF_SUGGESTIONS = 20;

export default function useAutocompleteResults(query: string, entityTypes: EntityType[]) {
    const [response, setResponse] = useState<GetAutoCompleteMultipleResultsQuery | undefined>();

    const [getSearchResults, { data, loading }] = useGetAutoCompleteMultipleResultsLazyQuery();

    useEffect(() => {
        if (query !== '') {
            getSearchResults({
                variables: {
                    input: {
                        query,
                        types: entityTypes,
                        limit: LIMIT_OF_SUGGESTIONS,
                    },
                },
            });
        }
    }, [query, entityTypes, getSearchResults]);

    useEffect(() => {
        if (query === '') {
            setResponse(undefined);
            return;
        }

        if (!loading) setResponse(data);
        // if (!loading) setResponse(data?.autoCompleteForMultiple?.suggestions?.filter(suggestion => entityTypes.includes(suggestion.type)));
    }, [query, data, loading]);

    return { data: response, loading };
}
