<<<<<<< HEAD
import { useEffect, useState } from 'react';

=======
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105
import {
    GetAutoCompleteMultipleResultsQuery,
    useGetAutoCompleteMultipleResultsLazyQuery,
} from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';
<<<<<<< HEAD
=======
import { useEffect, useState } from 'react';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

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
    }, [query, data, loading]);

    return { data: response, loading };
}
