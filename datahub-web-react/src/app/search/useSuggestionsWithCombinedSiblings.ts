import { useEffect, useState } from 'react';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '../../graphql/search.generated';
// import { CombinedSearchResult, combineSiblingsInSearchResults } from '../entity/shared/siblingUtils';

const useSuggestionsWithCombinedSiblings = () => {
    const [getSuggestions, { data }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const [suggestions, setSuggestions] = useState<Array<any>>();

    useEffect(() => {
        if (data === undefined) return;
        const newSuggestions = data.autoCompleteForMultiple?.suggestions;
        // todo - for each suggestion.entities, de-dupe those
        // ultimately constructing a new complex suggestions object where each type has a new list of CombinedSearchResult?
        const entities = newSuggestions?.map((suggestion) => suggestion.entities);
        // const combinedResults = combineSiblingsInSearchResults(newSuggestions);
        // setSuggestions(combinedResults);
        setSuggestions(newSuggestions);
    }, [data]);

    return [getSuggestions, { suggestions } as const] as const;
};

export default useSuggestionsWithCombinedSiblings;
