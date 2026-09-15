import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useDebounce } from 'react-use';

import EntitySearchInputResultV2 from '@app/entityV2/shared/EntitySearchInput/EntitySearchInputResultV2';
import AutoCompleteSelect from '@src/alchemy-components/components/Select/AutoCompleteSelect';

import {
    useGetAutoCompleteMultipleResultsLazyQuery,
    useGetSearchResultsForMultipleQuery,
} from '@graphql/search.generated';
import { AndFilterInput, Entity, EntityType } from '@types';

interface Props {
    entityTypes: EntityType[];
    placeholder?: string;
    searchPlaceholder?: string;
    orFilters?: AndFilterInput[];
    /** Entity to show as already selected. Callers that resolve it asynchronously may pass it late. */
    initialValue?: Entity;
    onUpdate?: (entity: Entity | undefined) => void;
}

/**
 * This component allows you to search and select entities. It will handle everything, including
 * resolving the entities to their display name when required.
 *
 * Version 2 uses the component library, has different styling of entities, and only supports single selection.
 */
export const EntitySearchInputV2 = ({
    entityTypes,
    placeholder,
    searchPlaceholder,
    orFilters,
    initialValue,
    onUpdate,
}: Props) => {
    const { t } = useTranslation('entity.shared.selectors');
    // Suggestions when user hasn't provided a search query
    const { data: searchResults } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: entityTypes,
                query: '*',
                start: 0,
                count: 10,
                orFilters,
                searchFlags: { skipCache: true }, // To support chain linking
            },
        },
    });

    const [autoComplete, { data: autoCompleteResults, error: autoCompleteError }] =
        useGetAutoCompleteMultipleResultsLazyQuery();

    const emptySuggestions = searchResults?.searchAcrossEntities?.searchResults?.map((result) => ({
        value: result.entity.urn,
        data: result.entity,
    }));
    const [searchQuery, setSearchQuery] = useState<string>('');

    // The response echoes the query it answered, and the request behind it is debounced, so results
    // for an earlier keystroke are dropped rather than offered for the string typed since. The echo
    // comes back escaped for Elasticsearch (ResolverUtils.escapeForwardSlash on the GMS side), so a
    // query carrying a slash is compared unescaped.
    const answeredQuery = autoCompleteResults?.autoCompleteForMultiple?.query?.replaceAll('\\/', '/');
    const autoCompleteForQuery =
        answeredQuery === searchQuery ? autoCompleteResults?.autoCompleteForMultiple : undefined;
    // A failed request offers nothing rather than the previous query's hits, and stops waiting.
    const autoCompleteSuggestions = autoCompleteForQuery
        ? autoCompleteForQuery.suggestions
              .flatMap((suggestion) => suggestion.entities)
              .map((entity) => ({ value: entity.urn, data: entity }))
        : (autoCompleteError && []) || undefined;
    const isAwaitingSuggestions = !!searchQuery && !autoCompleteForQuery && !autoCompleteError;
    useDebounce(
        () => {
            if (searchQuery) {
                autoComplete({
                    variables: {
                        input: {
                            types: entityTypes,
                            query: searchQuery,
                            limit: 10,
                            orFilters,
                        },
                    },
                });
            }
        },
        100,
        [searchQuery],
    );

    return (
        <AutoCompleteSelect<Entity>
            initialValue={initialValue ? { value: initialValue.urn, data: initialValue } : undefined}
            emptySuggestions={emptySuggestions}
            autoCompleteSuggestions={autoCompleteSuggestions}
            render={(entity) => <EntitySearchInputResultV2 entity={entity} />}
            placeholder={placeholder}
            searchPlaceholder={searchPlaceholder || t('entitySearch.placeholder')}
            onSearch={setSearchQuery}
            isLoading={isAwaitingSuggestions}
            onUpdate={onUpdate}
            width="full"
            data-testid="entity-search-input-v2"
        />
    );
};
