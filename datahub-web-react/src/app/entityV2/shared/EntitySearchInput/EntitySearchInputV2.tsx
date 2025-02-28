import AutoCompleteSelect from '@src/alchemy-components/components/Select/AutoCompleteSelect';
import EntitySearchInputResultV2 from '@app/entityV2/shared/EntitySearchInput/EntitySearchInputResultV2';
import React, { useState } from 'react';
import { useDebounce } from 'react-use';
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
    onUpdate?: (entity: Entity | undefined) => void;
}

/**
 * This component allows you to search and select entities. It will handle everything, including
 * resolving the entities to their display name when required.
 *
 * Version 2 uses the component library, has different styling of entities, and only supports single selection.
 */
export const EntitySearchInputV2 = ({ entityTypes, placeholder, searchPlaceholder, orFilters, onUpdate }: Props) => {
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

    const [autoComplete, { data: autoCompleteResults }] = useGetAutoCompleteMultipleResultsLazyQuery();

    const emptySuggestions = searchResults?.searchAcrossEntities?.searchResults?.map((result) => ({
        value: result.entity.urn,
        data: result.entity,
    }));
    const autoCompleteSuggestions = autoCompleteResults?.autoCompleteForMultiple?.suggestions
        .flatMap((suggestion) => suggestion.entities)
        .map((entity) => ({ value: entity.urn, data: entity }));

    const [searchQuery, setSearchQuery] = useState<string>('');
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
            emptySuggestions={emptySuggestions}
            autoCompleteSuggestions={autoCompleteSuggestions}
            render={(entity) => <EntitySearchInputResultV2 entity={entity} />}
            placeholder={placeholder}
            searchPlaceholder={searchPlaceholder || 'Search for entities...'}
            onSearch={setSearchQuery}
            onUpdate={onUpdate}
            width="full"
            data-testid="entity-search-input-v2"
        />
    );
};
