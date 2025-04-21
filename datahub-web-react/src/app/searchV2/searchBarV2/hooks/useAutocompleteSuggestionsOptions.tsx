import React, { useMemo } from 'react';

import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import SectionHeader from '@app/searchV2/searchBarV2/components/SectionHeader';
import { combineSiblingsInAutoComplete } from '@app/searchV2/utils/combineSiblingsInAutoComplete';
import { Loader } from '@src/alchemy-components';
import { AutoCompleteResultForEntity } from '@src/types.generated';

export default function useAutocompleteSuggestionsOptions(
    suggestions: AutoCompleteResultForEntity[],
    searchQuery: string,
    isLoading?: boolean,
    isInitialized?: boolean,
    shouldCombineSiblings?: boolean,
) {
    return useMemo(() => {
        const hasSuggestions = suggestions.length > 0;
        if (!isLoading && !hasSuggestions) return [];
        if (!searchQuery) return [];

        if (!isInitialized || !hasSuggestions)
            return [
                {
                    label: <Loader size="sm" />,
                    value: 'loader',
                    disabled: true,
                },
            ];

        return [
            {
                label: <SectionHeader text="Best Matches" />,
                options: suggestions
                    .map((suggestion: AutoCompleteResultForEntity) => {
                        const combinedSuggestion = combineSiblingsInAutoComplete(suggestion, {
                            combineSiblings: shouldCombineSiblings,
                        });

                        return combinedSuggestion.combinedEntities.map((combinedEntity) => ({
                            value: combinedEntity.entity.urn,
                            label: (
                                <AutoCompleteEntityItem
                                    entity={combinedEntity.entity}
                                    query={searchQuery}
                                    siblings={shouldCombineSiblings ? combinedEntity.matchedEntities : undefined}
                                />
                            ),
                            type: combinedEntity.entity.type,
                            style: { padding: '0 8px' },
                        }));
                    })
                    .flat(),
            },
        ];
    }, [shouldCombineSiblings, suggestions, searchQuery, isLoading, isInitialized]);
}
