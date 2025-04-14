import { Loader } from '@src/alchemy-components';
import { AutoCompleteResultForEntity } from '@src/types.generated';
import React, { useMemo } from 'react';
import SectionHeader from '../components/SectionHeader';
import { combineSiblingsInAutoComplete } from '../../utils/combineSiblingsInAutoComplete';
import AutoCompleteEntityItem from '../../autoCompleteV2/AutoCompleteEntityItem';

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
