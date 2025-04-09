import React, { useMemo } from 'react';

import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import SectionHeader from '@app/searchV2/searchBarV2/components/SectionHeader';
import { combineSiblingsInEntities } from '@app/searchV2/utils/combineSiblingsInEntities';
import { Loader } from '@src/alchemy-components';
import { Entity } from '@src/types.generated';

export default function useSearchResultsOptions(
    entities: Entity[] | undefined,
    searchQuery: string,
    isLoading?: boolean,
    isInitialized?: boolean,
    shouldCombineSiblings?: boolean,
) {
    return useMemo(() => {
        const hasResults = (entities?.length ?? 0) > 0;
        if (!isLoading && !hasResults) return [];
        if (!searchQuery) return [];

        if (!isInitialized || !hasResults)
            return [
                {
                    label: <Loader size="sm" />,
                    value: 'loader',
                    disabled: true,
                },
            ];

        const combinedEntities = combineSiblingsInEntities(entities, !!shouldCombineSiblings);

        return [
            {
                label: <SectionHeader text="Best Matches" />,
                options: combinedEntities.map((combinedEntity) => ({
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
                })),
            },
        ];
    }, [shouldCombineSiblings, entities, searchQuery, isLoading, isInitialized]);
}
