import React, { useMemo } from 'react';

import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import SectionHeader from '@app/searchV2/searchBarV2/components/SectionHeader';
import { SectionOption } from '@app/searchV2/searchBarV2/types';
import {
    EntityWithMatchedFields,
    combineSiblingsInEntitiesWithMatchedFields,
} from '@app/searchV2/utils/combineSiblingsInEntitiesWithMatchedFields';
import { Loader } from '@src/alchemy-components';

export default function useSearchResultsOptions(
    entitiesWithMatchedFields: EntityWithMatchedFields[] | undefined,
    searchQuery: string,
    isLoading?: boolean,
    isInitialized?: boolean,
    shouldCombineSiblings?: boolean,
): SectionOption[] {
    return useMemo(() => {
        const hasResults = (entitiesWithMatchedFields?.length ?? 0) > 0;
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

        const combinedEntities = combineSiblingsInEntitiesWithMatchedFields(
            entitiesWithMatchedFields,
            !!shouldCombineSiblings,
        );

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
                            matchedFields={combinedEntity.matchedFields}
                        />
                    ),
                    type: combinedEntity.entity.type,
                    style: { padding: '0 8px' },
                })),
            },
        ];
    }, [shouldCombineSiblings, entitiesWithMatchedFields, searchQuery, isLoading, isInitialized]);
}
