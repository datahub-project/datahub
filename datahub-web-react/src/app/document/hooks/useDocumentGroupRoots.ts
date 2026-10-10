import { useMemo } from 'react';

import {
    DocumentDomainGroup,
    buildDocumentDomainGroups,
    getDocumentDomainGroupField,
    sumFacetAggregationCounts,
} from '@app/document/utils/documentSidebarGrouping';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useAggregateAcrossEntitiesQuery } from '@graphql/search.generated';
import { EntityType, FilterOperator } from '@types';

const GROUP_FACET_MAX = 100;
const ENTITY_TYPE_FACET = '_entityType';

/**
 * Domain group headers for the documents sidebar — facet aggregations over Document,
 * plus an Unassigned bucket when any document has no domain.
 */
export default function useDocumentGroupRoots(
    unassignedLabel: string,
    activeGroup?: DocumentDomainGroup,
    viewUrn?: string | null,
    skip?: boolean,
): { groups: DocumentDomainGroup[]; loading: boolean } {
    const entityRegistry = useEntityRegistry();
    const field = getDocumentDomainGroupField();

    const { data, previousData, loading } = useAggregateAcrossEntitiesQuery({
        skip,
        fetchPolicy: 'network-only',
        variables: {
            input: {
                types: [EntityType.Document],
                query: '*',
                facets: [field],
                viewUrn: viewUrn ?? undefined,
                searchFlags: { maxAggValues: GROUP_FACET_MAX },
            },
        },
    });

    const {
        data: unassignedData,
        previousData: previousUnassignedData,
        loading: unassignedLoading,
    } = useAggregateAcrossEntitiesQuery({
        skip,
        fetchPolicy: 'network-only',
        variables: {
            input: {
                types: [EntityType.Document],
                query: '*',
                facets: [ENTITY_TYPE_FACET],
                viewUrn: viewUrn ?? undefined,
                orFilters: [
                    {
                        and: [
                            {
                                field,
                                condition: FilterOperator.Exists,
                                negated: true,
                            },
                        ],
                    },
                ],
            },
        },
    });

    const groups = useMemo(() => {
        const resolvedData = data ?? previousData;
        const facet = (resolvedData?.aggregateAcrossEntities?.facets ?? []).find((item) => item.field === field);
        const resolvedUnassignedData = unassignedData ?? previousUnassignedData;
        const unassignedFacet = (resolvedUnassignedData?.aggregateAcrossEntities?.facets ?? []).find(
            (item) => item.field === ENTITY_TYPE_FACET,
        );

        return buildDocumentDomainGroups({
            aggregations: facet?.aggregations ?? [],
            unassignedCount: sumFacetAggregationCounts(unassignedFacet),
            unassignedLabel,
            activeGroup,
            getDisplayName: (entity) => entityRegistry.getDisplayName(entity.type, entity),
        });
    }, [
        activeGroup,
        data,
        entityRegistry,
        field,
        previousData,
        previousUnassignedData,
        unassignedData,
        unassignedLabel,
    ]);

    return {
        groups,
        loading: skip ? false : loading || unassignedLoading,
    };
}
