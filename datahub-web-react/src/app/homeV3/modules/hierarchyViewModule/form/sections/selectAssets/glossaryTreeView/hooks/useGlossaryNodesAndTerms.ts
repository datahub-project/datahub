import { useMemo } from 'react';

import { isGlossaryNode } from '@app/entityV2/glossaryNode/utils';
import { isGlossaryTerm } from '@app/entityV2/glossaryTerm/utils';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';
import { ENTITY_INDEX_FILTER_NAME } from '@app/searchV2/utils/constants';

import { useScrollAcrossEntitiesQuery } from '@graphql/search.generated';
import { AndFilterInput, EntityType, FacetFilterInput, SortOrder } from '@types';

interface Props {
    parentGlossaryNodeUrn?: string;
    glossaryNodesAndTermsUrns?: string[];
}

export default function useGlossaryNodesAndTerms({ parentGlossaryNodeUrn, glossaryNodesAndTermsUrns }: Props) {
    const filters: AndFilterInput[] | undefined = useMemo(() => {
        const andFilters: FacetFilterInput[] = [];
        if (parentGlossaryNodeUrn) {
            andFilters.push({
                field: 'parentNode',
                values: [parentGlossaryNodeUrn],
            });
        }

        if (glossaryNodesAndTermsUrns?.length) {
            andFilters.push({
                field: 'urn',
                values: glossaryNodesAndTermsUrns,
            });
        }

        if (!andFilters.length) {
            return undefined;
        }

        return [{ and: andFilters }];
    }, [parentGlossaryNodeUrn, glossaryNodesAndTermsUrns]);

    const { data, loading } = useScrollAcrossEntitiesQuery({
        variables: {
            input: {
                query: '*',
                types: [EntityType.GlossaryNode, EntityType.GlossaryTerm],
                orFilters: filters,
                count: 1000,
                sortInput: {
                    sortCriteria: [
                        { field: ENTITY_INDEX_FILTER_NAME, sortOrder: SortOrder.Ascending },
                        { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending },
                    ],
                },
            },
        },
        skip: !filters,
    });

    const entities = useMemo(() => {
        if (data === undefined) return undefined;

        return (data.scrollAcrossEntities?.searchResults ?? []).map((result) => result.entity);
    }, [data]);

    const glossaryNodes = useMemo(() => {
        if (filters === undefined) return [];
        if (entities === undefined) return undefined;

        return entities.filter(isGlossaryNode);
    }, [entities, filters]);

    const glossaryTerms = useMemo(() => {
        if (filters === undefined) return [];
        if (entities === undefined) return undefined;

        return entities.filter(isGlossaryTerm);
    }, [entities, filters]);

    return { data, entities, glossaryNodes, glossaryTerms, loading };
}
