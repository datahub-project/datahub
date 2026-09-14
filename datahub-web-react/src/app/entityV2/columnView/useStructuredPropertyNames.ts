import { useMemo } from 'react';

import {
    ColumnLike,
    isStructuredPropertyColumn,
    structuredPropertyNameOf,
    structuredPropertyUrnOf,
} from '@app/entityV2/columnView/columnKinds';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType, StructuredPropertyEntity } from '@types';

/**
 * Display names for structured-property columns that only carry a urn (columns synthesized from the
 * legacy table layout, or whose entity failed to hydrate). Hydrated columns need no lookup. One
 * cache-first search by urn; no entity-page context required, so it works in the Schema-tab
 * popover, the builder modal and Settings alike.
 */
export function useStructuredPropertyNames(columns: ColumnLike[]): Record<string, string> {
    const unresolved = useMemo(
        () =>
            Array.from(
                new Set(
                    columns
                        .filter((c) => isStructuredPropertyColumn(c) && !structuredPropertyNameOf(c))
                        .map(structuredPropertyUrnOf)
                        .filter((urn): urn is string => Boolean(urn)),
                ),
            ),
        [columns],
    );

    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.StructuredProperty],
                query: '*',
                start: 0,
                count: unresolved.length,
                orFilters: [{ and: [{ field: 'urn', values: unresolved }] }],
            },
        },
        skip: unresolved.length === 0,
        fetchPolicy: 'cache-first',
    });

    return useMemo(() => {
        const names: Record<string, string> = {};
        (data?.searchAcrossEntities?.searchResults || []).forEach((r) => {
            const entity = r.entity as StructuredPropertyEntity;
            const name = entity.definition?.displayName || entity.definition?.qualifiedName;
            if (name) names[entity.urn] = name;
        });
        return names;
    }, [data]);
}
