/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect, useMemo, useState } from 'react';
import { useDebounce } from 'react-use';

import { useSearchFiltersContext } from '@app/searchV2/filtersV2/context';
import { FieldName, FieldToFacetStateMap } from '@app/searchV2/filtersV2/types';
import { convertFiltersMapToFilters } from '@app/searchV2/filtersV2/utils';
import { UnionType } from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@src/app/searchV2/utils/generateOrFilters';
import { useAggregateAcrossEntitiesLazyQuery } from '@src/graphql/search.generated';
import { FacetMetadata } from '@src/types.generated';

const DEBOUNCE_MS = 300;

interface Props {
    fieldNames: FieldName[] | FieldName;
    query: string;
    viewUrn?: string | null;
    onFacetsUpdated: (facets: FieldToFacetStateMap) => void;
}

export function FacetsUpdater({ fieldNames, query, viewUrn, onFacetsUpdated }: Props) {
    const [facets, setFacets] = useState<FacetMetadata[]>([]);
    const [isInitialized, setIsInitialized] = useState<boolean>(false);
    const { fieldToAppliedFiltersMap } = useSearchFiltersContext();

    const [aggregateAcrossEntities, { data, loading, called }] = useAggregateAcrossEntitiesLazyQuery();

    const wrappedFieldNames = useMemo(() => {
        if (Array.isArray(fieldNames)) return fieldNames;
        return [fieldNames];
    }, [fieldNames]);

    const filters = useMemo(
        () => convertFiltersMapToFilters(fieldToAppliedFiltersMap, { excludedFields: wrappedFieldNames }),
        [fieldToAppliedFiltersMap, wrappedFieldNames],
    );

    useDebounce(
        () => {
            if (wrappedFieldNames.length > 0) {
                aggregateAcrossEntities({
                    variables: {
                        input: {
                            query,
                            orFilters: generateOrFilters(UnionType.AND, filters),
                            facets: wrappedFieldNames,
                            viewUrn,
                        },
                    },
                });
            }
        },
        DEBOUNCE_MS,
        [aggregateAcrossEntities, query, viewUrn, filters, wrappedFieldNames],
    );

    useEffect(() => {
        if (called && !loading) {
            setFacets(data?.aggregateAcrossEntities?.facets ?? []);
            setIsInitialized(true);
        }
    }, [loading, data, called]);

    useEffect(() => {
        if (isInitialized) {
            onFacetsUpdated(
                new Map(
                    wrappedFieldNames.map((fieldName) => [
                        fieldName,
                        {
                            facet: facets.find((facet) => facet.field === fieldName),
                            loading,
                        },
                    ]),
                ),
            );
            setIsInitialized(false);
        }
    }, [onFacetsUpdated, facets, loading, isInitialized, wrappedFieldNames]);

    return null;
}
