import { useAggregateAcrossEntitiesLazyQuery } from '@src/graphql/search.generated';
import { FacetMetadata } from '@src/types.generated';
import { useEffect, useMemo, useState } from 'react';
import { useDebounce } from 'react-use';
import { generateOrFilters } from '@src/app/searchV2/utils/generateOrFilters';
import { UnionType } from '../../../utils/constants';
import { useSearchFiltersContext } from '../../context';
import { FieldName, FieldToFacetStateMap } from '../../types';
import { convertFiltersMapToFilters } from '../../utils';

const DEBOUNCE_MS = 100;

interface Props {
    fieldNames: FieldName[] | FieldName;
    query: string;
    onFacetsUpdated: (facets: FieldToFacetStateMap) => void;
}

export function FacetsUpdater({ fieldNames, query, onFacetsUpdated }: Props) {
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
                        },
                    },
                });
            }
        },
        DEBOUNCE_MS,
        [aggregateAcrossEntities, query, filters, wrappedFieldNames],
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
