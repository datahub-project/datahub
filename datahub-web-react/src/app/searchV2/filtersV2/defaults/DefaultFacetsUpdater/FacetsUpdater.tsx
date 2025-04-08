import { useAggregateAcrossEntitiesLazyQuery } from '@src/graphql/search.generated';
import { EntityType, FacetMetadata } from '@src/types.generated';
import { useEffect, useMemo, useState } from 'react';
import { useDebounce } from 'react-use';
import { ENTITY_SUB_TYPE_FILTER_NAME } from '../../../utils/constants';
import { useSearchFiltersContext } from '../../context';
import { FieldName, FieldToFacetStateMap } from '../../types';

const DEBOUNCE_MS = 100;

interface Props {
    fieldNames: FieldName[];
    query: string;
    onFacetsUpdated: (facets: FieldToFacetStateMap) => void;
}

export function FacetsUpdater({ fieldNames, query, onFacetsUpdated }: Props) {
    const [facets, setFacets] = useState<FacetMetadata[]>([]);
    const [isInitialized, setIsInitialized] = useState<boolean>(false);
    const { fieldToAppliedFiltersMap } = useSearchFiltersContext();

    const [aggregateAcrossEntities, { data, loading, called }] = useAggregateAcrossEntitiesLazyQuery();

    const appliedFiltersForAnotherFields = useMemo(
        () =>
            Array.from(fieldToAppliedFiltersMap?.entries?.() || [])
                .filter(([key, _]) => !fieldNames.includes(key))
                .flatMap(([_, value]) => value.filters),

        [fieldToAppliedFiltersMap, fieldNames],
    );

    const entityTypesFromFilters = useMemo(() => {
        return appliedFiltersForAnotherFields
            .filter((filter) => filter.field === ENTITY_SUB_TYPE_FILTER_NAME)
            .flatMap((filter) => filter.values)
            .filter((value): value is EntityType => !!value);
    }, [appliedFiltersForAnotherFields]);

    const filters = useMemo(() => {
        return appliedFiltersForAnotherFields.filter((filter) => filter.field !== ENTITY_SUB_TYPE_FILTER_NAME);
    }, [appliedFiltersForAnotherFields]);

    useDebounce(
        () => {
            if (fieldNames.length > 0) {
                aggregateAcrossEntities({
                    variables: {
                        input: {
                            query,
                            types: entityTypesFromFilters,
                            orFilters: [{ and: filters }],
                            facets: fieldNames,
                        },
                    },
                });
            }
        },
        DEBOUNCE_MS,
        [aggregateAcrossEntities, query, entityTypesFromFilters, filters, fieldNames],
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
                    fieldNames.map((fieldName) => [
                        fieldName,
                        {
                            facet: facets.find((facet) => facet.field === fieldName),
                            loading,
                        },
                    ]),
                ),
            );
        }
    }, [onFacetsUpdated, facets, loading, isInitialized, fieldNames]);

    return null;
}
