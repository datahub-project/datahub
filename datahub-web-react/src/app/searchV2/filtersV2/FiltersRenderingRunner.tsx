import React, { useMemo } from 'react';
import { useSearchFiltersContext } from './context';
import { FieldName, Filter } from './types';

interface Props {
    fieldNames: FieldName[];
    hideEmptyFilters?: boolean;
}

export default function FiltersRenderingRunner({ fieldNames, hideEmptyFilters = false }: Props) {
    const {
        filtersRenderer,
        fieldToFacetStateMap,
        updateFieldAppliedFilters,
        fieldToAppliedFiltersMap,
        filtersRegistry,
    } = useSearchFiltersContext();

    const Renderer = useMemo(() => filtersRenderer, [filtersRenderer]);

    const filters: Filter[] = useMemo(() => {
        const isEmptyFilter = (fieldName: string) => {
            const hasAnyAggregations = (fieldToFacetStateMap?.get(fieldName)?.facet?.aggregations?.length ?? 0) > 0;
            const hasAnyAppliedFilters = (fieldToAppliedFiltersMap?.get(fieldName)?.filters?.length ?? 0) > 0;

            return !(hasAnyAggregations || hasAnyAppliedFilters);
        };

        return fieldNames
            .filter((fieldName) => !hideEmptyFilters || !isEmptyFilter(fieldName))
            .map((fieldName) => ({
                fieldName,
                props: {
                    fieldName,
                    appliedFilters: fieldToAppliedFiltersMap?.get(fieldName),
                    onUpdate: (values) => updateFieldAppliedFilters?.(fieldName, values),
                    facetState: fieldToFacetStateMap?.get(fieldName),
                },
                component: filtersRegistry.get(fieldName) || (() => null),
            }));
    }, [
        fieldNames,
        fieldToFacetStateMap,
        updateFieldAppliedFilters,
        fieldToAppliedFiltersMap,
        hideEmptyFilters,
        filtersRegistry,
    ]);

    return <Renderer filters={filters} />;
}
