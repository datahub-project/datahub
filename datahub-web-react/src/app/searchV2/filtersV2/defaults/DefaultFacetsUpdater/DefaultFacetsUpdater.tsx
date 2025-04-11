import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useSearchFiltersContext } from '../../context';
import { FeildFacetState, FieldName, FieldToFacetStateMap } from '../../types';
import { FacetsUpdater } from './FacetsUpdater';

interface DynamicFacetsUpdaterProps {
    fieldNames: FieldName[];
    onFieldFacetsUpdated?: (fieldToFacetStateMap: FieldToFacetStateMap) => void;
    query: string;
}

export default ({ fieldNames, query, onFieldFacetsUpdated }: DynamicFacetsUpdaterProps) => {
    const { fieldToAppliedFiltersMap } = useSearchFiltersContext();

    const [fieldFacets, setFieldFacets] = useState<FieldToFacetStateMap>(new Map());

    const onFacetsUpdated = useCallback((facets: Map<FieldName, FeildFacetState>) => {
        setFieldFacets((currentFieldFacets) => new Map([...currentFieldFacets, ...facets]));
    }, []);

    useEffect(() => onFieldFacetsUpdated?.(fieldFacets), [onFieldFacetsUpdated, fieldFacets]);

    const fieldNamesWithAppliedFilters = useMemo(
        () =>
            Array.from(fieldToAppliedFiltersMap?.entries?.() || [])
                .filter(([_, filter]) => filter.filters.length > 0)
                .map(([fieldName, _]) => fieldName),
        [fieldToAppliedFiltersMap],
    );

    const fieldNamesWithoutAppliedFilters = useMemo(
        () =>
            fieldNames.filter(fieldName => !fieldNamesWithAppliedFilters.includes(fieldName)),
        [fieldNames, fieldNamesWithAppliedFilters],
    );

    return (
        <>
            {fieldNamesWithAppliedFilters.map((fieldName) => (
                <FacetsUpdater
                    fieldNames={fieldName}
                    key={fieldName}
                    query={query}
                    onFacetsUpdated={onFacetsUpdated}
                />
            ))}

            <FacetsUpdater
                fieldNames={fieldNamesWithoutAppliedFilters}
                query={query}
                onFacetsUpdated={onFacetsUpdated}
            />
        </>
    );
};
