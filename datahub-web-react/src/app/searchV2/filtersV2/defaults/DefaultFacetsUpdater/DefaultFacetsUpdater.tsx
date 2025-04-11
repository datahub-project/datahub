import React, { useMemo } from 'react';
import { useSearchFiltersContext } from '../../context';
import { FieldName, FieldToFacetStateMap } from '../../types';
import { FacetsUpdater } from './FacetsUpdater';

interface DynamicFacetsUpdaterProps {
    fieldNames: FieldName[];
    onFieldFacetsUpdated: (fieldToFacetStateMap: FieldToFacetStateMap) => void;
    query: string;
}

export default ({ fieldNames, query, onFieldFacetsUpdated }: DynamicFacetsUpdaterProps) => {
    const { fieldToAppliedFiltersMap } = useSearchFiltersContext();

    const fieldNamesWithAppliedFilters = useMemo(
        () =>
            Array.from(fieldToAppliedFiltersMap?.entries?.() || [])
                .filter(([_, filter]) => filter.filters.length > 0)
                .map(([fieldName, _]) => fieldName),
        [fieldToAppliedFiltersMap],
    );

    const fieldNamesWithoutAppliedFilters = useMemo(
        () => fieldNames.filter((fieldName) => !fieldNamesWithAppliedFilters.includes(fieldName)),
        [fieldNames, fieldNamesWithAppliedFilters],
    );

    return (
        <>
            {fieldNamesWithAppliedFilters.map((fieldName) => (
                <FacetsUpdater
                    fieldNames={fieldName}
                    key={fieldName}
                    query={query}
                    onFacetsUpdated={onFieldFacetsUpdated}
                />
            ))}

            <FacetsUpdater
                fieldNames={fieldNamesWithoutAppliedFilters}
                query={query}
                onFacetsUpdated={onFieldFacetsUpdated}
            />
        </>
    );
};
