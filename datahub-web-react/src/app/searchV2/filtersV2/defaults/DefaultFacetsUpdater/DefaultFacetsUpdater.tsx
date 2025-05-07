import React, { useMemo } from 'react';

import { useSearchFiltersContext } from '@app/searchV2/filtersV2/context';
import { FacetsUpdater } from '@app/searchV2/filtersV2/defaults/DefaultFacetsUpdater/FacetsUpdater';
import { FieldName, FieldToFacetStateMap } from '@app/searchV2/filtersV2/types';

interface DynamicFacetsUpdaterProps {
    fieldNames: FieldName[];
    onFieldFacetsUpdated: (fieldToFacetStateMap: FieldToFacetStateMap) => void;
    query: string;
    viewUrn?: string | null;
    shouldUpdateFacetsForFieldsWithAppliedFilters?: boolean;
    shouldUpdateFacetsForFieldsWithoutAppliedFilters?: boolean;
}

export default ({
    fieldNames,
    query,
    viewUrn,
    onFieldFacetsUpdated,
    shouldUpdateFacetsForFieldsWithAppliedFilters,
    shouldUpdateFacetsForFieldsWithoutAppliedFilters,
}: DynamicFacetsUpdaterProps) => {
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
            {shouldUpdateFacetsForFieldsWithAppliedFilters &&
                fieldNamesWithAppliedFilters.map((fieldName) => (
                    <FacetsUpdater
                        fieldNames={fieldName}
                        key={fieldName}
                        query={query}
                        viewUrn={viewUrn}
                        onFacetsUpdated={onFieldFacetsUpdated}
                    />
                ))}

            {shouldUpdateFacetsForFieldsWithoutAppliedFilters && (
                <FacetsUpdater
                    fieldNames={fieldNamesWithoutAppliedFilters}
                    query={query}
                    viewUrn={viewUrn}
                    onFacetsUpdated={onFieldFacetsUpdated}
                />
            )}
        </>
    );
};
