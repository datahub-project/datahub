import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { ENTITY_SUB_TYPE_FILTER_NAME } from '../../../utils/constants';
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

    const onFacetsUpdated = useCallback(
        (facets: Map<FieldName, FeildFacetState>) => {
            setFieldFacets((currentFieldFacets) => new Map([...currentFieldFacets, ...facets]));
        },
        [setFieldFacets],
    );

    useEffect(() => onFieldFacetsUpdated?.(fieldFacets), [onFieldFacetsUpdated, fieldFacets]);

    const hasFiltersByEntityType = useMemo(
        () =>
            Array.from(fieldToAppliedFiltersMap.entries()).some(
                ([key, filter]) => key === ENTITY_SUB_TYPE_FILTER_NAME && filter.filters.length > 0,
            ),
        [fieldToAppliedFiltersMap],
    );

    const hasAnotherFilters = useMemo(
        () =>
            Array.from(fieldToAppliedFiltersMap.entries()).some(
                ([key, filter]) => key !== ENTITY_SUB_TYPE_FILTER_NAME && filter.filters.length > 0,
            ),
        [fieldToAppliedFiltersMap],
    );

    return (
        <>
            {/* In a case when there're no any filters applied we can use a single request for all fields */}
            {!hasFiltersByEntityType && !hasAnotherFilters && (
                <FacetsUpdater fieldNames={fieldNames} query={query} onFacetsUpdated={onFacetsUpdated} />
            )}

            {/* In a case when there're only filters by entity type we have to call requests for entity types and another fields separately */}
            {hasFiltersByEntityType && !hasAnotherFilters && (
                <>
                    <FacetsUpdater
                        fieldNames={fieldNames.filter((fieldName) => fieldName === ENTITY_SUB_TYPE_FILTER_NAME)}
                        query={query}
                        onFacetsUpdated={onFacetsUpdated}
                    />
                    <FacetsUpdater
                        fieldNames={fieldNames.filter((fieldName) => fieldName !== ENTITY_SUB_TYPE_FILTER_NAME)}
                        query={query}
                        onFacetsUpdated={onFacetsUpdated}
                    />
                </>
            )}

            {/* In a case when there're both filters by entity type and by non-entity-type fields we have to call separated requests for each field */}
            {hasAnotherFilters &&
                fieldNames.map((fieldName) => (
                    <FacetsUpdater
                        key={fieldName}
                        fieldNames={[fieldName]}
                        query={query}
                        onFacetsUpdated={onFacetsUpdated}
                    />
                ))}
        </>
    );
};
