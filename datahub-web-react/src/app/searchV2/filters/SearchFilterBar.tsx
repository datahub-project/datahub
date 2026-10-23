import { FilterBar } from '@components';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import { FilterGroup, FilterRule } from '@components/components/FilterBar/types';

import { useUserContext } from '@app/context/useUserContext';
import SaveViewButton from '@app/searchV2/filters/SaveViewButton';
import {
    activeFiltersToFilterGroup,
    buildFilterBarFields,
    filterGroupToActiveFilters,
} from '@app/searchV2/filters/SearchFilterBar.utils';
import SearchFiltersLoadingSection from '@app/searchV2/filters/SearchFiltersLoadingSection';
import { BROWSE_PATH_V2_FILTER_NAME, UnionType } from '@app/searchV2/utils/constants';
import useDebouncedCallback from '@app/shared/hooks/useDebouncedCallback';
import { useIsContextDocumentsEnabled } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { FacetFilterInput, FacetMetadata } from '@types';

/** Soften multi-checkbox Apply Filter → URL/search updates (chips stay optimistic/instant). */
const FILTER_SEARCH_DEBOUNCE_MS = 200;

/**
 * Owned by the Navigate sidebar, not FilterBar chips. Preserve on chip edits so adding a Tag
 * doesn't wipe browse selection; drop on Clear all so Navigate deselects with the chips.
 */
const SIDEBAR_OWNED_FILTER_FIELDS = new Set([BROWSE_PATH_V2_FILTER_NAME]);

const Wrapper = styled.div`
    display: flex;
    flex: 1;
    min-width: 0;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
`;

const FilterBarArea = styled.div`
    flex: 1;
    min-width: 0;
`;

const Actions = styled.div`
    display: flex;
    flex-shrink: 0;
    align-items: center;
    gap: 8px;
`;

function areFiltersEqual(left: FacetFilterInput[], right: FacetFilterInput[]): boolean {
    if (left.length !== right.length) return false;
    const serialize = (filter: FacetFilterInput) =>
        `${filter.field}|${filter.condition || ''}|${!!filter.negated}|${(filter.values || []).slice().sort().join(',')}`;
    const rightKeys = new Set(right.map(serialize));
    return left.every((filter) => rightKeys.has(serialize(filter)));
}

interface Props {
    loading: boolean;
    availableFilters: FacetMetadata[];
    activeFilters: FacetFilterInput[];
    unionType: UnionType;
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    onChangeUnionType: (unionType: UnionType) => void;
}

export default function SearchFilterBar({
    loading,
    availableFilters,
    activeFilters,
    unionType,
    onChangeFilters,
    onChangeUnionType,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const isContextDocumentsEnabled = useIsContextDocumentsEnabled();
    const userContext = useUserContext();
    const selectedViewUrn = userContext?.localState?.selectedViewUrn;
    const showSaveViewButton = activeFilters?.length > 0 && selectedViewUrn === undefined;

    const [draftRules, setDraftRules] = useState<FilterRule[]>([]);
    // Keep chip order stable — URL rebuilds applied-first and would reshuffle drafts.
    const [optimisticGroup, setOptimisticGroup] = useState<FilterGroup | null>(null);
    const pendingFiltersRef = useRef<FacetFilterInput[] | null>(null);

    const debouncedApplyFilters = useDebouncedCallback((filters: FacetFilterInput[]) => {
        onChangeFilters(filters);
    }, FILTER_SEARCH_DEBOUNCE_MS);

    // Flush the last pending search update on unmount so a final checkbox isn't dropped.
    useEffect(() => () => debouncedApplyFilters.flush(), [debouncedApplyFilters]);

    const fields = useMemo(
        () =>
            buildFilterBarFields(availableFilters, activeFilters, entityRegistry, {
                isContextDocumentsEnabled,
            }),
        [availableFilters, activeFilters, entityRegistry, isContextDocumentsEnabled],
    );

    const urlDerivedGroup = useMemo(
        () => activeFiltersToFilterGroup(activeFilters, unionType, availableFilters, draftRules),
        [activeFilters, unionType, availableFilters, draftRules],
    );

    const value = optimisticGroup ?? urlDerivedGroup;

    // Keep optimistic chips after URL sync so order doesn't jump; only drop on external URL changes.
    useEffect(() => {
        if (!optimisticGroup) return;

        if (pendingFiltersRef.current) {
            if (areFiltersEqual(pendingFiltersRef.current, activeFilters)) {
                pendingFiltersRef.current = null;
                setDraftRules(filterGroupToActiveFilters(optimisticGroup, availableFilters).draftRules);
            }
            // Still waiting for our navigateToSearchUrl to land — keep optimistic order.
            return;
        }

        const mapped = filterGroupToActiveFilters(optimisticGroup, availableFilters);
        const sidebarFilters = activeFilters.filter((filter) => SIDEBAR_OWNED_FILTER_FIELDS.has(filter.field));
        const expectedFilters = [...sidebarFilters, ...mapped.filters];
        if (!areFiltersEqual(expectedFilters, activeFilters) || mapped.unionType !== unionType) {
            // URL changed from outside this bar (back/forward, browse, etc.).
            setOptimisticGroup(null);
        }
    }, [optimisticGroup, activeFilters, unionType, availableFilters]);

    const onChange = (next: FilterGroup) => {
        const mapped = filterGroupToActiveFilters(next, availableFilters);
        const isClearAll = next.filters.length === 0 && !next.groups?.length;
        // Chip edits keep Navigate (browsePathV2); Clear all removes it too.
        const sidebarFilters = isClearAll
            ? []
            : activeFilters.filter((filter) => SIDEBAR_OWNED_FILTER_FIELDS.has(filter.field));
        const nextFilters = [...sidebarFilters, ...mapped.filters];

        setOptimisticGroup(next);
        setDraftRules(mapped.draftRules);

        const filtersChanged = !areFiltersEqual(nextFilters, activeFilters);

        if (mapped.unionType !== unionType) {
            onChangeUnionType(mapped.unionType);
        }
        if (filtersChanged) {
            pendingFiltersRef.current = nextFilters;
            debouncedApplyFilters(nextFilters);
        }
    };

    if (loading && !availableFilters.length) {
        return <SearchFiltersLoadingSection />;
    }

    return (
        <Wrapper>
            <FilterBarArea>
                <FilterBar value={value} fields={fields} onChange={onChange} />
            </FilterBarArea>
            {showSaveViewButton && (
                <Actions>
                    <SaveViewButton activeFilters={activeFilters} unionType={unionType} />
                </Actions>
            )}
        </Wrapper>
    );
}
