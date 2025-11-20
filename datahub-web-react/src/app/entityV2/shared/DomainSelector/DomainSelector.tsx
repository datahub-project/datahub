import { debounce } from 'lodash';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { InfiniteScrollNestedSelect } from '@app/entityV2/shared/DomainSelector/InfiniteScrollNestedSelect';
import useInfiniteScrollDomains, {
    getDomainSelectorScrollInput,
} from '@app/entityV2/shared/DomainSelector/useInfiniteScrollDomains';
import {
    buildEntityCache,
    entitiesToNestedSelectOptions,
    isEntityResolutionRequired,
    mergeSelectedNestedOptions,
} from '@app/entityV2/shared/utils/selectorUtils';
import { DEBOUNCE_SEARCH_MS } from '@app/shared/constants';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetEntitiesLazyQuery } from '@src/graphql/entity.generated';
import {
    useGetAutoCompleteMultipleResultsLazyQuery,
    useScrollAcrossEntitiesLazyQuery,
} from '@src/graphql/search.generated';
import { Entity, EntityType } from '@src/types.generated';

type DomainSelectorProps = {
    selectedDomains: string[];
    onDomainsChange: (domainUrns: string[]) => void;
    placeholder?: string;
    label?: string;
    isMultiSelect?: boolean;
};

/**
 * Standalone domain selector component that doesn't rely on Ant Design form state
 * Supports both single and multiple domain selection based on isMultiSelect prop
 * Works with URN strings instead of Domain objects for simpler integration
 *
 * Features:
 * - Infinite scroll support at root level for domains without parents
 * - Paginated loading of nested children using scrollAcrossEntities (no 1000 limit)
 * - Debounced search with autocomplete
 * - Entity caching for selected domains
 */
const DomainSelector: React.FC<DomainSelectorProps> = ({
    selectedDomains,
    onDomainsChange,
    placeholder = 'Select domains',
    label = 'Domains',
    isMultiSelect = false,
}) => {
    const entityRegistry = useEntityRegistryV2();
    const [useSearch, setUseSearch] = useState(false);
    const [entityCache, setEntityCache] = useState<Map<string, Entity>>(new Map());

    // Entity hydration for selected domains
    const [getEntities, { data: resolvedEntitiesData }] = useGetEntitiesLazyQuery();

    // Bootstrap by resolving all URNs that are not in the cache yet
    useEffect(() => {
        if (selectedDomains.length > 0 && isEntityResolutionRequired(selectedDomains, entityCache)) {
            getEntities({ variables: { urns: selectedDomains } });
        }
    }, [selectedDomains, entityCache, getEntities]);

    // Build cache from resolved entities
    useEffect(() => {
        if (resolvedEntitiesData && resolvedEntitiesData.entities?.length) {
            // Should be a safe cast since we're only querying for entities
            const entities: Entity[] = (resolvedEntitiesData?.entities as Entity[]) || [];
            setEntityCache(buildEntityCache(entities));
        }
    }, [resolvedEntitiesData]);

    const [autoComplete, { data: autoCompleteData }] = useGetAutoCompleteMultipleResultsLazyQuery();

    // Infinite scroll for root level domains (domains without parents)
    const {
        domains: rootDomains,
        loading: rootDomainsLoading,
        hasMoreDomains,
        scrollRef,
    } = useInfiniteScrollDomains({
        skip: useSearch, // Skip infinite scroll when in search mode
    });

    // Convert selected domain URNs to NestedSelectOption format using utility
    // Use useMemo to prevent unnecessary recalculations and ensure NestedSelect properly syncs
    const initialOptions = useMemo(() => {
        return entitiesToNestedSelectOptions(selectedDomains, entityCache, entityRegistry);
    }, [selectedDomains, entityCache, entityRegistry]);

    const [childOptions, setChildOptions] = useState<NestedSelectOption[]>([]);
    const [loadedChildUrns, setLoadedChildUrns] = useState<Set<string>>(new Set());

    // Track scroll state per parent domain for nested infinite scroll
    const scrollStateRef = useRef<Map<string, { scrollId: string | null; isComplete: boolean }>>(new Map());

    const [scrollNestedDomains, { data: nestedScrollData }] = useScrollAcrossEntitiesLazyQuery({
        fetchPolicy: 'cache-and-network',
        notifyOnNetworkStatusChange: true,
    });

    // Process nested scroll results
    useEffect(() => {
        if (nestedScrollData?.scrollAcrossEntities?.searchResults) {
            const { searchResults: results, nextScrollId } = nestedScrollData.scrollAcrossEntities;
            const childOptionsToAdd: NestedSelectOption[] = [];

            results.forEach((result) => {
                const domain = result.entity;
                // Only add if we haven't loaded this URN yet and it's a domain
                if (domain.type === EntityType.Domain && !loadedChildUrns.has(domain.urn)) {
                    childOptionsToAdd.push({
                        value: domain.urn,
                        label: entityRegistry.getDisplayName(domain.type, domain),
                        isParent: !!(domain as any)?.children?.total,
                        parentValue: (domain as any)?.parentDomains?.domains?.[0]?.urn,
                        entity: domain,
                    });
                }
            });

            if (childOptionsToAdd.length > 0) {
                setChildOptions((existingOptions) => [...existingOptions, ...childOptionsToAdd]);
                setLoadedChildUrns((prev) => {
                    const updated = new Set(prev);
                    childOptionsToAdd.forEach((opt) => updated.add(opt.value));
                    return updated;
                });
            }

            // Check if we need to continue fetching for this parent
            if (nextScrollId && results.length > 0) {
                // Find the parent domain from the first result
                const firstResult = results[0]?.entity;
                const parentUrn = (firstResult as any)?.parentDomains?.domains?.[0]?.urn;

                if (parentUrn) {
                    // Update scroll state and continue fetching
                    scrollStateRef.current.set(parentUrn, { scrollId: nextScrollId, isComplete: false });
                    scrollNestedDomains({
                        variables: getDomainSelectorScrollInput(parentUrn, nextScrollId),
                    });
                }
            } else if (results.length > 0) {
                // Mark this parent as complete
                const firstResult = results[0]?.entity;
                const parentUrn = (firstResult as any)?.parentDomains?.domains?.[0]?.urn;
                if (parentUrn) {
                    scrollStateRef.current.set(parentUrn, { scrollId: null, isComplete: true });
                }
            }
        }
    }, [nestedScrollData, entityRegistry, loadedChildUrns, scrollNestedDomains]);

    // Root level options from infinite scroll (already in NestedSelectOption format)
    const options = rootDomains;

    const autoCompleteOptions =
        autoCompleteData?.autoCompleteForMultiple?.suggestions?.flatMap((s) =>
            s.entities.map((domain) => ({
                value: domain.urn,
                label: entityRegistry.getDisplayName(domain.type, domain),
                id: domain.urn,
                entity: domain,
            })),
        ) || [];

    function handleLoad(option: NestedSelectOption) {
        const parentUrn = option.value;

        // Check if we've already loaded this parent's children
        const scrollState = scrollStateRef.current.get(parentUrn);
        if (scrollState?.isComplete) {
            return; // Already loaded all children
        }

        // Start fetching children using scrollAcrossEntities for pagination support
        // This removes the 1000 child limit and fetches in batches
        scrollStateRef.current.set(parentUrn, { scrollId: null, isComplete: false });
        scrollNestedDomains({
            variables: getDomainSelectorScrollInput(parentUrn, null),
        });
    }

    // Debounced search handler to avoid querying on every keystroke
    // eslint-disable-next-line react-hooks/exhaustive-deps
    const handleSearch = useCallback(
        debounce((query: string) => {
            if (query) {
                autoComplete({ variables: { input: { query, types: [EntityType.Domain] } } });
                setUseSearch(true);
            } else {
                setUseSearch(false);
            }
        }, DEBOUNCE_SEARCH_MS),
        [autoComplete],
    );

    function handleUpdate(values: NestedSelectOption[]) {
        if (values.length) {
            const domainUrnsToUpdate = values.map((v) => v.value);
            onDomainsChange(domainUrnsToUpdate);
        } else {
            onDomainsChange([]);
        }
    }

    // Merge options to ensure selected domains remain visible
    const baseOptions = [...options, ...childOptions].sort((a, b) => a.label.localeCompare(b.label));
    const searchOptions = [...autoCompleteOptions].sort((a, b) => a.label.localeCompare(b.label));

    const defaultOptions = mergeSelectedNestedOptions(baseOptions, initialOptions);
    const searchOptionsWithSelected = mergeSelectedNestedOptions(searchOptions, initialOptions);

    return (
        <InfiniteScrollNestedSelect
            label={label}
            placeholder={placeholder}
            searchPlaceholder="Search all domains..."
            options={useSearch ? searchOptionsWithSelected : defaultOptions}
            initialValues={initialOptions}
            loadData={handleLoad}
            onSearch={handleSearch}
            onUpdate={handleUpdate}
            loading={rootDomainsLoading}
            hasMore={hasMoreDomains && !useSearch}
            scrollRef={scrollRef}
            width="full"
            isMultiSelect={isMultiSelect}
            showSearch
            implicitlySelectChildren={false}
            areParentsSelectable
            shouldAlwaysSyncParentValues
            hideParentCheckbox={false}
        />
    );
};

export default DomainSelector;
