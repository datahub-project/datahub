import debounce from 'lodash/debounce';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';

import DomainOptionLabel from '@app/entityV2/shared/DomainSelector/DomainOptionLabel';
import { InfiniteScrollNestedSelect } from '@app/entityV2/shared/DomainSelector/InfiniteScrollNestedSelect';
import useInfiniteScrollDomains, {
    getDomainSelectorScrollInput,
} from '@app/entityV2/shared/DomainSelector/useInfiniteScrollDomains';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import {
    buildEntityCache,
    entitiesToNestedSelectOptions,
    isEntityResolutionRequired,
    mergeSelectedNestedOptions,
} from '@app/entityV2/shared/utils/selectorUtils';
import { DEBOUNCE_SEARCH_MS } from '@app/shared/constants';
import { StyledSpinner } from '@src/alchemy-components/components/Loader/components';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { CustomOptionRenderer } from '@src/alchemy-components/components/Select/types';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetEntitiesLazyQuery } from '@src/graphql/entity.generated';
import {
    useGetAutoCompleteMultipleResultsLazyQuery,
    useScrollAcrossEntitiesLazyQuery,
} from '@src/graphql/search.generated';
import { Domain, EntityType } from '@src/types.generated';

// `DomainSelector` builds its option list from several independent sources (root domains, loaded
// children, autocomplete search results, and hydrated selected urns) — whichever one a domain's
// option comes from, apply the same icon so it doesn't matter which list "wins" a merge.
function withDomainIcons(options: NestedSelectOption[]): NestedSelectOption[] {
    return options.map((option) =>
        option.entity
            ? { ...option, icon: <DomainColoredIcon domain={option.entity as Domain} size={14} fontSize={8} /> }
            : option,
    );
}

type DomainSelectorProps = {
    selectedDomains: string[];
    onDomainsChange: (domainUrns: string[], selectedOptions?: NestedSelectOption[]) => void;
    placeholder?: string;
    label?: string;
    isMultiSelect?: boolean;
    selectChildrenWithParent?: boolean;
    renderCustomOptionText?: CustomOptionRenderer<NestedSelectOption>;
    renderCustomSelectedValue?: (option: NestedSelectOption) => React.ReactNode;
    isRequired?: boolean;
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
    placeholder,
    label,
    isMultiSelect = false,
    selectChildrenWithParent = true,
    renderCustomOptionText = (option) => <DomainOptionLabel option={option} />,
    renderCustomSelectedValue,
    isRequired = false,
}) => {
    const { t } = useTranslation(['entity.shared.selectors', 'common.feedback']);
    const resolvedPlaceholder =
        placeholder ?? t(isMultiSelect ? 'domainSelector.placeholder' : 'domainSelector.singlePlaceholder');
    const resolvedLabel = label ?? t(isMultiSelect ? 'domainSelector.label' : 'domainSelector.singleLabel');
    const entityRegistry = useEntityRegistryV2();
    const [useSearch, setUseSearch] = useState(false);
    const attemptedUrnsRef = useRef<Set<string>>(new Set());

    // Entity hydration for selected domains
    const [getEntities, { data: resolvedEntitiesData, loading: entitiesLoading }] = useGetEntitiesLazyQuery();

    // Derived directly from `resolvedEntitiesData` (not useState+useEffect) so the cache updates
    // in the same render `entitiesLoading` flips to false — an effect-based copy lags one render
    // behind, long enough to flash the raw urn before the follow-up render fills in the real name.
    const entityCache = useMemo(() => {
        return buildEntityCache(resolvedEntitiesData?.entities);
    }, [resolvedEntitiesData]);

    // Bootstrap by resolving all URNs that are not in the cache yet
    useEffect(() => {
        if (selectedDomains.length === 0) {
            return;
        }
        const attemptedUrns = attemptedUrnsRef.current;
        if (!isEntityResolutionRequired(selectedDomains, entityCache, attemptedUrns)) {
            return;
        }
        selectedDomains.forEach((urn) => attemptedUrns.add(urn));
        getEntities({ variables: { urns: selectedDomains } });
    }, [selectedDomains, entityCache, getEntities]);

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

    // Placeholder shown for a selected domain whose entity hasn't been hydrated yet,
    // so we don't flash the raw urn while `getEntities` is in flight.
    const loadingPlaceholder = useMemo(
        () =>
            entitiesLoading ? { label: t('common.feedback:loading'), icon: <StyledSpinner $height={12} /> } : undefined,
        [entitiesLoading, t],
    );

    // Convert selected domain URNs to NestedSelectOption format using utility
    // Use useMemo to prevent unnecessary recalculations and ensure NestedSelect properly syncs
    const initialOptions = useMemo(() => {
        return withDomainIcons(
            entitiesToNestedSelectOptions(selectedDomains, entityCache, entityRegistry, loadingPlaceholder),
        );
    }, [selectedDomains, entityCache, entityRegistry, loadingPlaceholder]);

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
            onDomainsChange(domainUrnsToUpdate, values);
        } else {
            onDomainsChange([], []);
        }
    }

    // Merge options to ensure selected domains remain visible
    const baseOptions = withDomainIcons([...rootDomains, ...childOptions]).sort((a, b) =>
        a.label.localeCompare(b.label),
    );
    const searchOptions = withDomainIcons(autoCompleteOptions).sort((a, b) => a.label.localeCompare(b.label));

    const defaultOptions = mergeSelectedNestedOptions(baseOptions, initialOptions);
    const searchOptionsWithSelected = mergeSelectedNestedOptions(searchOptions, initialOptions);

    return (
        <InfiniteScrollNestedSelect
            label={resolvedLabel}
            placeholder={resolvedPlaceholder}
            searchPlaceholder={t('domainSelector.searchPlaceholder')}
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
            isRequired={isRequired}
            showSearch
            implicitlySelectChildren={false}
            selectChildrenWithParent={selectChildrenWithParent}
            areParentsSelectable
            shouldAlwaysSyncParentValues
            hideParentCheckbox={false}
            renderCustomOptionText={renderCustomOptionText}
            renderCustomSelectedValue={renderCustomSelectedValue}
            selectLabelProps={renderCustomSelectedValue ? { variant: 'custom' } : undefined}
            showClear
        />
    );
};

export default DomainSelector;
