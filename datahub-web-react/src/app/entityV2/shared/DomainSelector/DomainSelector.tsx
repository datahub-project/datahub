import React, { useEffect, useMemo, useState } from 'react';

import {
    buildEntityCache,
    entitiesToNestedSelectOptions,
    isEntityResolutionRequired,
    mergeSelectedNestedOptions,
} from '@app/entityV2/shared/utils/selectorUtils';
import { InfiniteScrollNestedSelect } from './InfiniteScrollNestedSelect';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useListDomainsLazyQuery } from '@src/graphql/domain.generated';
import { useGetEntitiesLazyQuery } from '@src/graphql/entity.generated';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '@src/graphql/search.generated';
import { Entity, EntityType } from '@src/types.generated';
import useInfiniteScrollDomains from './useInfiniteScrollDomains';

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
 * Now includes infinite scroll support for large domain hierarchies
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
        skip: useSearch // Skip infinite scroll when in search mode
    });

    // Convert selected domain URNs to NestedSelectOption format using utility
    // Use useMemo to prevent unnecessary recalculations and ensure NestedSelect properly syncs
    const initialOptions = useMemo(() => {
        return entitiesToNestedSelectOptions(selectedDomains, entityCache, entityRegistry);
    }, [selectedDomains, entityCache, entityRegistry]);

    const [childOptions, setChildOptions] = useState<NestedSelectOption[]>([]);

    const [listDomains] = useListDomainsLazyQuery({
        onCompleted: (listDomainsData) => {
            const childOptionsToAdd: NestedSelectOption[] = [];
            listDomainsData.listDomains?.domains?.forEach((domain) => {
                const { urn, type } = domain;
                childOptionsToAdd.push({
                    value: urn,
                    label: entityRegistry.getDisplayName(type, domain),
                    isParent: !!domain.children?.total,
                    parentValue: domain.parentDomains?.domains?.[0]?.urn,
                    entity: domain,
                });
            });
            setChildOptions((existingOptions) => [...existingOptions, ...childOptionsToAdd]);
        },
    });

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
        listDomains({ variables: { input: { start: 0, count: 1000, parentDomain: option.value } } });
    }

    function handleSearch(query: string) {
        if (query) {
            autoComplete({ variables: { input: { query, types: [EntityType.Domain] } } });
            setUseSearch(true);
        } else {
            setUseSearch(false);
        }
    }

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
            loadingMessage="Loading more domains..."
        />
    );
};

export default DomainSelector;
