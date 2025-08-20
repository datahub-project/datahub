import React, { useEffect, useState } from 'react';

import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useListDomainsLazyQuery, useListDomainsQuery } from '@src/graphql/domain.generated';
import { useGetEntitiesLazyQuery } from '@src/graphql/entity.generated';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '@src/graphql/search.generated';
import { Domain, Entity, EntityType } from '@src/types.generated';

// Utility functions for entity caching, similar to EntitySearchSelect
const buildCache = (entities: Entity[]) => {
    const cache = new Map<string, Entity>();
    entities.forEach((entity) => cache.set(entity.urn, entity));
    return cache;
};

const isResolutionRequired = (urns: string[], cache: Map<string, Entity>) => {
    const uncachedUrns = urns.filter((urn) => !cache.has(urn));
    return uncachedUrns.length > 0;
};

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
        if (selectedDomains.length > 0 && isResolutionRequired(selectedDomains, entityCache)) {
            getEntities({ variables: { urns: selectedDomains } });
        }
    }, [selectedDomains, entityCache, getEntities]);

    // Build cache from resolved entities
    useEffect(() => {
        if (resolvedEntitiesData && resolvedEntitiesData.entities?.length) {
            const entities: Entity[] = (resolvedEntitiesData?.entities as Entity[]) || [];
            setEntityCache(buildCache(entities));
        }
    }, [resolvedEntitiesData]);

    const [autoComplete, { data: autoCompleteData }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const { data } = useListDomainsQuery({
        variables: {
            input: {
                query: '*',
                count: 1000,
            },
        },
    });

    // Convert selected domain URNs to NestedSelectOption format using hydrated entities
    const initialOptions = selectedDomains.map((urn: string) => {
        const domain = entityCache.get(urn) as Domain;
        if (domain) {
            return {
                value: domain.urn,
                label: entityRegistry.getDisplayName(domain.type, domain),
                id: domain.urn,
                parentId: domain.parentDomains?.domains?.[0]?.urn,
                entity: domain,
            };
        }
        // Fallback option when entity isn't hydrated yet - prevents jitter
        // Extract domain name from URN (e.g., "urn:li:domain:engineering" -> "engineering")
        const domainName = urn.split(':').pop() || urn;
        return {
            value: urn,
            label: domainName, // Temporary label until entity is resolved
            id: urn,
            parentId: undefined,
            entity: undefined,
        };
    });

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

    const options =
        data?.listDomains?.domains?.map((domain) => ({
            value: domain.urn,
            label: entityRegistry.getDisplayName(domain.type, domain),
            id: domain.urn,
            isParent: !!domain.children?.total,
            entity: domain,
        })) || [];

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

    const defaultOptions = [...options, ...childOptions].sort((a, b) => a.label.localeCompare(b.label));

    return (
        <div style={{ width: '100%' }}>
            <NestedSelect
                label={label}
                placeholder={placeholder}
                searchPlaceholder="Search all domains..."
                options={useSearch ? autoCompleteOptions : defaultOptions}
                initialValues={initialOptions}
                loadData={handleLoad}
                onSearch={handleSearch}
                onUpdate={handleUpdate}
                width="full"
                isMultiSelect={isMultiSelect}
                showSearch
                implicitlySelectChildren={false}
                areParentsSelectable
                shouldAlwaysSyncParentValues
                hideParentCheckbox={false}
            />
        </div>
    );
};

export default DomainSelector;
