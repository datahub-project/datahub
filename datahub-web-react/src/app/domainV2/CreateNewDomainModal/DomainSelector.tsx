import React, { useState } from 'react';

import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useListDomainsLazyQuery, useListDomainsQuery } from '@src/graphql/domain.generated';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '@src/graphql/search.generated';
import { Domain, EntityType } from '@src/types.generated';

type DomainSelectorProps = {
    selectedDomains: Domain[];
    onDomainsChange: (domains: Domain[]) => void;
    placeholder?: string;
    label?: string;
    isMultiSelect?: boolean;
};

/**
 * Standalone domain selector component that doesn't rely on Ant Design form state
 * Supports both single and multiple domain selection based on isMultiSelect prop
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

    // Convert selected domains to NestedSelectOption format
    const initialOptions = selectedDomains.map((domain: Domain) => ({
        value: domain.urn,
        label: entityRegistry.getDisplayName(domain.type, domain),
        id: domain.urn,
        parentId: domain.parentDomains?.domains?.[0]?.urn,
        entity: domain,
    }));

    const [autoComplete, { data: autoCompleteData }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const { data } = useListDomainsQuery({
        variables: {
            input: {
                query: '*',
                count: 1000,
            },
        },
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
            const selectedDomains = values.map((v) => v.entity).filter((r) => !!r) as Domain[];
            onDomainsChange(selectedDomains);
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
                areParentsSelectable={true}
                shouldAlwaysSyncParentValues={true}
                hideParentCheckbox={false}
            />
        </div>
    );
};

export default DomainSelector;
