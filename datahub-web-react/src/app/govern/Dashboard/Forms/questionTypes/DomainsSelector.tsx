import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useListDomainsLazyQuery, useListDomainsQuery } from '@src/graphql/domain.generated';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '@src/graphql/search.generated';
import { Domain, EntityType } from '@src/types.generated';
import { Form } from 'antd';
import React, { useState } from 'react';
import { SelectorWrapper } from '../styledComponents';

const DomainsSelector = () => {
    const entityRegistry = useEntityRegistryV2();
    const [useSearch, setUseSearch] = useState(false);
    const form = Form.useFormInstance();
    const initialAllowedDomains = form.getFieldValue(['domainParams', 'allowedDomains']) || [];
    const initialOptions = initialAllowedDomains.map((domain: Domain) => ({
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

    const [childOptions, setChildOptions] = useState<SelectOption[]>([]);

    const [listDomains] = useListDomainsLazyQuery({
        onCompleted: (listDomainsData) => {
            const childOptionsToAdd: SelectOption[] = [];
            listDomainsData.listDomains?.domains.forEach((domain) => {
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
        data?.listDomains?.domains.map((domain) => ({
            value: domain.urn,
            label: entityRegistry.getDisplayName(domain.type, domain),
            id: domain.urn,
            isParent: !!domain.children?.total,
            entity: domain,
        })) || [];
    const autoCompleteOptions =
        autoCompleteData?.autoCompleteForMultiple?.suggestions.flatMap((s) =>
            s.entities.map((domain) => ({
                value: domain.urn,
                label: entityRegistry.getDisplayName(domain.type, domain),
                id: domain.urn,
                entity: domain,
            })),
        ) || [];

    function handleLoad(option: SelectOption) {
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

    function handleUpdate(values: SelectOption[]) {
        if (values.length) {
            const allowedDomains = values.map((v) => v.entity).filter((r) => !!r);
            form.setFieldValue(['domainParams', 'allowedDomains'], allowedDomains);
        } else {
            form.setFieldValue(['domainParams', 'allowedDomains'], undefined);
        }
    }

    const defaultOptions = [...options, ...childOptions].sort((a, b) => a.label.localeCompare(b.label));

    return (
        <SelectorWrapper>
            <NestedSelect
                label="Allowed Domains:"
                placeholder="Select allowed domains"
                searchPlaceholder="Search all domains..."
                options={useSearch ? autoCompleteOptions : defaultOptions}
                initialValues={initialOptions}
                loadData={handleLoad}
                onSearch={handleSearch}
                onUpdate={handleUpdate}
                width="full"
                isMultiSelect
                showSearch
            />
        </SelectorWrapper>
    );
};

export default DomainsSelector;
