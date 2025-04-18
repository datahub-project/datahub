import { Select } from 'antd';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import type { SelectDropdownProps } from '@app/automations/fields/TermSelector/types';
import { TagTermLabel } from '@app/shared/tags/TagTermLabel';

import { useGetSearchResultsQuery } from '@graphql/search.generated';

export const Label = styled.div`
    font-size: 12px;
    font-weight: 600;
    margin-bottom: 4px;

    &.heading {
        font-size: 14px;
    }
`;

// Clean data
const cleanData = (data: any) =>
    data?.search?.searchResults?.map((searchResult: any) => searchResult.entity as any) || [];

export const SelectDropdown = ({
    label,
    type,
    placeholder,
    preselectedOptions,
    enabled = true,
    onChange,
}: SelectDropdownProps) => {
    const [query, setQuery] = useState('');
    const [selectedOptions, setSelectedOptions] = useState<string[]>(preselectedOptions);

    // Get options
    const { data, loading, error } = useGetSearchResultsQuery({
        variables: {
            input: {
                query,
                type,
                filters: [],
                start: 0,
                count: 40,
            },
        },
        skip: !enabled,
    });

    const prevProps = useRef(selectedOptions);

    // Send the data back to the parent component
    // Only sends the data if the form data has changed
    useEffect(() => {
        const prevData = prevProps.current;
        const hasChanged = JSON.stringify(prevData) !== JSON.stringify(selectedOptions);
        if (hasChanged) onChange?.({ [type]: selectedOptions });
        prevProps.current = selectedOptions;
    }, [selectedOptions, onChange, type]);

    if (!enabled) return null;

    const defaultOptions = preselectedOptions || selectedOptions || [];

    return (
        <div>
            <Label>
                {defaultOptions?.length || 0} {label?.toLowerCase()} selected for propagation
            </Label>
            <Select
                mode="multiple"
                options={cleanData(data).map((entity: any) => ({
                    label: <TagTermLabel entity={entity} termName={entity.properties?.name} />,
                    value: entity.urn,
                }))}
                loading={loading}
                status={error && 'error'}
                value={defaultOptions}
                placeholder={placeholder}
                onChange={(value) => setSelectedOptions(value)}
                onSearch={(q) => setQuery(q)}
                style={{ marginTop: '4px' }}
                allowClear
                showArrow
                showSearch
            />
        </div>
    );
};
