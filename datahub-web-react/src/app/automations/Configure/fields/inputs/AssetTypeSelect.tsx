/*
 * Resuable Tag Select Component
 * Please keep this agnostic and reusable
 */

import React from 'react';

import { Select } from 'antd';

import { useGetSearchResultsQuery } from '@graphql/search.generated';

// Clean data
const cleanData = (data: any) =>
    data?.search?.searchResults?.map((searchResult: any) => searchResult.entity as any) || [];

// Util to simplify term data
const dataForSelect = (items: any) =>
    items.map((item: any) => ({
        label: item.properties?.name,
        value: item.urn,
    }));

// Component
export const AssetTypeSelect = ({ entityTypes, mode, selected }: any) => {
    // Get data
    const { data, loading, error } = useGetSearchResultsQuery({
        variables: {
            input: {
                query: '',
                type: entityTypes[0],
                filters: [],
                start: 0,
                count: 100,
            },
        },
    });

    // Combined loading state
    const isLoading = loading || !data;
    const isError = error ? 'error' : undefined;

    // Clean up data
    const items = dataForSelect(cleanData(data));

    return (
        <Select
            mode={mode}
            options={items}
            loading={isLoading}
            status={isError}
            onChange={(value) => console.log(value)}
            placeholder={`Select ${entityTypes}…`}
            value={selected}
            allowClear
            showArrow
            showSearch
        />
    );
};
