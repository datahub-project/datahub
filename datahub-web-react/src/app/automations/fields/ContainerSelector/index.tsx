import React, { useState } from 'react';
import { Select } from 'antd';
import { debounce } from 'lodash';

import { EntityType, SearchAcrossEntitiesInput, Container } from '@types';
import type { ComponentBaseProps } from '@app/automations/types';

import {
    useGetAutoCompleteMultipleResultsQuery,
    useGetSearchResultsForMultipleQuery,
} from '@src/graphql/search.generated';
import { PLATFORM_FILTER_NAME } from '../../../searchV2/utils/constants';

export type ContainerSelectorStateType = {
    containers: string[];
};

export const ContainerSelector = ({ state, props, passStateToParent }: ComponentBaseProps) => {
    const [query, setQuery] = useState('');
    const { containers } = state as ContainerSelectorStateType;
    const { platforms } = props;

    // Dynamic input for initial results
    const input: SearchAcrossEntitiesInput = {
        types: [EntityType.Container],
        query: '*', // initial query to get some results
        start: 0,
        count: 5,
        filters: [],
    };

    if (platforms && platforms.length > 0) {
        input.filters?.push({ field: PLATFORM_FILTER_NAME, values: platforms });
    }

    // Query to fetch initial container list
    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input,
        },
        skip: !!query, // skip if there's a query (i.e. searching)
        fetchPolicy: 'cache-first',
    });

    // Autocomplete (Search) query
    const { data: autocompleteData, loading: autoCompleteLoading } = useGetAutoCompleteMultipleResultsQuery({
        variables: {
            input: {
                types: [EntityType.Container],
                query,
                limit: 5,
                filters: input.filters,
            },
        },
        skip: !query, // skip when no search query
    });

    // Handle the search input
    const handleSearch = (value: string) => {
        setQuery(value);
    };

    // Initial list of containers (default state before search)
    const initialAssets =
        (data?.searchAcrossEntities?.searchResults
            ?.filter((result) => result.entity)
            .map((result) => result.entity) as Container[]) || ([] as Container[]);

    // Results from autocomplete query
    const searchResults =
        autocompleteData?.autoCompleteForMultiple?.suggestions?.flatMap((suggestion) => suggestion.entities) || [];

    // Use search results if a query exists, otherwise use the initial assets
    const assets = query ? searchResults : initialAssets;

    return (
        <Select
            value={containers || []}
            loading={loading || autoCompleteLoading}
            mode="multiple"
            placeholder="Select containers…"
            onSelect={(containerUrn: string) => {
                passStateToParent({ containers: [...containers, containerUrn] });
            }}
            onDeselect={(containerUrn: string) => {
                passStateToParent({ containers: containers.filter((p) => containerUrn !== p) });
            }}
            allowClear={false}
            onSearch={debounce(handleSearch, 200)}
            filterOption={false} // disable antd's built-in filtering to work our custom filtering
        >
            {assets.map((container) => (
                <Select.Option value={container.urn} key={container.urn}>
                    {container.properties?.name}
                </Select.Option>
            ))}
        </Select>
    );
};
