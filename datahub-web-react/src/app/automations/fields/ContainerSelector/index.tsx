import React from 'react';
import { Select } from 'antd';

import { EntityType, SearchAcrossEntitiesInput, Container } from '@types';
import type { ComponentBaseProps } from '@app/automations/types';

import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { PLATFORM_FILTER_NAME } from '../../../searchV2/utils/constants';

// State Type (ensures the state is correctly applied across templates)
export type ContainerSelectorStateType = {
    containers: string[];
};

// Component
export const ContainerSelector = ({ state, props, passStateToParent }: ComponentBaseProps) => {
    // Defined in @app/automations/fields/index
    const { containers } = state as ContainerSelectorStateType;

    // Defined in @app/automations/fields/index
    const { platforms } = props;

    // Dynamic input
    const input: SearchAcrossEntitiesInput = {
        types: [EntityType.Container],
        query: '*',
        start: 0,
        count: 5,
        filters: [],
    };
    if (platforms && platforms.length > 0) {
        input.filters?.push({ field: PLATFORM_FILTER_NAME, values: platforms });
    }

    // Get selectables containers
    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input,
        },
        fetchPolicy: 'cache-first',
    });

    const assets =
        (data?.searchAcrossEntities?.searchResults
            ?.filter((result) => result.entity)
            .map((result) => result.entity) as Container[]) || ([] as Container[]);

    return (
        <Select
            value={containers || []}
            loading={loading}
            mode="multiple"
            placeholder="Select containers…"
            onSelect={(containerUrn: string) => {
                passStateToParent({ containers: [...containers, containerUrn] });
            }}
            onDeselect={(containerUrn: string) => {
                passStateToParent({ containers: containers.filter((p) => containerUrn !== p) });
            }}
        >
            {assets.map((container) => (
                <Select.Option value={container.urn} key={container.urn}>
                    {container.properties?.name}
                </Select.Option>
            ))}
        </Select>
    );
};
