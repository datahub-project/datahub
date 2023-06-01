import React from 'react';
import { getTestEntityRegistry } from '../../../../utils/test-utils/TestPageContainer';
import {
    ENTITY_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    ENTITY_TYPE_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '../../utils/constants';
import { getDisplayedFilterOptions, getNumActiveFilters } from '../EntityTypeFilter/entityTypeFilterUtils';
import FilterOption from '../FilterOption';

describe('getDisplayedFilterOptions', () => {
    const testEntityRegistry = getTestEntityRegistry();
    const mockSetSelectedFilterOptions = () => {};
    const mockData = {
        aggregateAcrossEntities: {
            facets: [
                {
                    field: ENTITY_SUB_TYPE_FILTER_NAME,
                    aggregations: [
                        { value: 'DATASET', count: 12 },
                        { value: 'DATASET␞table', count: 6 },
                        { value: 'DATASET␞view', count: 6 },
                        { value: 'CONTAINER', count: 6 },
                        { value: 'CONTAINER␞test', count: 3 },
                    ],
                },
                {
                    field: ENTITY_TYPE_FILTER_NAME,
                    aggregations: [
                        { value: 'DASHBOARD', count: 5 },
                        { value: 'CHART', count: 100 },
                    ],
                },
                {
                    field: TAGS_FILTER_NAME,
                    aggregations: [{ value: 'urn:li:tag:tag1', count: 20 }],
                },
            ],
        },
    };

    it('should get the list of displayed filter options with nested filters underneath their parents', () => {
        const filterOptions = getDisplayedFilterOptions(
            [],
            testEntityRegistry,
            mockSetSelectedFilterOptions,
            '',
            mockData,
        );

        const expectedFilterOptions = [
            {
                key: 'DATASET',
                label: (
                    <FilterOption
                        filterOption={{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET', count: 12 }}
                        selectedFilterOptions={[]}
                        setSelectedFilterOptions={mockSetSelectedFilterOptions}
                    />
                ),
                style: { padding: 0 },
                displayName: 'Datasets',
                nestedOptions: [
                    { field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET␞table', count: 6 },
                    { field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET␞view', count: 6 },
                ],
            },
            {
                key: 'CONTAINER',
                label: (
                    <FilterOption
                        filterOption={{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'CONTAINER', count: 6 }}
                        selectedFilterOptions={[]}
                        setSelectedFilterOptions={mockSetSelectedFilterOptions}
                    />
                ),
                style: { padding: 0 },
                displayName: 'Containers',
                nestedOptions: [{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'CONTAINER␞test', count: 3 }],
            },
            {
                key: 'DASHBOARD',
                label: (
                    <FilterOption
                        filterOption={{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DASHBOARD', count: 5 }}
                        selectedFilterOptions={[]}
                        setSelectedFilterOptions={mockSetSelectedFilterOptions}
                    />
                ),
                style: { padding: 0 },
                displayName: 'Dashboards',
                nestedOptions: [],
            },
            {
                key: 'CHART',
                label: (
                    <FilterOption
                        filterOption={{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'CHART', count: 100 }}
                        selectedFilterOptions={[]}
                        setSelectedFilterOptions={mockSetSelectedFilterOptions}
                    />
                ),
                style: { padding: 0 },
                displayName: 'Charts',
                nestedOptions: [],
            },
        ];

        expect(filterOptions).toMatchObject(expectedFilterOptions);
    });

    it('should get displayed filters and filter for nestedFilters', () => {
        const filterOptions = getDisplayedFilterOptions(
            [],
            testEntityRegistry,
            mockSetSelectedFilterOptions,
            'vie',
            mockData,
        );

        const expectedFilterOptions = [
            {
                key: 'DATASET',
                label: (
                    <FilterOption
                        filterOption={{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET', count: 12 }}
                        selectedFilterOptions={[]}
                        setSelectedFilterOptions={mockSetSelectedFilterOptions}
                    />
                ),
                style: { padding: 0 },
                displayName: 'Datasets',
                nestedOptions: [{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET␞view', count: 6 }],
            },
        ];

        expect(filterOptions).toMatchObject(expectedFilterOptions);
    });

    it('should get displayed filters and filter for top level filters', () => {
        const filterOptions = getDisplayedFilterOptions(
            [],
            testEntityRegistry,
            mockSetSelectedFilterOptions,
            'datase',
            mockData,
        );

        const expectedFilterOptions = [
            {
                key: 'DATASET',
                label: (
                    <FilterOption
                        filterOption={{ field: ENTITY_SUB_TYPE_FILTER_NAME, value: 'DATASET', count: 12 }}
                        selectedFilterOptions={[]}
                        setSelectedFilterOptions={mockSetSelectedFilterOptions}
                    />
                ),
                style: { padding: 0 },
                displayName: 'Datasets',
                nestedOptions: [],
            },
        ];

        expect(filterOptions).toMatchObject(expectedFilterOptions);
    });
});

describe('getNumActiveFilters', () => {
    it('should get the number of active filters in a backwards compatible way', () => {
        const activeFilters = [
            { field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['DATASET␞view', 'DATASET␞table'] },
            { field: ENTITY_FILTER_NAME, values: ['DATASET', 'CONTAINER'] },
            { field: ENTITY_TYPE_FILTER_NAME, values: ['CONTAINER'] },
            { field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag1'] },
        ];
        const numActiveFilters = getNumActiveFilters(activeFilters);
        expect(numActiveFilters).toBe(4); // should not count duplicate values
    });
});
